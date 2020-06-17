# Concord
#
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import os.path
import random
import unittest

import trio

from util import bft_network_partitioning as net

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
            else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcNetworkPartitioningTest(unittest.TestCase):

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_while_dropping_packets(self, bft_network, tracker):
        """
         Run a bunch of concurrent requests in batches and verify
         linearizability, while dropping a small amount of packets
         between all replicas.
         """
        num_ops = 500

        with net.PacketDroppingAdversary(
                bft_network, drop_rate_percentage=5) as adversary:
            self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            self.bft_network = bft_network
            bft_network.start_all_replicas()

            adversary.interfere()

            await tracker.run_concurrent_ops(num_ops)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_single_vc_primary_isolated(self, bft_network, tracker):
        """
        The goal of this test is to check the view change
        workflow in case the primary is up, but its outgoing
        communication is intercepted by an adversary.

        1) Given a BFT network,
        2) Insert an adversary that isolates the primary's outgoing communication
        3) Send a batch of write requests.
        4) Verify the BFT network eventually transitions to the next view.
        5) Perform a "read-your-writes" check in the new view
        """
        with net.PrimaryIsolatingAdversary(bft_network) as adversary:
            bft_network.start_all_replicas()

            initial_primary = 0
            await bft_network.wait_for_view(
                replica_id=initial_primary,
                expected=lambda v: v == initial_primary,
                err_msg="Make sure we are in the initial view "
                        "before isolating the primary."
            )

            await adversary.interfere()
            expected_next_primary = 1

            await self._send_random_writes(tracker)

            await bft_network.wait_for_view(
                replica_id=random.choice(bft_network.all_replicas(without={0})),
                expected=lambda v: v == expected_next_primary,
                err_msg="Make sure view change has been triggered."
            )

            await self._wait_for_read_your_writes_success(tracker)

            await tracker.run_concurrent_ops(100)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_isolate_f_non_primaries_slow_path(self, bft_network, tracker):
        """
        This test makes sure that a BFT network continues making progress (albeit on the slow path),
        despite the presence of an adversary that isolates f replicas.

        Once the adversary disappears, we check that the isolated replicas catch up
        with the others and correctly participate in consensus.

        Note: there is no state transfer in this test scenario, because the replica isolating
        adversary hasn't been active for long enough for the unaffected replicas
        to trigger a checkpoint.
        """
        bft_network.start_all_replicas()

        f = bft_network.config.f
        isolated_replicas = await self._select_random_non_primaries(bft_network, f)

        num_ops = 100
        write_weight = 0.5

        # make sure the presence of the adversary triggers the slow path
        # (because f replicas cannot participate in consensus)
        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            adversary.interfere()

            await tracker.run_concurrent_ops(num_ops=num_ops, write_weight=write_weight)

            await bft_network.wait_for_slow_path_to_be_prevalent(as_of_seq_num=1)

        # Once the adversary is gone, the disconnected replicas should be able
        # to resume their participation in consensus & request execution
        await tracker.run_concurrent_ops(num_ops=num_ops, write_weight=write_weight)
        last_executed_seq_num = await bft_network.wait_for_last_executed_seq_num()

        for ir in isolated_replicas:
            await bft_network.wait_for_last_executed_seq_num(
                replica_id=ir, expected=last_executed_seq_num)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_isolate_f_non_primaries_state_transfer(self, bft_network):
        """
        In this test we isolate f replicas long enough for the unaffected replicas to
        trigger a checkpoint. Then, once the adversary is not active anymore, we make
        sure the isolated replicas catch up via state transfer.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        f = bft_network.config.f
        isolated_replicas = await self._select_random_non_primaries(bft_network, f)

        live_replicas = set(bft_network.all_replicas()) - set(isolated_replicas)

        # reach a checkpoint, despite the presence of an adversary
        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            adversary.interfere()

            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=list(live_replicas),
                checkpoint_num=1,
                verify_checkpoint_persistency=False
            )

        # at this point the adversary is inactive, so the isolated replicas
        # should be able to catch-up via state transfer
        await bft_network.wait_for_state_transfer_to_start()

        # state transfer should complete on all isolated replicas
        for ir in isolated_replicas:
            await bft_network.wait_for_state_transfer_to_stop(0, ir)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability
    async def test_isolate_non_primaries_subset_with_view_change(self, bft_network, tracker):
        """
        In this test we isolate f-1 replicas from the rest of the BFT network.
        We crash the primary and trigger view change while the f-1 replicas are still isolated.
        At this point we have a total of f unavailable replicas.

        The adversary is then deactivated and we make sure the previously isolated replicas
        activate the new view and correctly process incoming client requests.
        """
        bft_network.start_all_replicas()

        f = bft_network.config.f
        initial_primary = await bft_network.get_current_primary()
        expected_next_primary = 1 + initial_primary

        isolated_replicas = await self._select_random_non_primaries(
            bft_network=bft_network,
            num=f-1,  # for a total of "f" unavailable replicas, including a crashed primary
            other_excluded_replicas={expected_next_primary}  # to avoid a double view change
        )

        print(f"Isolating network traffic to/from replicas {isolated_replicas}.")
        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            adversary.interfere()

            bft_network.stop_replica(initial_primary)
            await self._send_random_writes(tracker)

            await bft_network.wait_for_view(
                replica_id=random.choice(bft_network.all_replicas(
                    without={initial_primary}.union(isolated_replicas))),
                expected=lambda v: v == expected_next_primary,
                err_msg="Make sure view change has been triggered."
            )

            # waiting for the active window to be rebuilt after the view change
            await trio.sleep(seconds=10)

        # the adversary is not active anymore:
        # make sure the isolated replicas activate the new view
        for ir in isolated_replicas:
            await bft_network.wait_for_view(
                replica_id=ir,
                expected=lambda v: v == expected_next_primary,
                err_msg=f"Make sure isolated replica #{ir} works in new view {expected_next_primary}."
            )

        # then make sure the isolated replicas participate in consensus & request execution
        await tracker.run_concurrent_ops(num_ops=50)

        expected_last_executed_seq_num = await bft_network.wait_for_last_executed_seq_num(
            replica_id=random.choice(bft_network.all_replicas(without={initial_primary}.union(isolated_replicas))))

        for ir in isolated_replicas:
            await bft_network.wait_for_last_executed_seq_num(
                replica_id=ir, expected=expected_last_executed_seq_num)

    @staticmethod
    async def _wait_for_read_your_writes_success(tracker):
        with trio.fail_after(seconds=60):
            while True:
                with trio.move_on_after(seconds=5):
                    try:
                        await tracker.tracked_read_your_writes()
                    except Exception:
                        continue
                    else:
                        break

    @staticmethod
    async def _send_random_writes(tracker):
        with trio.move_on_after(seconds=1):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.send_indefinite_tracked_ops, 1)

    @staticmethod
    async def _select_random_non_primaries(bft_network, num, other_excluded_replicas=None):
        if other_excluded_replicas is None:
            other_excluded_replicas = set()

        primary = await bft_network.get_current_primary()
        all_non_primaries = bft_network.all_replicas(
            without={primary}.union(other_excluded_replicas))
        random.shuffle(all_non_primaries)
        selected_replicas = all_non_primaries[:num]

        return selected_replicas
