
# Concord
#
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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
from os import environ

import trio

from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX

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


class SkvbcChaoticStartupTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario


    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_delayed_replicas_start_up(self, bft_network, skvbc, nursery):
        """
        The goal is to make sure that if replicas are started in a random
        order, with delays in-between, and with constant load (request sent every 1s),
        the BFT network eventually stabilizes its view and correctly processes requests
        from this point onwards.
        1) Start sending constant requests, even before the BFT network is started
        2) Create a random replica start order
        3) Use the order defined above to start all replicas with delays in-between
        4) Make sure at least one view change was agreed (because not enough replicas for quorum initially)
        5) Make sure the agreed view is stable after all replicas are started
        6) Make sure the system correctly processes requests in the new view
        """

        replicas_starting_order = bft_network.all_replicas()
        random.shuffle(replicas_starting_order)

        initial_view = 0
        try:
            # Delayed replica start-up...
            for r in replicas_starting_order:
                bft_network.start_replica(r)
                await trio.sleep(seconds=10)

            current_view = await bft_network.wait_for_view(
                replica_id=0,
                expected=lambda v: v > initial_view,
                err_msg="Make sure view change has occurred during the delayed replica start-up."
            )

            client = bft_network.random_client()
            current_block = skvbc.parse_reply(
                await client.read(skvbc.get_last_block_req()))

            with trio.move_on_after(seconds=60):
                while True:
                    # Make sure the current view is stable
                    await bft_network.wait_for_view(
                        replica_id=0,
                        expected=lambda v: v == current_view,
                        err_msg="Make sure view is stable after all replicas are started."
                    )
                    await trio.sleep(seconds=5)

                    # Make sure the system is processing requests
                    last_block = skvbc.parse_reply(
                        await client.read(skvbc.get_last_block_req()))
                    self.assertTrue(last_block > current_block)
                    current_block = last_block

            current_primary = current_view % bft_network.config.n
            non_primaries = bft_network.all_replicas(without={current_primary})
            restarted_replica = random.choice(non_primaries)
            print(f"Restart replica #{restarted_replica} to make sure its view is persistent...")

            bft_network.stop_replica(restarted_replica)
            await trio.sleep(seconds=5)
            bft_network.start_replica(restarted_replica)

            await trio.sleep(seconds=20)

            # Stop sending requests, and make sure the restarted replica
            # is up-and-running and participates in consensus
            nursery.cancel_scope.cancel()

            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
            last_executed_seq_num = await bft_network.metrics.get(current_primary, *key)

            with trio.fail_after(seconds=30):
                while True:
                    last_executed_seq_num_restarted = await bft_network.metrics.get(restarted_replica, *key)
                    if last_executed_seq_num_restarted >= last_executed_seq_num:
                        break

        except Exception as e:
            print(f"Delayed replicas start-up failed for start-up order: {replicas_starting_order}")
            raise e
        else:
            print(f"Delayed replicas start-up succeeded for start-up order: {replicas_starting_order}. "
                  f"The BFT network eventually stabilized in view #{current_view}.")

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: f >= 2)
    @with_constant_load
    async def test_f_staggered_replicas_requesting_vc(self, bft_network, skvbc, nursery):
        """
        The goal of this test is to verify correct behaviour in the situation where
        a subset of F replicas not big enough to reach quorum for execution starts early,
        accepts client requests and initiates view change. The correct behaviour in this
        case will be to for the system to make progress in the slow path after the remaining
        2F+1 replicas are up. After execution is confirmed in the slow path, we stop one
        replica, but not the current or next Primary to verify View Change will happen due
        to inability to reach quorums for execution in the current View. When the new View
        is active we verify the system is able to make progress.

        1) We use a decorator providing constant load of client requests from the start of the test.
        2) Given a BFT network, we start F out of all N replicas.
        3) We wait for them to initiate View Change.
        4) Then we start the remaining 2F+1 replicas.
        5) We verify the system is making progress until a Checkpoint is reached.
        6) Then we verify the early F replicas that initiated View Change catchup by State Transfer.
        7) After this we stop 1 replica, but not the initial or expected next primary.
        8) We verify View Change happens and the system is able to execute requests.
        """

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_view = initial_primary = 0
        expected_next_view = expected_next_primary = 1

        # take a random set containing F replicas out of all N without the initial primary
        early_replicas = set()
        for _ in range(f):
            exclude_replicas = early_replicas | {initial_primary}
            early_replicas.add(random.choice(bft_network.all_replicas(without=exclude_replicas)))

        late_replicas = bft_network.all_replicas(without=early_replicas)

        print(f"STATUS: Starting F={f} replicas.")
        bft_network.start_replicas(replicas=early_replicas)

        print("STATUS: Wait for early replicas to initiate View Change.")
        for r in early_replicas:
            active_view = 0
            view = 0
            while active_view == view:
                active_view = await self._get_gauge(r, bft_network, 'currentActiveView')
                view = await self._get_gauge(r, bft_network, 'view')
                await trio.sleep(seconds=0.1)

        print("STATUS: Early replicas initiated View Change.")
        print(f"STATUS: Starting the remaining {n-f} replicas.")
        bft_network.start_replicas(late_replicas)

        view = await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == initial_primary,
            err_msg="Make sure we are in the initial view "
        )

        self.assertTrue(initial_view == view)

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=initial_primary)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without=early_replicas),
            checkpoint_num=1,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )
        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=initial_primary)
        # Verify the system is able to make progress
        self.assertTrue(checkpoint_after > checkpoint_before)
        
        # Verify replicas that have initiated View Change catch up on state
        await bft_network.wait_for_state_transfer_to_start()
        for r in early_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_primary,
                                                              r,
                                                              stop_on_stable_seq_num=True)
        print("STATUS: Early replicas that have initiated View Change catch up on state.")

        # Stop one of the later started replicas, but not the initial Primary
        # in order to verify that View Change will happen due to inability to
        # reach quorums in the slow path. Also don't stop next primary, because
        # in this test we check a single view change.
        replica_to_stop = random.choice(bft_network.all_replicas(without=early_replicas | {initial_primary,
                                                                                           expected_next_primary}))
        print("STATUS: Stopping one of the later replicas with ID={}".format(replica_to_stop))
        bft_network.stop_replica(replica_to_stop)

        # Wait for View Change to happen.
        view = await bft_network.wait_for_view(
            replica_id=expected_next_primary,
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure we are in the next view "
        )

        self.assertTrue(expected_next_view == view)

        await trio.sleep(10)  # TODO: remove when bft_network.wait_for_view also waits for system liveness.

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=expected_next_primary)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without={replica_to_stop}),
            checkpoint_num=1,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )
        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=expected_next_primary)

        # Verify that the system is making progress after the View Change.
        self.assertTrue(checkpoint_after > checkpoint_before)

    @classmethod
    async def _get_gauge(cls, replica_id, bft_network, gauge):
        with trio.fail_after(seconds=30):
            while True:
                with trio.move_on_after(seconds=1):
                    try:
                        key = ['replica', 'Gauges', gauge]
                        value = await bft_network.metrics.get(replica_id, *key)
                    except KeyError:
                        # metrics not yet available, continue looping
                        print(f"KeyError! '{gauge}' not yet available.")
                    else:
                        return value
