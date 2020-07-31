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

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX


def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", view_change_timeout_milli,
            "-p",
            "-t", os.environ.get('STORAGE_TYPE')]


def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper


class SkvbcBackupRestoreTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_checkpoint_propagation_after_restarting_replicas(self, bft_network):
        """
        Here we trigger a checkpoint, restart all replicas in a random order with 10s delay in-between,
        both while stopping and starting. We verify checkpoint persisted upon restart and then trigger
        another checkpoint. We make sure checkpoint is propagated to all the replicas.
        1) Given a BFT network, we make sure all nodes are up
        2) Send sufficient number of client requests to trigger checkpoint protocol
        3) Stop all replicas in a random order (with 10s delay in between)
        4) Start all replicas in a random order (with 10s delay in between)
        5) Make sure the initial view is stable
        6) Send sufficient number of client requests to trigger another checkpoint
        7) Make sure checkpoint propagates to all the replicas
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n

        self.assertEqual(len(bft_network.procs), n, "Make sure all replicas are up initially.")

        current_primary = await bft_network.get_current_primary()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False
        )

        # stop n replicas in a random order with a delay of 10s in between
        stopped_replicas = await self._stop_random_replicas_with_delay(bft_network, delay=5)

        # start stopped replicas in a random order with a delay of 10s in between
        await self._start_random_replicas_with_delay(bft_network, stopped_replicas, current_primary, delay=5)

        # verify checkpoint persistence
        await bft_network.wait_for_replicas_to_checkpoint(
            stopped_replicas,
            expected_checkpoint_num=lambda ecn: ecn == checkpoint_before + 1)

        # verify current view is stable
        for replica in bft_network.all_replicas():
            await bft_network.wait_for_view(
                replica_id=replica,
                expected=lambda v: v == current_primary,
                err_msg="Make sure view is stable after all replicas started."
            )

        # create second checkpoint and wait for checkpoint propagation
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False
        )

    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("20000"),
                      selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_checkpoint_propagation_after_restarting_all_replicas_under_load(self, bft_network, skvbc, nursery):
        """
        Here we trigger a checkpoint, restart all replicas in a random order with 10s delay in-between,
        both while stopping and starting. We verify checkpoint persisted upon restart and then trigger
        another checkpoint. We make sure checkpoint is propagated to all the replicas.
        1) Given a BFT network, we make sure all nodes are up
        2) Send sufficient number of client requests to trigger checkpoint protocol
        3) Stop all replicas in a random order (with 10/n seconds delay in between)
        4) Start all replicas in a random order (with 10s delay in between)
        5) Wait for view to stabilize
        6) Send sufficient number of client requests to trigger another checkpoint
        7) Make sure checkpoint propagates to all the replicas
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n

        self.assertEqual(len(bft_network.procs), n, "Make sure all replicas are up initially.")

        current_primary = await bft_network.get_current_primary()

        initial_view = 0

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=current_primary)
        await self._fill_and_wait_for_checkpoint_under_constant_load(skvbc,
                                                                     bft_network,
                                                                     initial_nodes=bft_network.all_replicas(),
                                                                     num_of_checkpoints_to_add=1,
                                                                     verify_checkpoint_persistency=False,
                                                                     assert_state_transfer_not_started=False)

        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=current_primary)
        self.assertGreaterEqual(checkpoint_before + 1, checkpoint_after)

        # stop n replicas in a random order with a delay of 10/n seconds in between, so that
        # all replicas are stopped by 10seconds, and no view change is triggered during stopping
        # of replicas.
        stopped_replicas = await self._stop_random_replicas_with_delay(bft_network, delay=10/n)

        stopped_replicas.remove(current_primary)

        # start stopped replicas in a random order with a delay of 10s in between
        # view change only happens if the initial primary starts at time > viewchangeTimer
        # to make the test robust we start primary at the end.
        await self._start_random_replicas_with_delay(bft_network, stopped_replicas, current_primary, delay=10)

        # wait for view to stabilize
        for replica in bft_network.all_replicas():
            if await self._wait_for_view_under_constant_load(
                    replica_id=replica,
                    bft_network=bft_network,
                    expected=lambda v: v > initial_view,
                    err_msg="Make sure view is stable after all replicas are started."):
                break

        current_primary = await bft_network.get_current_primary()

        await self._wait_for_processing_window_after_view_change(current_primary, bft_network)

        # verify checkpoint persistency
        await bft_network.wait_for_replicas_to_checkpoint(bft_network.all_replicas(),
                                                          expected_checkpoint_num=lambda ecn: ecn >= checkpoint_after)

        next_checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=current_primary)
        await self._fill_and_wait_for_checkpoint_under_constant_load(skvbc,
                                                                     bft_network,
                                                                     initial_nodes=bft_network.all_replicas(),
                                                                     num_of_checkpoints_to_add=1,
                                                                     verify_checkpoint_persistency=False,
                                                                     assert_state_transfer_not_started=False)

        next_checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=current_primary)
        self.assertGreaterEqual(next_checkpoint_before + 1, next_checkpoint_after)

    @staticmethod
    async def _stop_random_replicas_with_delay(bft_network, delay=10, exclude_replicas=None):
        all_replicas = bft_network.all_replicas(without=exclude_replicas)
        random.shuffle(all_replicas)
        for replica in all_replicas:
            print("stopping replica: ", replica)
            bft_network.stop_replica(replica)
            await trio.sleep(delay)
        return list(all_replicas)

    @staticmethod
    async def _start_random_replicas_with_delay(bft_network, stopped_replicas, initial_primary, delay=10):
        random.shuffle(stopped_replicas)
        if initial_primary not in stopped_replicas:
            stopped_replicas.append(initial_primary)
        for replica in stopped_replicas:
            print("starting replica: ", replica)
            bft_network.start_replica(replica)
            await trio.sleep(delay)
        return stopped_replicas

    async def _wait_for_processing_window_after_view_change(self, primary_id, bft_network):
        with trio.fail_after(seconds=30):
            last_exec_seq_num = await self._get_gauge(primary_id, bft_network, "lastExecutedSeqNum")
            conc_level = await self._get_gauge(primary_id, bft_network, "concurrencyLevel")
            prim_last_used_seq_num = await self._get_gauge(primary_id, bft_network, "primaryLastUsedSeqNum")
            while prim_last_used_seq_num >= last_exec_seq_num + conc_level:
                await trio.sleep(seconds=1)
                last_exec_seq_num = await self._get_gauge(primary_id, bft_network, "lastExecutedSeqNum")
                conc_level = await self._get_gauge(primary_id, bft_network, "concurrencyLevel")

    async def _wait_for_view_under_constant_load(self, replica_id, bft_network, expected=None,
                                                 err_msg="Expected view not reached"):
        """
        Similar to wait_for_view method, except it allows for consecutive unexpected
        view changes when waiting for active view.
        """
        if expected is None:
            expected = lambda _: True

        matching_view = None
        nb_replicas_in_matching_view = 0
        try:
            matching_view = await bft_network._wait_for_matching_agreed_view(replica_id, expected)
            print(f'Matching view #{matching_view} has been agreed among replicas.')

            nb_replicas_in_matching_view = await self._wait_for_active_view_under_constant_load(
                matching_view, bft_network, replica_id, expected)
            print(f'View #{matching_view} has been activated by '
                  f'{nb_replicas_in_matching_view} >= n-f = {bft_network.config.n - bft_network.config.f}')

            return matching_view
        except trio.TooSlowError:
            assert False, err_msg + \
                          f'(matchingView={matching_view} ' \
                          f'replicasInMatchingView={nb_replicas_in_matching_view})'

    @staticmethod
    async def _wait_for_active_view_under_constant_load(view, bft_network, replica_id, expected,
                                                        fail_after_time=30):
        """
        Wait for the latest matching_view to become active on enough (n-f) replicas
        """
        with trio.fail_after(seconds=fail_after_time):
            while True:
                nb_replicas_in_view = await bft_network._count_replicas_in_view(view)

                # wait for n-f = 2f+2c+1 replicas to be in the expected view
                if nb_replicas_in_view >= 2 * bft_network.config.f + 2 * bft_network.config.c + 1:
                    break

                # if matching_view updates due to unexpected view change, wait for the latest
                # matching_view to become active
                matching_view = await bft_network._wait_for_matching_agreed_view(replica_id, expected)
                if matching_view > view:
                    print(f'Updated matching view #{matching_view} has been agreed among replicas.')
                    view = matching_view
                    fail_after_time += 30
        return nb_replicas_in_view

    @staticmethod
    async def _fill_and_wait_for_checkpoint_under_constant_load(skvbc, bft_network, initial_nodes,
                                                                num_of_checkpoints_to_add=2,
                                                                verify_checkpoint_persistency=True,
                                                                assert_state_transfer_not_started=True):
        """
        Similar to fill_and_wait_for_checkpoint, except under constant load additional
        checkpoints may be created. The expected_checkpoint_num in that case may not
        necessarily be checkpoint_before + num_of_checkpoints_to_add. This function
        account for the unexpected checkpoints created due to constant load.
        """
        client = kvbc.SkvbcClient(bft_network.random_client())
        checkpoint_before = await bft_network.wait_for_checkpoint(
            replica_id=random.choice(initial_nodes))
        # Write enough data to checkpoint and create a need for state transfer
        for i in range(1 + num_of_checkpoints_to_add * 150):
            key = skvbc.random_key()
            val = skvbc.random_value()
            reply = await client.write([], [(key, val)])
            assert reply.success

        await skvbc.network_wait_for_checkpoint(
            initial_nodes,
            expected_checkpoint_num=lambda ecn: ecn >= checkpoint_before,
            verify_checkpoint_persistency=verify_checkpoint_persistency,
            assert_state_transfer_not_started=assert_state_transfer_not_started)

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
