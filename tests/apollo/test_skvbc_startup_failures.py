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
from os import environ

import trio

from util import bft_network_partitioning as net
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability
from trio._timeouts import TooSlowError

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


class SkvbcStartupFailuresTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: f >= 2)
    async def test_client_msgs_during_state_transfer(self, bft_network):
        """
        The goal of this test is to validate that during state transfer
        in a Replica no View Change is generated, even while clients
        send requests to all Replicas.

        1) Start all replicas except 2 (this test is valid only for f>=2).
        2) Initiate Client requests and reach a Checkpoint.
        3) Start the delayed replicas.
        4) Verify State transfer is initiated.
        5) Send Client requests to all Replicas.
        6) Verify no View Change msgs were sent from delayed replicas.
        """
        initial_primary = 0
        delayed_replicas = {1,2}
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        
        initially_started_replicas = bft_network.all_replicas(without=delayed_replicas)
        bft_network.start_replicas(replicas=initially_started_replicas)
        
        first_primary = await bft_network.get_current_primary()
        initial_view = await bft_network.get_current_view()
        print("DEBUG: primary={} view={}.".format(first_primary, initial_view))

        actual_view = 0
        timeout = 60
        with trio.fail_after(seconds=timeout): 
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    self._send_requests_to_all_replicas, timeout, bft_network, skvbc)
                actual_view = await bft_network.wait_for_view(
                    replica_id=initial_primary,
                    expected=lambda v: v == initial_primary,
                    err_msg="Make sure we are in the initial view "
                )
                nursery.cancel_scope.cancel()
        
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=initial_primary)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without=delayed_replicas),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )
        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=initial_primary)

        self.assertTrue(checkpoint_after > checkpoint_before)

        await self._send_requests_to_all_replicas(5, bft_network, skvbc)

        print("DEBUG: Starting replicas {} in view {}.".format(delayed_replicas, actual_view))

        bft_network.start_replicas(delayed_replicas)

        await self._send_requests_to_all_replicas(5, bft_network, skvbc)

        with trio.fail_after(seconds=timeout): 
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    self._send_requests_to_all_replicas, timeout, bft_network, skvbc)
                await bft_network.wait_for_state_transfer_to_start()
                nursery.cancel_scope.cancel()

        await self._send_requests_to_all_replicas(20, bft_network, skvbc)

        for r in delayed_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_primary,
                                                              r)
        for r in delayed_replicas:
            vc_msgs = await self._get_total_view_change_msgs(r, bft_network)
            self.assertEqual(vc_msgs, 0)
        
    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: f >= 2)
    async def test_bug_2513_with_adversary(self, bft_network):
        """
        The goal of this test is to validate that during state transfer
        in a Replica no View Change is generated, even while clients
        send requests to all Replicas.

        1) Start all replicas and insulate 2 of them (this test is valid only for f>=2).
        2) Initiate Client requests and reach a Checkpoint.
        3) Release insulated replicas.
        4) Verify State transfer is initiated.
        5) Send Client requests to all Replicas.
        6) Verify no View Change msgs were sent from delayed replicas.
        """
        initial_primary = 0
        insulated_replicas = {1,2}
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for c in bft_network.clients.values():
            await c.bind()
        
        with net.NodesInsulatingAdversary(bft_network, insulated_replicas) as adversary:
            adversary.interfere()
            bft_network.start_all_replicas()

            first_primary = await bft_network.get_current_primary()
            initial_view = await bft_network.get_current_view()
            print("DEBUG: primary={} view={}.".format(first_primary, initial_view))

            actual_view = 0
            timeout = 60
            with trio.fail_after(seconds=timeout):
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(
                        self._send_requests_to_all_replicas, timeout, bft_network, skvbc)
                    actual_view = await bft_network.wait_for_view(
                        replica_id=initial_primary,
                        expected=lambda v: v == initial_primary,
                        err_msg="Make sure we are in the initial view "
                    )
                    nursery.cancel_scope.cancel()
        
            checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=initial_primary)

            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without=insulated_replicas),
                checkpoint_num=1,
                verify_checkpoint_persistency=False
            )
            checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=initial_primary)

            self.assertTrue(checkpoint_after > checkpoint_before)

        print("DEBUG: Release Replicas from Adversary.")

        await self._send_requests_to_all_replicas(15, bft_network, skvbc)

        with trio.fail_after(seconds=timeout): 
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    self._send_requests_to_all_replicas, timeout, bft_network, skvbc)
                await bft_network.wait_for_state_transfer_to_start()
                nursery.cancel_scope.cancel()
        print("DEBUG: State Transfer Started.")
        
        await self._send_requests_to_all_replicas(25, bft_network, skvbc)
        print("DEBUG: Requests Sent.")
        
        for r in insulated_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_primary,
                                                              r)
        
        for r in insulated_replicas:
            vc_msgs = await self._get_total_view_change_msgs(r, bft_network)
            self.assertEqual(vc_msgs, 0)

    async def _send_requests_to_all_replicas(self, sec, bft_network, skvbc):
        for client in bft_network.clients.values():
            client.primary = None
        try:
            with trio.move_on_after(seconds=sec):  # seconds
                await skvbc.send_indefinite_write_requests()

        except trio.TooSlowError:
            pass

    async def _get_total_view_change_msgs(self, replica_id, bft_network):
        count_of_sent_viewchange_msgs = None
        try:
            with trio.move_on_after(seconds=5):
                key = ['replica', 'Counters', 'sentViewChangeMsgDueToTimer']
                count_of_sent_viewchange_msgs = await bft_network.metrics.get(replica_id, *key)

        except KeyError:
            # metrics not yet available, continue looping
            print("KeyError!")
        except TooSlowError:
            print("TooSlowError!")
        return count_of_sent_viewchange_msgs
