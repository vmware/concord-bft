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
import time
import trio

from util.test_base import ApolloTest
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "3000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-e", str(True)
            ]


class SkvbcMultiSig(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, rotate_keys=True )
    async def test_happy_initial_key_exchange(self, bft_network):
        """
        Validates that if all replicas are up and all key-exchnage msgs reached consensus via the fast path
        then the counter of the exchanged keys is equal to the cluster size in all replicas.
        """
        bft_network.start_all_replicas()


    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_rough_initial_key_exchange(self, bft_network):
        """
        validates that the system can start with a partial set of replicas that exchanged keys.
        The other will get the keys via ST
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        replicas = bft_network.random_set_of_replicas(6, without={2})
        bft_network.start_replicas(replicas)

        with trio.fail_after(seconds=20):
            for replica_id in {0}:
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            self_key_exchange_counter = await bft_network.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                            public_key_exchange_for_peer_counter = await bft_network.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                            value = self_key_exchange_counter + public_key_exchange_for_peer_counter
                            if value < 6:
                                continue
                        except trio.TooSlowError:
                            self.assertTrue(False)
                        else:
                            self.assertEqual(value, 6)
                            break

        lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
        lastExecutedValBefore = await bft_network.metrics.get(0, *lastExecutedKey)

        await trio.sleep(5)
        lastExecutedValAfter = await bft_network.metrics.get(0, *lastExecutedKey)
        self.assertGreaterEqual(lastExecutedValAfter, lastExecutedValBefore)

        # make key exchange complete with partial set of replica
        bft_network.stop_replica(3)
        time.sleep(2)
        bft_network.start_replica(2)

        for i in range(10):
            await skvbc.send_write_kv_set()

        with trio.fail_after(seconds=20):
            for replica_id in {0,2}:
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            self_key_exchange_counter = await bft_network.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                            public_key_exchange_for_peer_counter = await bft_network.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                            value = self_key_exchange_counter + public_key_exchange_for_peer_counter
                            if value < 7:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to exchange keys on start")
                            self.assertTrue(False)
                        else:
                            self.assertEqual(value, 7)
                            break

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without={3}),
            num_of_checkpoints_to_add=3,
            verify_checkpoint_persistency=False
        )
        # replica #3 should catch keys with ST
        bft_network.start_replica(3)
        await bft_network.wait_for_state_transfer_to_start()
        await bft_network.wait_for_state_transfer_to_stop(0, 3)
        await skvbc.assert_successful_put_get()

        lastExecutedValST = await bft_network.metrics.get(3, *lastExecutedKey)

        for i in range(10):
            await skvbc.send_write_kv_set()

        with trio.fail_after(seconds=20):
            for replica_id in {3}:
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            lastExecutedValSTAfter = await bft_network.metrics.get(3, *lastExecutedKey)
                            if lastExecutedValSTAfter ==  lastExecutedValST:
                                continue
                        except trio.TooSlowError:
                            self.assertTrue(False)
                        else:
                            self.assertGreater(lastExecutedValSTAfter, lastExecutedValST)
                            break

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, rotate_keys=True)
    async def test_reload_fast_path_after_key_exchange(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        for i in range(20):
            await skvbc.send_write_kv_set()

        lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
        lastExecutedValBefore = await bft_network.metrics.get(3, *lastExecutedKey)
        bft_network.stop_replica(3)
        bft_network.start_replica(3)
        for i in range(10):
            await skvbc.send_write_kv_set()

        with trio.fail_after(seconds=20):
            for replica_id in {3}:
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            lastExecutedValAfter = await bft_network.metrics.get(3, *lastExecutedKey)
                            if lastExecutedValAfter ==  lastExecutedValBefore:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to exchange keys on start")
                            self.assertTrue(False)
                        else:
                            break

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, rotate_keys=True)
    async def test_reload_slows_path_after_key_exchange(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        bft_network.stop_replica(2)
        for i in range(20):
            await skvbc.send_write_kv_set()

        lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
        lastExecutedValBefore = await bft_network.metrics.get(3, *lastExecutedKey)
        bft_network.stop_replica(3)
        bft_network.start_replica(3)
        for i in range(10):
            await skvbc.send_write_kv_set()

        with trio.fail_after(seconds=20):
            for replica_id in {3}:
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            lastExecutedValAfter = await bft_network.metrics.get(3, *lastExecutedKey)
                            if lastExecutedValAfter ==  lastExecutedValBefore:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to exchange keys on start")
                            self.assertTrue(False)
                        else:
                            break

# need to test view change within the first window e.g. view change at sn  == 70

        
        
                    
                    

   
