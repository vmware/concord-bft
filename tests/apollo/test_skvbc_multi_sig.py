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

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability


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
            "-e", str(True),
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcMultiSig(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    def setUp(self):
        self.evaluation_period_seq_num = 64

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7    )
    @verify_linearizability()
    async def test_happy_initial_key_exchange(self, bft_network,tracker):
        """
        Validates that if all replicas are up and all key-exchnage msgs reached consensus via the fast path
        then the counter of the exchanged keys is equal to the cluster size in all replicas.
        """
        bft_network.start_all_replicas()

        with trio.fail_after(seconds=20):
            for replica_id in range(bft_network.config.n):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            key = ['KeyManager', 'Counters', 'KeyExchangedOnStartCounter']
                            value = await bft_network.metrics.get(replica_id, *key)
                            if value < 7:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to exchange keys on start")
                            self.assertTrue(False)
                        else:
                            self.assertEqual(value, 7)
                            break
        
       

        
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)   
    async def test_rough_initial_key_exchange(self, bft_network):
        """
        Validates that if not all replicas are up, then key-exchnage msgs will be dropped and no execution will happen.
        then when all replicas are available, key exchanged will be performed.
        """
        
        replicas = bft_network.random_set_of_replicas(6, without={2})
        bft_network.start_replicas(replicas)
        
        time.sleep(5)

        for replica_id in replicas:
            key = ['KeyManager', 'Counters', 'KeyExchangedOnStartCounter']
            value = await bft_network.metrics.get(replica_id, *key)
            lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
            lastExecutedVal = await bft_network.metrics.get(replica_id, *lastExecutedKey)
            self.assertEqual(value, 0)
            self.assertGreater(lastExecutedVal, 0)

        bft_network.start_replica(2)
        time.sleep(2)

        
        with trio.fail_after(seconds=20):
            for replica_id in range(bft_network.config.n):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            key = ['KeyManager', 'Counters', 'KeyExchangedOnStartCounter']
                            value = await bft_network.metrics.get(replica_id, *key)
                            if value < 7:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to exchange keys on start")
                            self.assertTrue(False)
                        else:
                            self.assertEqual(value, 7)
                            break

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)   
    @verify_linearizability()
    async def test_no_request_processing_till_initial_key_exchange(self, bft_network,tracker):
        replicas = bft_network.random_set_of_replicas(6, without={2})
        bft_network.start_replicas(replicas)
        
        time.sleep(1)
            

        write_weight = 1
        await tracker.run_concurrent_ops(num_ops=10, write_weight=write_weight)

        key = ['KeyManager', 'Counters', 'KeyExchangedOnStartCounter']
        value = await bft_network.metrics.get(0, *key)
        self.assertEqual(value, 0)

        lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
        lastExecutedVal = await bft_network.metrics.get(0, *lastExecutedKey)
        keyExDroppdMsgsKey = ['KeyManager', 'Counters', 'DroppedMsgsCounter']
        keyExDroppdMsgsValue = await bft_network.metrics.get(0, *keyExDroppdMsgsKey)
        self.assertGreaterEqual(keyExDroppdMsgsValue,lastExecutedVal)
        
        bft_network.start_replica(2)
        
        with trio.fail_after(seconds=120):
            for replica_id in range(bft_network.config.n):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            key = ['KeyManager', 'Counters', 'KeyExchangedOnStartCounter']
                            value = await bft_network.metrics.get(replica_id, *key)
                            if value < 7:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to exchange keys on start")
                            self.assertTrue(False)
                        else:
                            self.assertEqual(value, 7)
                            break

       
        keyExDroppdMsgsKey = ['KeyManager', 'Counters', 'DroppedMsgsCounter']
        keyExDroppdMsgsValueBefore = await bft_network.metrics.get(0, *keyExDroppdMsgsKey)                       
        lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
        lastExecutedVal = await bft_network.metrics.get(replica_id, *lastExecutedKey)
        await tracker.run_concurrent_ops(num_ops=100, write_weight=write_weight)
        
        with trio.fail_after(seconds=40):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            lastExecutedValAfter = await bft_network.metrics.get(replica_id, *lastExecutedKey)
                            keyExDroppdMsgsValueAfter = await bft_network.metrics.get(0, *keyExDroppdMsgsKey)  
                            if lastExecutedValAfter == lastExecutedVal:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"couldn't prove that msgs were processed fter key exchange")
                            self.assertTrue(False)
                        else:
                            self.assertGreater(lastExecutedValAfter,lastExecutedVal)
                            self.assertEqual(keyExDroppdMsgsValueBefore, keyExDroppdMsgsValueAfter)
                            break


# need to test view change within the first window e.g. view change at sn  == 70

        
        
                    
                    

   