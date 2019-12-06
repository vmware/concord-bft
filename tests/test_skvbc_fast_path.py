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

import trio

import unittest

from util import bft
from util import skvbc as kvbc

KEY_FILE_PREFIX = "replica_keys_"


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli]


class SkvbcFastPathTest(unittest.TestCase):

    def setUp(self):
        # Whenever a replica goes down, all messages initially go via the slow path.
        # However, when an "evaluation period" elapses (set at 64 sequence numbers),
        # the system should return to the fast path.
        self.evaluation_period_seq_num = 64

    def test_fast_path_read_your_write(self):
        """
        This test aims to check that the fast commit path is prevalent
        in the normal, synchronous case (no failed replicas, no network partitioning).

        First we write a series of known K/V entries.
        Then we check that, in the process, we have stayed on the fast path.

        Finally we check if a known K/V has been executed.
        """
        trio.run(self._test_fast_path_read_your_write)

    async def _test_fast_path_read_your_write(self):
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                for _ in range(10):
                    key, val = await skvbc.write_known_kv()

                await bft_network.assert_fast_path_prevalent()

                await skvbc.assert_kv_write_executed(key, val)

    def test_fast_to_slow_path_transition(self):
        """
        This test aims to check the correct transition from fast to slow commit path.

        First we write a series of known K/V entries, making sure
        we stay on the fast path.

        Once the first series of K/V writes have been processed, we bring down
        one of the replicas, which should trigger a transition to the slow path.

        We send a new series of K/V writes and make sure they
        have been processed using the slow commit path.

        Finally we check if a known K/V has been executed.
        """
        trio.run(self._test_fast_to_slow_path_transition)

    async def _test_fast_to_slow_path_transition(self):
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                for _ in range(10):
                    await skvbc.write_known_kv()

                await bft_network.assert_fast_path_prevalent()

                unstable_replicas = bft_network.all_replicas(without={0})
                bft_network.stop_replica(
                    replica=random.choice(unstable_replicas))

                for _ in range(10):
                    key, val = await skvbc.write_known_kv()

                await bft_network.assert_slow_path_prevalent(as_of_seq_num=10)

                await skvbc.assert_kv_write_executed(key, val)

    def test_fast_path_resilience_to_crashes(self):
        """
        In this test we check the fast path's resilience when up to "c" nodes fail.

        As a first step, we bring down no more than c replicas,
        triggering initially the slow path.

        Then we write a series of known K/V entries, making sure
        the fast path is eventually restored and becomes prevalent.

        Finally we check if a known K/V write has been executed.
        """
        trio.run(self._test_fast_path_resilience_to_crashes)

    async def _test_fast_path_resilience_to_crashes(self):
        for bft_config in bft.interesting_configs(lambda n, f, c: c >= 1):
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                unstable_replicas = bft_network.all_replicas(without={0})
                for _ in range(config.c):
                    replica_to_stop = random.choice(unstable_replicas)
                    bft_network.stop_replica(replica_to_stop)

                # make sure we first downgrade to the slow path...
                for _ in range(self.evaluation_period_seq_num):
                    await skvbc.write_known_kv()
                await bft_network.assert_slow_path_prevalent()

                # ...but eventually (after the evaluation period), the fast path is restored!
                for _ in range(self.evaluation_period_seq_num+1,
                               self.evaluation_period_seq_num*2):
                    key, val = await skvbc.write_known_kv()
                await bft_network.assert_fast_path_prevalent(
                    as_of_seq_num=self.evaluation_period_seq_num+1,
                    nb_slow_paths_so_far=self.evaluation_period_seq_num)

                await skvbc.assert_kv_write_executed(key, val)
