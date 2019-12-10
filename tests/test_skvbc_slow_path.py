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
    viewChangeTimeoutMilli = "3000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli]


class SkvbcSlowPathTest(unittest.TestCase):

    def setUp(self):
        # Whenever a replica goes down, all messages initially go via the slow path.
        # However, when an "evaluation period" elapses (set at 64 sequence numbers),
        # the system should return to the fast path.
        self.evaluation_period_seq_num = 64

    def test_persistent_slow_path(self):
        """
        Start a full BFT network with c=0 then bring one replica down.

        Write a batch of known K/V entries.
        
        Then we check that messages from now on are processed on the slow commit path.
        (note that this is not the case for c>0 where the BFT network eventually
        returns to the fast commit path)
        
        Finally we check if a known K/V has been executed and readable.
        """
        trio.run(self._test_persistent_slow_path)

    async def _test_persistent_slow_path(self):
        for bft_config in bft.interesting_configs(lambda n, f, c: c == 0):
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
                bft_network.stop_replica(
                    replica=random.choice(unstable_replicas))

                for _ in range(self.evaluation_period_seq_num*2):
                    key, val = await skvbc.write_known_kv()

                await bft_network.assert_slow_path_prevalent(as_of_seq_num=1)

                await skvbc.assert_kv_write_executed(key, val)

    def test_slow_to_fast_path_transition(self):
        """
        This test aims to check that the system correctly restores
        the fast path once all failed nodes are back online.

        First we bring down a non-primary replica, and make sure 
        a batch of K/V entries is processed on the slow path.

        Once the first batch of K/V writes have been processed, we bring the
        failed replica back up, which should restore the system's ability to
        process requests via the fast commit path.

        We send a new batch of K/V writes and make sure they
        have been processed using the fast path.

        Finally we check if a known K/V has been executed and readable.
        """
        trio.run(self._test_slow_to_fast_path_transition)

    async def _test_slow_to_fast_path_transition(self):
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

                unstable_replicas = bft_network.all_replicas(without={0})
                crashed_replica = random.choice(unstable_replicas)
                bft_network.stop_replica(crashed_replica)

                for _ in range(10):
                    await skvbc.write_known_kv()

                await bft_network.assert_slow_path_prevalent(as_of_seq_num=1)

                bft_network.start_replica(crashed_replica)

                for _ in range(10):
                    key, val = await skvbc.write_known_kv()

                await bft_network.assert_fast_path_prevalent(nb_slow_paths_so_far=10)

                await skvbc.assert_kv_write_executed(key, val)

    def test_slow_path_view_change(self):
        """
        This test validates the BFT engine's transition to the slow path
        when the primary goes down. This effectively triggers a view change in the slow path.

        First we write a batch of known K/V entries.

        We check those entries have been processed via the fast commit path.

        We stop the primary and send a batch of requests, triggering slow path & view change.

        We bring the primary back up.

        We make sure the second batch of requests have been processed via the slow path.
        """
        trio.run(self._test_slow_path_view_change)

    async def _test_slow_path_view_change(self):
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

                bft_network.stop_replica(0)

                with trio.move_on_after(seconds=5):
                    async with trio.open_nursery() as nursery:
                        nursery.start_soon(skvbc.send_indefinite_write_requests)

                bft_network.start_replica(0)

                await self._wait_for_slow_path_after_view_change(bft_network, as_of_seq_num=10)

    async def _wait_for_slow_path_after_view_change(self, bft_network, as_of_seq_num):
        with trio.fail_after(seconds=5):
            while True:
                with trio.move_on_after(seconds=.5):
                    try:
                        await bft_network.assert_slow_path_prevalent(as_of_seq_num)
                    except KeyError:
                        # metrics not yet available, continue looping
                        continue
                    else:
                        # slow path prevalent - done.
                        break
