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

from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX


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
            "-s", statusTimerMilli,
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else ""]


class SkvbcFastPathTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    def setUp(self):
        # Whenever a replica goes down, all messages initially go via the slow path.
        # However, when an "evaluation period" elapses (set at 64 sequence numbers),
        # the system should return to the fast path.
        self.evaluation_period_seq_num = 64

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_fast_path_read_your_write(self, bft_network, tracker):
        """
        This test aims to check that the fast commit path is prevalent
        in the normal, synchronous case (no failed replicas, no network partitioning).

        First we write a series of K/V entries and tracked them using the tracker from the decorator.
        Then we check that, in the process, we have stayed on the fast path.

        Finally the decorator verifies the KV execution.
        """
        bft_network.start_all_replicas()
        write_weight = .50
        numops = 20

        nb_slow_path = await bft_network.num_of_slow_path()

        await tracker.run_concurrent_ops(num_ops=numops, write_weight=write_weight)

        await bft_network.assert_fast_path_prevalent(nb_slow_paths_so_far=nb_slow_path)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_fast_to_slow_path_transition(self, bft_network, tracker):
        """
        This test aims to check the correct transition from fast to slow commit path.

        First we write a series of K/V entries and track them using the tracker from the decorator, making sure
        we stay on the fast path.

        Once the first series of K/V writes have been processed, we bring down
        one of the replicas, which should trigger a transition to the slow path.

        We send a new series of K/V writes and make sure they
        have been processed using the slow commit path.

        Finally the decorator verifies the KV execution.
        """
        bft_network.start_all_replicas()

        write_weight = 0.5
        numops = 20

        _, fast_path_writes = await tracker.run_concurrent_ops(
            num_ops=numops, write_weight=write_weight)

        await bft_network.assert_fast_path_prevalent()

        unstable_replicas = bft_network.all_replicas(without={0})
        bft_network.stop_replica(
            replica_id=random.choice(unstable_replicas))

        await tracker.run_concurrent_ops(num_ops=numops, write_weight=write_weight)

        await bft_network.assert_slow_path_prevalent(as_of_seq_num=fast_path_writes+1)

    @with_trio
    @with_bft_network(start_replica_cmd,
                      num_clients=4,
                      selected_configs=lambda n, f, c: c >= 1)
    @verify_linearizability
    async def test_fast_path_resilience_to_crashes(self, bft_network, tracker):
        """
        In this test we check the fast path's resilience when up to "c" nodes fail.

        As a first step, we bring down no more than c replicas,
        triggering initially the slow path.

        Then we write a series of K/V entries and track them using the tracker from the decorator, making sure
        the fast path is eventually restored and becomes prevalent.

        Finally the decorator verifies the KV execution.
        """
        bft_network.start_all_replicas()
        unstable_replicas = bft_network.all_replicas(without={0})
        for _ in range(bft_network.config.c):
            replica_to_stop = random.choice(unstable_replicas)
            bft_network.stop_replica(replica_to_stop)
        write_weight = 0.5
        # make sure we first downgrade to the slow path...

        _, slow_path_writes = await tracker.run_concurrent_ops(
            num_ops=self.evaluation_period_seq_num-1, write_weight=1)
        await bft_network.assert_slow_path_prevalent()

        # ...but eventually (after the evaluation period), the fast path is restored!

        await tracker.run_concurrent_ops(num_ops=self.evaluation_period_seq_num*2/write_weight, write_weight=write_weight)

        await trio.sleep(5)
        await bft_network.assert_fast_path_prevalent(
            nb_slow_paths_so_far=slow_path_writes)

