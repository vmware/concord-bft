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
import unittest

from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc

NUM_OF_WRITES = 100
MAX_CONCURRENCY = 30
CONCURRENCY_LEVEL = "1"
MAX_REQS_SIZE_IN_BATCH = "300"
MAX_REQ_NUM_IN_BATCH = "3"
BATCH_FLUSH_PERIOD = "250"
BATCH_SELF_ADJUSTED = "0"
BATCH_BY_REQ_SIZE = "1"
BATCH_BY_REQ_NUM = "2"
BATCHING_POLICY = BATCH_SELF_ADJUSTED

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess. Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-y", CONCURRENCY_LEVEL,
            "-b", BATCHING_POLICY,
            "-m", MAX_REQS_SIZE_IN_BATCH,
            "-q", MAX_REQ_NUM_IN_BATCH,
            "-z", BATCH_FLUSH_PERIOD
            ]

class SkvbcConsensusBatchingPoliciesTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    async def launch_concurrent_requests(self, bft_network, tracker):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        rw = await skvbc.send_concurrent_ops(NUM_OF_WRITES, max_concurrency=MAX_CONCURRENCY, max_size=10, write_weight=0.9)
        self.assertTrue(rw[0] + rw[1] >= NUM_OF_WRITES)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_batching_by_req_num(self, bft_network, tracker):
        """
        This test verifies that BATCH_SELF_ADJUSTED consensus policy works
        """

        global BATCHING_POLICY
        BATCHING_POLICY = BATCH_SELF_ADJUSTED
        await self.launch_concurrent_requests(bft_network, tracker)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_batching_by_req_num(self, bft_network, tracker):
        """
        This test verifies that BATCH_BY_REQ_NUM consensus policy works
        """

        global BATCHING_POLICY
        BATCHING_POLICY = BATCH_BY_REQ_NUM
        await self.launch_concurrent_requests(bft_network, tracker)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_batching_by_reqs_size(self, bft_network, tracker):
        """
        This test verifies that BATCH_BY_REQ_SIZE consensus policy works
        """

        global BATCHING_POLICY
        BATCHING_POLICY = BATCH_BY_REQ_SIZE
        await self.launch_concurrent_requests(bft_network, tracker)
