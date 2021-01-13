# Concord
#
# Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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
import trio
import random

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, with_constant_load
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc

import util.bft_network_partitioning as net
import util.eliot_logging as log

SKVBC_INIT_GRACE_TIME = 2
BATCH_SIZE = 3
NUM_OF_SEQ_WRITES = 10
NUM_OF_PARALLEL_WRITES = 1000
MAX_CONCURRENCY = 10
SHORT_REQ_TIMEOUT_MILLI = 3000
LONG_REQ_TIMEOUT_MILLI = 15000

def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).
    Note each arguments is an element in a list.
    """

    status_timer_milli = "500"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            "-v", view_change_timeout_milli,
            "-p",
            "-t", os.environ.get('STORAGE_TYPE')
            ]

def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper


class SkvbcBatchPreExecutionTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_sequential_pre_process_requests(self, bft_network, tracker):
        """
        Use a random client to launch one pre-process request in time and ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await tracker.send_tracked_write_batch(client, 2, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(0, NUM_OF_SEQ_WRITES * BATCH_SIZE)
