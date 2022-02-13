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
import time
import unittest

import trio

from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, ConsensusPathType
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc
from util import eliot_logging as log
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from util.skvbc import SimpleKVBCProtocol
    from util.pyclient.bft_client import BftClient
    from util.bft import BftTestNetwork
    from util.skvbc_history_tracker import SkvbcTracker

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    view_change_timeout_milli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", view_change_timeout_milli,
            "-e", str(True)
            ]

class SkvbcCommitPathTest(ApolloTest):
    __test__ = False  # so that PyTest ignores this test scenario
    # This is constant is shared between the c++ (EvaluationPeriod = 64) and the python code
    # TODO: Share properly via a configuration file
    EVALUATION_PERIOD_SEQUENCES = 64
    # There is no need to rotate keys as it is unrelated to path transition and takes time
    ROTATE_KEYS = False

    @staticmethod
    async def send_kvs_sequentially(skvbc: 'SimpleKVBCProtocol', kv_count: int, client: 'BftClient' = None):
        """
        Reaches a consensus over kv_count blocks, Waits for execution reply message after each block
        so that multiple client requests are not batched
        """
        client = client or skvbc.bft_network.random_client()
        for i in range(kv_count):
            await skvbc.send_write_kv_set(client=client, kv=[(b'A' * i, b'')], description=f'{i}')

    async def wait_for_stable_state(self, skvbc: 'SimpleKVBCProtocol', timeout_secs: int,
                                    sleep_time: float = 1, client: 'BftClient' = None):
        with trio.fail_after(timeout_secs):
            while True:
                try:
                    await self.send_kvs_sequentially(skvbc, 1, client)
                    break
                except:
                    await trio.sleep(sleep_time)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=ROTATE_KEYS)
    @verify_linearizability()
    async def test_fast_path_is_default(self, bft_network: 'BftTestNetwork', tracker: 'SkvbcTracker'):
        """
        This test aims to check that the fast commit path is prevalent
        in the normal, synchronous case (no failed replicas, no network partitioning).

        First we write a series of K/V entries and check that in the process
        we have stayed on the fast path.

        Finally the decorator verifies the KV execution.
        """
        op_count = 5
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        await bft_network.wait_for_consensus_path(path_type=ConsensusPathType.OPTIMISTIC_FAST,
                                                  run_ops=lambda: self.send_kvs_sequentially(skvbc, op_count),
                                                  threshold=op_count)
