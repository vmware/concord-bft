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
import unittest
import trio
import random

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, with_constant_load
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc

import util.bft_network_partitioning as net
import util.eliot_logging as log

SKVBC_INIT_GRACE_TIME = 2
BATCH_SIZE = 10
NUM_OF_SEQ_WRITES = 1
NUM_OF_PARALLEL_WRITES = 100
MAX_CONCURRENCY = 10
SHORT_REQ_TIMEOUT_MILLI = 3000
LONG_REQ_TIMEOUT_MILLI = 15000
BATCH_BY_REQ_NUM = "2"
MAX_REQ_NUM_IN_BATCH = 5
BATCH_FLUSH_PERIOD = "250"

def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout.

    Note each arguments is an element in a list.
    """

    status_timer_milli = "500"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            "-v", view_change_timeout_milli,
            "-b", BATCH_BY_REQ_NUM,
            "-q", str(MAX_REQ_NUM_IN_BATCH),
            "-z", BATCH_FLUSH_PERIOD,
            "-u", str(True)
            ]

class SkvbcBlockAccumulationTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    async def send_single_batch_write_with_pre_execution_and_kv(self, skvbc, client, batch_size, long_exec=False, ):
        msg_batch = []
        batch_seq_nums = []
        dic_writeset = {}
        final_block_ids = set()
        for i in range(batch_size):
            readset = set()
            writeset = self.writeset(skvbc, 2, dic_writeset)
            msg_batch.append(skvbc.write_req(readset, writeset, 0, long_exec))
            seq_num = client.req_seq_num.next()
            batch_seq_nums.append(seq_num)
        replies = await client.write_batch(msg_batch, batch_seq_nums)
        for seq_num, reply_msg in replies.items():
            self.assertTrue(skvbc.parse_reply(reply_msg.get_common_data()).success)
            reply = skvbc.parse_reply(reply_msg.get_common_data())
            final_block_ids.add(reply.last_block_id)
        return final_block_ids,dic_writeset

    def writeset(self, skvbc, max_size, dic_writeset, keys=None):
       write_set = [(skvbc.unique_random_key() , skvbc.random_value()),
                    (skvbc.unique_random_key() , skvbc.random_value())]
       dic_writeset.update(write_set)
       return write_set

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_batch_block_accumulation_request_block_write_validation(self, bft_network, skvbc, constant_load):
        """
        Launch a batch request and validate the block write keys.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        client = bft_network.random_client()
        client.config = client.config._replace(
            req_timeout_milli=SHORT_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )
        result = await self.send_single_batch_write_with_pre_execution_and_kv(
            skvbc, client, 3, long_exec=False)

        await bft_network.assert_successful_pre_executions_count(0, 3)

        for val in result[0]:
            readclient = bft_network.random_client()
            data = await readclient.read(skvbc.get_block_data_req(val))
            blocks = skvbc.parse_reply(data)
            final_result = set(blocks.items()).issubset(set(result[1].items()))
            self.assertTrue((final_result),
                        "Final Blockvalidation Failed.")

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_batch_block_accumulation_request_block_count_validation(self, bft_network, tracker):
        """
        Launch concurrent requests from different clients. Ensure that created accumulated block count is as expected.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = NUM_OF_PARALLEL_WRITES
        wr = await tracker.run_concurrent_batch_ops(num_of_requests, BATCH_SIZE, block_accumulation = True)
        self.assertTrue(wr >= num_of_requests)

        await bft_network.assert_successful_pre_executions_count(0, wr * BATCH_SIZE)
        computed_block_count = ((wr * BATCH_SIZE)/MAX_REQ_NUM_IN_BATCH);
        await trio.sleep(seconds=3)

        read_client = bft_network.random_client()

        final_block_count = await tracker.get_last_block_id(read_client)
        print(f"final_block_count {final_block_count}")
        print(f"computed_block_count =  {computed_block_count}")
        self.assertTrue((final_block_count < wr * BATCH_SIZE),
                        "Final Block Count is not as expected for Block Accumulation")
