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

from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, with_constant_load
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc

import util.bft_network_partitioning as net
import util.eliot_logging as log

SKVBC_INIT_GRACE_TIME = 2
BATCH_SIZE = 4
INDEFINITE_BATCH_WRITES_TIMEOUT = 1 * BATCH_SIZE
NUM_OF_SEQ_WRITES = 25
NUM_OF_PARALLEL_WRITES = 100
MAX_CONCURRENCY = 10
SHORT_REQ_TIMEOUT_MILLI = 3000
LONG_REQ_TIMEOUT_MILLI = 15000

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
            "-v", view_change_timeout_milli
            ]

def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper


class SkvbcBatchPreExecutionTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    async def send_single_batch_write_with_pre_execution_and_kv(self, skvbc, client, batch_size, long_exec=False):
        msg_batch = []
        batch_seq_nums = []
        for i in range(batch_size):
            readset = set()
            writeset = self.writeset(skvbc, 2)
            msg_batch.append(skvbc.write_req(readset, writeset, 0, long_exec))
            seq_num = client.req_seq_num.next()
            batch_seq_nums.append(seq_num)
        replies = await client.write_batch(msg_batch, batch_seq_nums)
        for seq_num, reply_msg in replies.items():
            self.assertTrue(skvbc.parse_reply(reply_msg.get_common_data()).success)

    def writeset(self, skvbc, max_size, keys=None):
        writeset_keys = skvbc.random_keys(random.randint(0, max_size)) if keys is None else keys
        writeset_values = skvbc.random_values(len(writeset_keys))
        return list(zip(writeset_keys, writeset_values))

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_sequential_pre_process_requests(self, bft_network, tracker):
        """
        Use a random client to launch one batch pre-process request at a time and ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await skvbc.send_write_kv_set_batch(client, 2, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(0, NUM_OF_SEQ_WRITES * BATCH_SIZE)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_concurrent_pre_process_requests(self, bft_network, tracker):
        """
        Launch concurrent requests from different clients in parallel. Ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = NUM_OF_PARALLEL_WRITES
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        wr = await skvbc.run_concurrent_batch_ops(num_of_requests, BATCH_SIZE)
        self.assertTrue(wr >= num_of_requests)

        await bft_network.assert_successful_pre_executions_count(0, wr * BATCH_SIZE)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_long_time_executed_pre_process_request(self, bft_network, tracker):
        """
        Launch pre-process request with a long-time execution and ensure that created blocks are as expected
        and no view-change was triggered.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        client = bft_network.random_client()
        client.config = client.config._replace(
            req_timeout_milli=LONG_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        await skvbc.send_write_kv_set_batch(client, 2, BATCH_SIZE, long_exec=True)

        last_block = await tracker.get_last_block_id(client)
        self.assertEqual(last_block, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(0, BATCH_SIZE)

        with trio.move_on_after(seconds=1):
            await skvbc.send_indefinite_ops(write_weight=1)

        initial_primary = 0
        with trio.move_on_after(seconds=15):
            while True:
                await bft_network.wait_for_view(replica_id=initial_primary,
                                                expected=lambda v: v == initial_primary,
                                                err_msg="Make sure the view did not change.")
                await trio.sleep(seconds=5)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_long_request_with_constant_load(self, bft_network, skvbc, constant_load):
        """
        In this test we make sure a long-running request executes
        concurrently with a constant system load in the background.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        client = bft_network.random_client()
        client.config = client.config._replace(
            req_timeout_milli=LONG_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )

        await self.send_single_batch_write_with_pre_execution_and_kv(
            skvbc, client, BATCH_SIZE, long_exec=True)

        # Wait for some background "constant load" requests to execute
        await trio.sleep(seconds=5)

        await bft_network.assert_successful_pre_executions_count(0, BATCH_SIZE)

        # Let's just check no view change occurred in the meantime
        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure the view did not change.")

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_view_change(self, bft_network, tracker):
        """
        Crash the primary replica and verify that the system triggers a view change and moves to a new view.
        """
        bft_network.start_all_replicas()

        await trio.sleep(5)
        await bft_network.init_preexec_count()

        clients = bft_network.clients.values()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        for client in clients:
            await skvbc.send_write_kv_set_batch(client, 2, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(0, len(clients) * BATCH_SIZE)

        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure we are in the initial view before crashing the primary.")

        last_block = await tracker.get_last_block_id(client)
        expected_last_block =  len(clients) * BATCH_SIZE
        with trio.move_on_after(seconds=10):
            while last_block < expected_last_block:
                last_block = await tracker.get_last_block_id(client)
                await trio.sleep(seconds=1)
        self.assertEqual(last_block, expected_last_block)
        bft_network.stop_replica(initial_primary)

        try:
            with trio.move_on_after(seconds=INDEFINITE_BATCH_WRITES_TIMEOUT):
                await skvbc.send_indefinite_batch_writes(BATCH_SIZE)
        except trio.TooSlowError:
            pass
        finally:
            expected_next_primary = 1
            await bft_network.wait_for_view(replica_id=random.choice(bft_network.all_replicas(without={0})),
                                            expected=lambda v: v == expected_next_primary,
                                            err_msg="Make sure view change has been triggered.")
            await skvbc.send_write_kv_set_batch(client, 2, BATCH_SIZE)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_parallel_tx_after_f_nonprimary_crash(self, bft_network, tracker):
        '''
        Crash f nonprimary replicas and submit X parallel write submissions.
        Block processing of the network should be unaffected with f-count interruption.
        Final block length should match submitted transactions count exactly.
        '''
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        read_client = bft_network.random_client()
        submit_clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = 10 * len(submit_clients) # each client will send 10 tx
        nonprimaries = bft_network.all_replicas(without={0}) # primary index is 0
        crash_targets = random.sample(nonprimaries, bft_network.config.f) # pick random f to crash
        bft_network.stop_replicas(crash_targets) # crash chosen nonprimary replicas
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        wr = await skvbc.run_concurrent_batch_ops(num_of_requests, BATCH_SIZE)
        final_block_count = await tracker.get_last_block_id(read_client)
        print(f"final_block_count {final_block_count}")

        log.log_message(message_type=f"Randomly picked replica indexes {crash_targets} (nonprimary) to be stopped.")
        log.log_message(message_type=f"Total of {num_of_requests} write pre-exec tx, "
                                     f"concurrently submitted through {len(submit_clients)} clients.")
        log.log_message(message_type=f"Finished at block {final_block_count}.")
        self.assertTrue(wr >= num_of_requests)

        await bft_network.assert_successful_pre_executions_count(0, wr * BATCH_SIZE)
