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
import random
import unittest
from os import environ

import trio

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, with_constant_load
from util.skvbc_history_tracker import verify_linearizability
import util.eliot_logging as log

SKVBC_INIT_GRACE_TIME = 2
BATCH_SIZE = 4
NUM_OF_SEQ_WRITES = 25
NUM_OF_PARALLEL_WRITES = 100
MAX_CONCURRENCY = 10
LONG_REQ_TIMEOUT_MILLI = 15000
ASSERT_TIMEOUT_SEC = 90

def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    The replica is started with a short view change timeout.
    Note each arguments is an element in a list.
    """

    status_timer_milli = "500"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    cmd = [path,
           "-k", KEY_FILE_PREFIX,
           "-i", str(replica_id),
           "-s", status_timer_milli,
           "-v", view_change_timeout_milli,
           "-x"
           ]
    if(replica_id == 0) :
        cmd.extend(["-g", "MangledPreProcessResultMsgStrategy"])

    return cmd

def start_replica_cmd_assymetric_communication(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    The replica is started with a short view change timeout.
    Note each arguments is an element in a list.
    """

    status_timer_milli = "500"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    cmd = [path,
           "-k", KEY_FILE_PREFIX,
           "-i", str(replica_id),
           "-s", status_timer_milli,
           "-v", view_change_timeout_milli,
           "-x"
           ]
    if(replica_id == 0) :
        cmd.extend(["-d", "-g", "MangledPreProcessResultMsgStrategy"])

    return cmd


class SkvbcPrimaryByzantinePreExecutionTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    async def check_viewchange_noexcept(self, bft_network, initial_primary, viewchange_timeout_secs=5):
        try:
            with trio.move_on_after(seconds=4*viewchange_timeout_secs):
                await bft_network.wait_for_view(
                    replica_id=random.choice(
                        bft_network.all_replicas(without={initial_primary})),
                    expected=lambda v: v != initial_primary,
                    err_msg="Make sure view change has occurred."
                )
                new_primary = await bft_network.get_current_primary()
                await bft_network.init_preexec_count(new_primary)
                await trio.sleep(seconds=viewchange_timeout_secs)
                return new_primary
        except trio.TooSlowError:
            return self.check_viewchange_noexcept(bft_network, initial_primary, viewchange_timeout_secs)

    async def send_single_read(self, skvbc, client):
        req = skvbc.read_req(skvbc.random_keys(1))
        await client.read(req)

    async def send_single_write_with_pre_execution_and_kv(self, skvbc, write_set, client, long_exec=False):
        reply = await client.write(skvbc.write_req([], write_set, 0, long_exec), pre_process=True)
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)

    async def send_single_write_with_pre_execution(self, skvbc, client, long_exec=False):
        write_set = [(skvbc.random_key(), skvbc.random_value()),
                     (skvbc.random_key(), skvbc.random_value())]
        await self.send_single_write_with_pre_execution_and_kv(skvbc, write_set, client, long_exec)

    async def run_concurrent_pre_execution_requests(self, skvbc, clients, num_of_requests, write_weight=.90):
        sent = 0
        write_count = 0
        read_count = 0
        while sent < num_of_requests:
            async with trio.open_nursery() as nursery:
                for client in clients:
                    if random.random() <= write_weight:
                        nursery.start_soon(self.send_single_write_with_pre_execution, skvbc, client, True)
                        write_count += 1
                    else:
                        nursery.start_soon(self.send_single_read, skvbc, client)
                        read_count += 1
                    sent += 1
                    if sent == num_of_requests:
                        break
            await trio.sleep(.1)
        return read_count + write_count

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_sequential_pre_process_requests(self, bft_network, tracker):
        """
        Use a random client to launch one batch pre-process request at a time and ensure that created blocks are as expected.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        client = bft_network.random_client()

        await skvbc.send_tracked_write_batch(client, 10, BATCH_SIZE)

        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await skvbc.send_tracked_write_batch(client, 10, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(new_primary, NUM_OF_SEQ_WRITES * BATCH_SIZE,
                                                                 1.0, ASSERT_TIMEOUT_SEC)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_concurrent_pre_process_requests(self, bft_network, tracker):
        """
        Launch concurrent requests from different clients in parallel. Ensure that created blocks are as expected.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        await skvbc.run_concurrent_batch_ops(4, BATCH_SIZE)
        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        num_of_requests = NUM_OF_PARALLEL_WRITES
        wr = await skvbc.run_concurrent_batch_ops(num_of_requests, BATCH_SIZE)
        self.assertGreaterEqual(wr, num_of_requests)
        await bft_network.assert_successful_pre_executions_count(new_primary, wr * BATCH_SIZE,
                                                                 0.8, ASSERT_TIMEOUT_SEC)

    @with_trio
    @with_bft_network(start_replica_cmd_assymetric_communication, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_sequential_pre_process_requests_with_assymetric_comm(self, bft_network, tracker):
        """
        Use a random client to launch one batch pre-process request at a time and ensure that created blocks are as expected.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        client = bft_network.random_client()
        await skvbc.send_tracked_write_batch(client, 10, BATCH_SIZE)
        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await skvbc.send_tracked_write_batch(client, 10, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(new_primary,
                                                                 NUM_OF_SEQ_WRITES * BATCH_SIZE,
                                                                 0.8, ASSERT_TIMEOUT_SEC)

    @with_trio
    @with_bft_network(start_replica_cmd_assymetric_communication, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_concurrent_pre_process_requests_with_assymetric_comm(self, bft_network, tracker):
        """
        Launch concurrent requests from different clients in parallel. Ensure that created blocks are as expected.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        await skvbc.run_concurrent_batch_ops(4, BATCH_SIZE)
        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = NUM_OF_PARALLEL_WRITES
        wr = await skvbc.run_concurrent_batch_ops(num_of_requests, BATCH_SIZE)
        self.assertGreaterEqual(wr, num_of_requests)
        await bft_network.assert_successful_pre_executions_count(new_primary, wr * BATCH_SIZE, 0.8,
                                                                 ASSERT_TIMEOUT_SEC)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_long_time_executed_pre_process_request(self, bft_network, tracker):
        """
        Launch pre-process request with a long-time execution and ensure that created blocks are as expected
        and no view-change was triggered.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        client = bft_network.random_client()
        client.config = client.config._replace(
            req_timeout_milli=LONG_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )

        await skvbc.send_tracked_write(client, 2, long_exec=True)

        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        last_block = await tracker.get_last_block_id(client)
        self.assertEqual(last_block, 1)

        with trio.move_on_after(seconds=1):
            await skvbc.send_indefinite_tracked_ops(write_weight=1)

        with trio.move_on_after(seconds=15):
            while True:
                await bft_network.wait_for_view(replica_id=new_primary,
                                                expected=lambda v: v == new_primary,
                                                err_msg="Make sure the view did not change.")
                await trio.sleep(seconds=5)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @with_constant_load
    async def test_long_request_with_constant_load(self, bft_network, skvbc, constant_load):
        """
        In this test we make sure a long-running request executes
        concurrently with a constant system load in the background.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        write_set = [(skvbc.random_key(), skvbc.random_value()),
                     (skvbc.random_key(), skvbc.random_value())]

        client = bft_network.random_client()
        client.config = client.config._replace(
            req_timeout_milli=2*LONG_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )

        await self.send_single_write_with_pre_execution_and_kv(
            skvbc, write_set, client, long_exec=True)

        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        # Let's just check no view change occurred in the meantime
        await bft_network.wait_for_view(replica_id=new_primary,
                                        expected=lambda v: v == new_primary,
                                        err_msg="Make sure the view did not change.")

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_parallel_tx_after_fminus1_nonprimary_crash(self, bft_network, tracker):
        '''
        Crash f-1 nonprimary replicas and submit X parallel write submissions.
        Block processing of the network should be unaffected with f-count interruption.
        Final block length should match submitted transactions count exactly.
        '''
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        initial_primary = 0
        await skvbc.run_concurrent_ops(2, write_weight=1)
        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        read_client = bft_network.random_client()
        submit_clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = 10 * len(submit_clients) # each client will send 10 tx
        self.assertNotEqual(initial_primary, new_primary)
        nonprimaries = bft_network.all_replicas(without={initial_primary, new_primary}) # primary index is 0
        crash_targets = random.sample(nonprimaries, bft_network.config.f - 1) # pick random f-1 to crash
        bft_network.stop_replicas(crash_targets) # crash chosen nonprimary replicas

        rw = await skvbc.run_concurrent_ops(num_of_requests, write_weight=1)
        final_block_count = await tracker.get_last_block_id(read_client)

        log.log_message(message_type=f"Randomly picked replica indexes {crash_targets} (nonprimary) to be stopped.")
        log.log_message(message_type=f"Total of {num_of_requests} write pre-exec tx, "
                                     f"concurrently submitted through {len(submit_clients)} clients.")
        log.log_message(message_type=f"Finished at block {final_block_count}.")
        self.assertGreaterEqual(rw[0] + rw[1], num_of_requests)

        await bft_network.assert_successful_pre_executions_count(new_primary, rw[1],
                                                                 0.5, ASSERT_TIMEOUT_SEC)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=False)
    async def test_conflicting_requests(self, bft_network, tracker):
        """
        Launch pre-process conflicting request and make sure that conflicting requests are not committed
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        read_client = bft_network.random_client()
        start_block = await tracker.get_last_block_id(read_client)
        await skvbc.run_concurrent_conflict_ops(2, write_weight=1)
        await self.check_viewchange_noexcept(bft_network, 0, 10)

        ops = 50

        try:
            with trio.move_on_after(seconds=30):
                await skvbc.run_concurrent_conflict_ops(ops, write_weight=1)
        except trio.TooSlowError:
            pass
        last_block = await tracker.get_last_block_id(read_client)

        # We produced at least one conflict.
        self.assertLess(last_block, start_block + ops)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=False)
    async def test_conflicting_requests_with_fminus1_failures(self, bft_network, tracker):
        """
        Launch pre-process conflicting request and make sure that conflicting requests are not committed
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_primary = 0
        crashed_replicas = bft_network.random_set_of_replicas(f-1, without={initial_primary})
        bft_network.stop_replicas(replicas=crashed_replicas)

        read_client = bft_network.random_client()
        start_block = await tracker.get_last_block_id(read_client)

        await skvbc.run_concurrent_conflict_ops(2, write_weight=1)
        await self.check_viewchange_noexcept(bft_network, initial_primary, 10)

        ops = 50
        try:
            with trio.move_on_after(seconds=30):
                await skvbc.run_concurrent_conflict_ops(ops, write_weight=1)
        except trio.TooSlowError:
            pass

        last_block = await tracker.get_last_block_id(read_client)

        # We produced at least one conflict.
        self.assertLess(last_block, start_block + ops)

if __name__ == '__main__':
    unittest.main()
