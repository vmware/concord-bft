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
import os
import unittest
import trio
import random

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, with_constant_load, skip_for_tls
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc

import util.bft_network_partitioning as net
import util.eliot_logging as log

SKVBC_INIT_GRACE_TIME = 2
NUM_OF_SEQ_WRITES = 100
NUM_OF_PARALLEL_WRITES = 1000
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

    # Decide from the environment if PreExecution result authentication feature should be enabled or not
    pre_exec_result_auth_enabled = os.environ.get("PRE_EXEC_RESULT_AUTH_ENABLED", default="False").lower() == "true"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            "-v", view_change_timeout_milli,
            "-x" if pre_exec_result_auth_enabled else ""
            ]

def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper


class SkvbcPreExecutionTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

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
                        nursery.start_soon(self.send_single_write_with_pre_execution, skvbc, client)
                        write_count += 1
                    else:
                        nursery.start_soon(self.send_single_read, skvbc, client)
                        read_count += 1
                    sent += 1
                    if sent == num_of_requests:
                        break
            await trio.sleep(.1)
        return read_count + write_count

    @unittest.skip("Unstable test - BC-17831")
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_sequential_pre_process_requests(self, bft_network, tracker):
        """
        Use a random client to launch one pre-process request in time and ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
            await skvbc.send_write_kv_set(client, max_set_size=2)
        await bft_network.assert_successful_pre_executions_count(0, NUM_OF_SEQ_WRITES)

    @with_trio
    @with_bft_network(start_replica_cmd)
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
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        rw = await skvbc.run_concurrent_ops(num_of_requests, write_weight=0.9)
        self.assertTrue(rw[0] + rw[1] >= num_of_requests)

        await bft_network.assert_successful_pre_executions_count(0, rw[1])

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
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_write_kv_set(client, max_set_size=2, long_exec=True)

        last_block = await tracker.get_last_block_id(client)
        self.assertEqual(last_block, 1)

        await bft_network.assert_successful_pre_executions_count(0, 1)

        with trio.move_on_after(seconds=1):
            await skvbc.send_indefinite_ops(write_weight=1)

        initial_primary = 0
        with trio.move_on_after(seconds=15):
            while True:
                await bft_network.wait_for_view(replica_id=initial_primary,
                                                expected=lambda v: v == initial_primary,
                                                err_msg="Make sure the view did not change.")
                await trio.sleep(seconds=5)

    @unittest.skip("Reproduces BC-4982 outside of LR")
    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("3000"),
                      selected_configs=lambda n, f, c: n == 7)
    async def test_view_change_during_long_request(self, bft_network):
        """
        1) Select two clients - one that runs "long" and one that runs "short" requests
        2) Set the timeout of the long client to LONG_REQ_TIMEOUT_MILLI
        3) Concurrently:
          3.1) Run a long executing request via the "long" client
          3.2) Trigger a view change during the long execution
        4) Run a short write using the "short" client (succeeds)
        5) Run a short write using the "long" client (fails because the long client is not freed in the pre-processor)
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        write_set = [(skvbc.random_key(), skvbc.random_value()),
                     (skvbc.random_key(), skvbc.random_value())]

        clients = list(bft_network.random_clients(MAX_CONCURRENCY))
        long_request_client = clients[0]
        short_request_client = clients[1]

        self.assertNotEqual(long_request_client, short_request_client)

        long_request_client.config = long_request_client.config._replace(
            req_timeout_milli=LONG_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )

        try:
            # Send long running request and trigger view change at the same time
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self._send_long_write, skvbc, write_set, long_request_client)
                await trio.sleep(seconds=1)
                nursery.start_soon(self._trigger_view_change, bft_network, short_request_client, skvbc, write_set)
        except trio.TooSlowError:
            pass

        long_request_client.config = long_request_client.config._replace(
            req_timeout_milli=5000,
            retry_timeout_milli=250
        )

        await trio.sleep(seconds=30)

        # the short client should have been released in the pre-processor
        await self.send_single_write_with_pre_execution_and_kv(
            skvbc, write_set, short_request_client, long_exec=False)

        # the long client should have been released in the pre-processor
        await self.send_single_write_with_pre_execution_and_kv(
            skvbc, write_set, long_request_client, long_exec=False)

    async def _send_long_write(self, skvbc, write_set, client):
        await self.send_single_write_with_pre_execution_and_kv(
            skvbc, write_set, client, long_exec=True)

    async def _trigger_view_change(self, bft_network, short_request_client, skvbc, write_set):
        initial_primary = await bft_network.get_current_primary()
        bft_network.stop_replica(initial_primary)
        try:
            with trio.move_on_after(seconds=1):
                await self.send_single_write_with_pre_execution_and_kv(
                    skvbc, write_set, short_request_client, long_exec=True)
        except trio.TooSlowError:
            pass
        finally:
            expected_next_primary = initial_primary + 1
            await bft_network.wait_for_view(replica_id=random.choice(bft_network.all_replicas(without={0})),
                                            expected=lambda v: v == expected_next_primary,
                                            err_msg="Make sure view change has been triggered.")

    @unittest.skip("Unstable test - BC-17831")
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

        write_set = [(skvbc.random_key(), skvbc.random_value()),
                     (skvbc.random_key(), skvbc.random_value())]

        client = bft_network.random_client()
        client.config = client.config._replace(
            req_timeout_milli=LONG_REQ_TIMEOUT_MILLI,
            retry_timeout_milli=1000
        )

        await self.send_single_write_with_pre_execution_and_kv(
            skvbc, write_set, client, long_exec=True)

        # Wait for some background "constant load" requests to execute
        await trio.sleep(seconds=5)

        await bft_network.assert_successful_pre_executions_count(0, 1)

        # Let's just check no view change occurred in the meantime
        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure the view did not change.")

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_view_change(self, bft_network, tracker):
        """
        Crash the primary replica and verify that the system triggers a view change and moves to a new view.
        """
        bft_network.start_all_replicas()

        await trio.sleep(5)
        await bft_network.init_preexec_count()

        clients = bft_network.clients.values()
        client = random.choice(list(clients))
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_write_kv_set(client, max_set_size=2)

        await bft_network.assert_successful_pre_executions_count(0, 1)

        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure we are in the initial view before crashing the primary.")

        last_block = await tracker.get_last_block_id(client)
        bft_network.stop_replica(initial_primary)

        try:
            with trio.move_on_after(seconds=1):
                await skvbc.send_indefinite_ops(write_weight=1, excluded_clients={client})
        except trio.TooSlowError:
            pass
        finally:
            expected_next_primary = 1
            await bft_network.wait_for_view(replica_id=random.choice(bft_network.all_replicas(without={0})),
                                            expected=lambda v: v == expected_next_primary,
                                            err_msg="Make sure view change has been triggered.")
            await skvbc.send_write_kv_set(client, max_set_size=2, raise_slowErrorIfAny=False)

    @with_trio
    @with_bft_network(start_replica_cmd)
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
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        rw = await skvbc.run_concurrent_ops(num_of_requests, write_weight=1)
        final_block_count = await tracker.get_last_block_id(read_client)

        log.log_message(message_type=f"Randomly picked replica indexes {crash_targets} (nonprimary) to be stopped.")
        log.log_message(message_type=f"Total of {num_of_requests} write pre-exec tx, "
              f"concurrently submitted through {len(submit_clients)} clients.")
        log.log_message(message_type=f"Finished at block {final_block_count}.")
        self.assertTrue(rw[0] + rw[1] >= num_of_requests)

        await bft_network.assert_successful_pre_executions_count(0, rw[1])

    @with_trio
    @with_bft_network(start_replica_cmd)
    @with_constant_load
    async def test_pre_execution_with_added_constant_load(self, bft_network, skvbc, constant_load):
        """
        Run a batch of concurrent pre-execution requests, while
        sending a constant "time service like" load on the normal execution path.

        This test validates that pre-execution and normal execution coexist correctly.
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        num_preexecution_requests = 200
        clients = bft_network.random_clients(MAX_CONCURRENCY)
        await self.run_concurrent_pre_execution_requests(
            skvbc, clients, num_preexecution_requests, write_weight=1)


        client = bft_network.random_client()
        current_block = skvbc.parse_reply(
            await client.read(skvbc.get_last_block_req()))

        self.assertTrue(current_block > num_preexecution_requests,
                        "Make sure all pre-execution requests were processed, in"
                        "addition to the constant load in the background.")

        await bft_network.assert_successful_pre_executions_count(0, num_preexecution_requests)

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_primary_isolation_from_replicas(self, bft_network, tracker):
        '''
        Isolate the from the other replicas, wait for view change and ensure the system is still able to make progress
        '''
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        with net.PrimaryIsolatingAdversary(bft_network) as adversary:
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            await adversary.interfere()

            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            expected_next_primary = 1
            await bft_network.wait_for_view(replica_id=random.choice(bft_network.all_replicas(without={0})),
                                        expected=lambda v: v == expected_next_primary,
                                        err_msg="Make sure view change has been triggered.")

            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_dropping_packets(self, bft_network, tracker):
        '''
        Drop 5% of the packets in the network and make sure the system is able to make progress
        '''
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        with net.PacketDroppingAdversary(bft_network, drop_rate_percentage=5) as adversary:
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(bft_network, tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_f_isolated_non_primaries(self, bft_network, tracker):
        '''
        Isolate f non primaries replicas and make sure the system is able to make progress
        '''
        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        with net.ReplicaSubsetIsolatingAdversary(bft_network, bft_network.random_set_of_replicas(f, without={0}))\
                as adversary:
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_f_isolated_with_primary(self, bft_network, tracker):
        '''
        Isolate f replicas including the primary and make sure the system is able to make progress
        '''
        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_primary = 0
        expected_next_primary = 1
        isolated_replicas = bft_network.random_set_of_replicas(f - 1, without={initial_primary, expected_next_primary})
        isolated_replicas.add(initial_primary)

        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            await bft_network.wait_for_view(replica_id=expected_next_primary,
                                            expected=lambda v: v == expected_next_primary,
                                            err_msg="Make sure view change has been triggered.")

            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_alternate_f_isolated(self, bft_network, tracker):
        '''
        Isolate f replicas and make sure the system is able to make progress.
        Then, isolate a different set of f replicas and make sure the system is able to make progress
        '''
        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_primary = 0
        isolated_replicas_take_1 = bft_network.random_set_of_replicas(f, without={initial_primary})

        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        read_client = bft_network.random_client()

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas_take_1) as adversary:
            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

        isolated_replicas_take_1.add(initial_primary)
        isolated_replicas_take_2 = bft_network.random_set_of_replicas(f, without=isolated_replicas_take_1)

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas_take_2) as adversary:

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(bft_network,tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=False)
    async def test_conflicting_requests(self, bft_network, tracker):
        """
        Launch pre-process conflicting request and make sure that conflicting requests are not committed
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        read_client = bft_network.random_client()
        start_block = await tracker.get_last_block_id(read_client)

        ops = 50

        try:
            with trio.move_on_after(seconds=30):
                skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
                await skvbc.run_concurrent_conflict_ops(ops, write_weight=1)
        except trio.TooSlowError:
            pass

        last_block = await tracker.get_last_block_id(read_client)

        # We produced at least one conflict.
        assert last_block < start_block + ops

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=False)
    async def test_conflicting_requests_with_f_failures(self, bft_network, tracker):
        """
        Launch pre-process conflicting request and make sure that conflicting requests are not committed
        """
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_primary = 0
        crashed_replicas = bft_network.random_set_of_replicas(f, without={initial_primary})
        bft_network.stop_replicas(replicas=crashed_replicas)

        read_client = bft_network.random_client()
        start_block = await tracker.get_last_block_id(read_client)

        ops = 50

        try:
            with trio.move_on_after(seconds=30):
                skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
                await skvbc.run_concurrent_conflict_ops(ops, write_weight=1)
        except trio.TooSlowError:
            pass

        last_block = await tracker.get_last_block_id(read_client)

        # We produced at least one conflict.
        assert last_block < start_block + ops

    async def issue_tracked_ops_to_the_system(self, bft_network, tracker):
        try:
            with trio.move_on_after(seconds=30):
                skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
                await skvbc.run_concurrent_ops(50, write_weight=.70)
        except trio.TooSlowError:
            pass
