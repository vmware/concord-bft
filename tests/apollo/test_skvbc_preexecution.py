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

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util import skvbc as kvbc
from util.skvbc_history_tracker import verify_linearizability
import util.bft_network_partitioning as net

SKVBC_INIT_GRACE_TIME = 2
NUM_OF_SEQ_WRITES = 100
MAX_CONCURRENCY = 10
SHORT_REQ_TIMEOUT_MILLI = 3000
LONG_REQ_TIMEOUT_MILLI = 15000

def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
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
            "-v", view_change_timeout_milli,
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else "",
            "-t", os.environ.get('STORAGE_TYPE')]

def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper

class SkvbcPreExecutionTest(unittest.TestCase):

    async def send_single_read(self, skvbc, client):
        req = skvbc.read_req(skvbc.random_keys(1))
        await client.read(req)

    async def send_indefinite_pre_execution_requests(self, skvbc, clients):
        while True:
            client = random.choice(list(clients))
            try:
                await self.send_single_write_with_pre_execution(skvbc, client)
            except:
                pass
            await trio.sleep(.1)

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
            sent += len(clients)
            await trio.sleep(.1)
        return read_count + write_count

    @unittest.skip("unstable")
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_sequential_pre_process_requests(self, bft_network, tracker):
        """
        Use a random client to launch one pre-process request in time and ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await tracker.send_tracked_write(client, 2)

    @unittest.skip("unstable")
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_concurrent_pre_process_requests(self, bft_network, tracker):
        """
        Launch concurrent requests from different clients in parallel. Ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()

        clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = len(clients)
        rw = await tracker.run_concurrent_ops(num_of_requests, write_weight=0.9)
        self.assertTrue(rw[0] + rw[1] >= num_of_requests)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_long_time_executed_pre_process_request(self, bft_network, tracker):
        """
        Launch pre-process request with a long-time execution and ensure that created blocks are as expected
        and no view-change was triggered.
        """
        bft_network.start_all_replicas()

        client = bft_network.random_client()
        client.config = client.config._replace(req_timeout_milli=LONG_REQ_TIMEOUT_MILLI, retry_timeout_milli=1000)
        await tracker.send_tracked_write(client, 2, long_exec=True)

        with trio.move_on_after(seconds=1):
            await tracker.send_indefinite_tracked_ops(write_weight=1)

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

        clients = bft_network.clients.values()
        client = random.choice(list(clients))

        await tracker.send_tracked_write(client, 2)

        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure we are in the initial view before crashing the primary.")

        last_block = await tracker.get_last_block_id(client)
        bft_network.stop_replica(initial_primary)

        try:
            with trio.move_on_after(seconds=1):
                await tracker.send_indefinite_tracked_ops(write_weight=1)

        except trio.TooSlowError:
            pass
        finally:
            expected_next_primary = 1
            await bft_network.wait_for_view(replica_id=random.choice(bft_network.all_replicas(without={0})),
                                            expected=lambda v: v == expected_next_primary,
                                            err_msg="Make sure view change has been triggered.")

        new_last_block = 0
        for retry in range(60):
            try:
                new_last_block = await tracker.get_last_block_id(client)
            except trio.TooSlowError:
                continue
            else:
                break

        self.assertEqual(new_last_block, last_block)

    @unittest.skip("Unstable due to BC-3145 TooSlow from pyclient.write")
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

        read_client = bft_network.random_client()
        submit_clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = 10 * len(submit_clients) # each client will send 10 tx
        nonprimaries = bft_network.all_replicas(without={0}) # primary index is 0
        crash_targets = random.sample(nonprimaries, bft_network.config.f) # pick random f to crash
        bft_network.stop_replicas(crash_targets) # crash chosen nonprimary replicas

        rw = await tracker.run_concurrent_ops(num_of_requests, write_weight=1)
        final_block_count = await tracker.get_last_block_id(read_client)

        print("")
        print(f"Randomly picked replica indexes {crash_targets} (nonprimary) to be stopped.")
        print(f"Total of {num_of_requests} write pre-exec tx, "
              f"concurrently submitted through {len(submit_clients)} clients.")
        print(f"Finished at block {final_block_count}.")
        self.assertTrue(rw[0] + rw[1] >= num_of_requests)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_primary_isolation_from_replicas(self, bft_network, tracker):
        '''
        Isolate the from the other replicas, wait for view change and ensure the system is still able to make progress
        '''
        with net.PrimaryIsolatingAdversary(bft_network) as adversary:
            bft_network.start_all_replicas()
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            await adversary.interfere()

            await self.issue_tracked_ops_to_the_system(tracker)

            expected_next_primary = 1
            await bft_network.wait_for_view(replica_id=random.choice(bft_network.all_replicas(without={0})),
                                        expected=lambda v: v == expected_next_primary,
                                        err_msg="Make sure view change has been triggered.")

            await self.issue_tracked_ops_to_the_system(tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_dropping_packets(self, bft_network, tracker):
        '''
        Drop 5% of the packets in the network and make sure the system is able to make progress
        '''
        with net.PacketDroppingAdversary(bft_network, drop_rate_percentage=5) as adversary:
            bft_network.start_all_replicas()
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

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

        with net.ReplicaSubsetIsolatingAdversary(bft_network, bft_network.random_set_of_replicas(f, without={0}))\
                as adversary:
            bft_network.start_all_replicas()
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

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

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            bft_network.start_all_replicas()
            read_client = bft_network.random_client()

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(tracker)

            await bft_network.wait_for_view(replica_id=expected_next_primary,
                                            expected=lambda v: v == expected_next_primary,
                                            err_msg="Make sure view change has been triggered.")

            await self.issue_tracked_ops_to_the_system(tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block


    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("20000"), selected_configs=lambda n, f, c: f >= 2)
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
        read_client = bft_network.random_client()

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas_take_1) as adversary:
            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block

        isolated_replicas_take_1.add(initial_primary)
        isolated_replicas_take_2 = bft_network.random_set_of_replicas(f, without=isolated_replicas_take_1)

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas_take_2) as adversary:

            start_block = await tracker.get_last_block_id(read_client)

            adversary.interfere()
            await self.issue_tracked_ops_to_the_system(tracker)

            last_block = await tracker.get_last_block_id(read_client)
            assert last_block > start_block


    async def issue_tracked_ops_to_the_system(self, tracker):
        try:
            with trio.move_on_after(seconds=30):
                await tracker.run_concurrent_ops(50, write_weight=1)
        except trio.TooSlowError:
            pass
