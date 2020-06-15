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

SKVBC_INIT_GRACE_TIME = 2
NUM_OF_SEQ_WRITES = 100
MAX_CONCURRENCY = 10
SHORT_REQ_TIMEOUT_MILLI = 3000
LONG_REQ_TIMEOUT_MILLI = 15000

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).

    Note each arguments is an element in a list.
    """

    status_timer_milli = "500"
    view_change_timeout_milli = "10000"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            "-v", view_change_timeout_milli,
            "-p",
            "-t", os.environ.get('STORAGE_TYPE')
            ]


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
    async def test_sequential_pre_process_requests(self, bft_network):
        """
        Use a random client to launch one pre-process request in time and ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await self.send_single_write_with_pre_execution(skvbc, client)

    @unittest.skip("unstable")
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_concurrent_pre_process_requests(self, bft_network):
        """
        Launch concurrent requests from different clients in parallel. Ensure that created blocks are as expected.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = len(clients)
        sent_count = await self.run_concurrent_pre_execution_requests(skvbc, clients, num_of_requests)
        self.assertTrue(sent_count >= num_of_requests)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_long_time_executed_pre_process_request(self, bft_network):
        """
        Launch pre-process request with a long-time execution and ensure that created blocks are as expected
        and no view-change was triggered.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        client = bft_network.random_client()
        client.config = client.config._replace(req_timeout_milli=LONG_REQ_TIMEOUT_MILLI, retry_timeout_milli=1000)
        await self.send_single_write_with_pre_execution(skvbc, client, long_exec=True)

        clients = bft_network.clients.values()
        with trio.move_on_after(seconds=1):
            await self.send_indefinite_pre_execution_requests(skvbc, clients)

        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure the view did not change.")

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_view_change(self, bft_network):
        """
        Crash the primary replica and verify that the system triggers a view change and moves to a new view.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        await trio.sleep(5)

        clients = bft_network.clients.values()
        client = random.choice(list(clients))
        key_before_vc = skvbc.random_key()
        value_before_vc = skvbc.random_value()
        write_set = [(key_before_vc, value_before_vc)]

        await self.send_single_write_with_pre_execution_and_kv(skvbc, write_set, client)
        await skvbc.assert_kv_write_executed(key_before_vc, value_before_vc)

        initial_primary = 0
        await bft_network.wait_for_view(replica_id=initial_primary,
                                        expected=lambda v: v == initial_primary,
                                        err_msg="Make sure we are in the initial view before crashing the primary.")
        last_block = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))
        bft_network.stop_replica(initial_primary)

        try:
            with trio.move_on_after(seconds=1):
                await self.send_indefinite_pre_execution_requests(skvbc, clients)

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
                new_last_block = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))
            except trio.TooSlowError:
                continue
            else:
                break

        self.assertEqual(new_last_block, last_block)

    @unittest.skip("Unstable due to BC-3145 TooSlow from pyclient.write")
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_parallel_tx_after_f_nonprimary_crash(self, bft_network):
        '''
        Crash f nonprimary replicas and submit X parallel write submissions.
        Block processing of the network should be unaffected with f-count interruption.
        Final block length should match submitted transactions count exactly.
        '''
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        
        read_client = bft_network.random_client()
        submit_clients = bft_network.random_clients(MAX_CONCURRENCY)
        num_of_requests = 10 * len(submit_clients) # each client will send 10 tx
        nonprimaries = bft_network.all_replicas(without={0}) # primary index is 0
        crash_targets = random.sample(nonprimaries, bft_network.config.f) # pick random f to crash
        bft_network.stop_replicas(crash_targets) # crash chosen nonprimary replicas

        await self.run_concurrent_pre_execution_requests(skvbc, submit_clients, num_of_requests, write_weight=1.0)
        final_block_count = skvbc.parse_reply(await read_client.read(skvbc.get_last_block_req()))

        print("")
        print(f"Randomly picked replica indexes {crash_targets} (nonprimary) to be stopped.")
        print(f"Total of {num_of_requests} write pre-exec tx, "
              f"concurrently submitted through {len(submit_clients)} clients.")
        print(f"Finished at block {final_block_count} (expected {num_of_requests}).")
        self.assertTrue(final_block_count == num_of_requests)

