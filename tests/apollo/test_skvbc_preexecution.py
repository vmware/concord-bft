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

    async def send_single_write_with_pre_execution(self, skvbc, client):
        kv = [(skvbc.keys[0], skvbc.random_value()),
              (skvbc.keys[1], skvbc.random_value())]
        reply = await client.write(skvbc.write_req([], kv, 0), pre_process=True)
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)

    async def run_concurrent_pre_execution_requests(self, skvbc, clients, num_of_requests, write_weight=.90):
        sent = 0
        write_count = 0
        read_count = 0
        while sent <= num_of_requests:
            async with trio.open_nursery() as nursery:
                for client in clients:
                    if random.random() <= write_weight:
                        nursery.start_soon(self.send_single_write_with_pre_execution, skvbc, client)
                        write_count += 1
                    else:
                        nursery.start_soon(self.send_single_read, skvbc, client)
                        read_count += 1
            sent += len(clients)
        return read_count + write_count

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_sequential_pre_process_requests(self, bft_network):
        """
        Use a random client to launch one pre-process request in time and ensure that created block are as expected.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        for i in range(100):
            client = bft_network.random_client()
            await self.send_single_write_with_pre_execution(skvbc, client)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_concurrent_pre_process_requests(self, bft_network):
        """
        Launch concurrent requests from different clients in parallel. Ensure that created block are as expected.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        max_concurrency = len(bft_network.clients)
        clients = bft_network.random_clients(max_concurrency)
        num_of_requests = max_concurrency * len(clients)
        sent_count = await self.run_concurrent_pre_execution_requests(skvbc, clients, num_of_requests)
        self.assertTrue(sent_count >= num_of_requests)
