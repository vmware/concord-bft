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

import unittest
import trio
import os.path
import random
import time

from util import bft, skvbc_history_tracker
from util import skvbc as kvbc

KEY_FILE_PREFIX = "replica_keys_"

# The max number of blocks to check for read intersection during conditional
# writes
MAX_LOOKBACK=10

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "3000"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli
            ]

class Status:
    """
    Status about the running test.
    This is useful for debugging if the test fails.

    TODO: Should this live in the tracker?
    """
    def __init__(self, config):
        self.config = config
        self.start_time = time.monotonic()
        self.end_time = 0
        self.last_client_reply = 0
        self.client_timeouts = {}
        self.client_replies = {}

    def record_client_reply(self, client_id):
        self.last_client_reply = time.monotonic()
        count = self.client_replies.get(client_id, 0)
        self.client_replies[client_id] = count + 1

    def record_client_timeout(self, client_id):
        count = self.client_timeouts.get(client_id, 0)
        self.client_timeouts[client_id] = count + 1

    def __str__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  config={self.config}\n'
           f'  test_duration={self.end_time - self.start_time} seconds\n'
           f'  time_since_last_client_reply='
           f'{self.end_time - self.last_client_reply} seconds\n'
           f'  client_timeouts={self.client_timeouts}\n'
           f'  client_replies={self.client_replies}\n')

class SkvbcChaosTest(unittest.TestCase):
    def test_healthy(self):
        """
        Run a bunch of concurrrent requests in batches and verify
        linearizability. The system is healthy and stable and no faults are
        intentionally generated.
        """
        trio.run(self._test_healthy)

    async def _test_healthy(self):
        num_ops = 500
        for c in bft.interesting_configs():
            config = bft.TestConfig(c['n'],
                                    c['f'],
                                    c['c'],
                                    c['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)

            with bft.BftTestNetwork(config) as bft_network:
                self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
                init_state = self.skvbc.initial_state()
                self.tracker = skvbc_history_tracker.SkvbcTracker(init_state)
                self.bft_network = bft_network
                self.status = Status(c)
                await bft_network.init()
                bft_network.start_all_replicas()
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(self.run_concurrent_ops, num_ops)

                await self.verify()

    def test_wreak_havoc(self):
        """
        Run a bunch of concurrrent requests in batches and verify
        linearizability. In this test we generate faults periodically and verify
        linearizability at the end of the run.
        """
        trio.run(self._test_wreak_havoc)

    async def _test_wreak_havoc(self):
        num_ops = 500
        for c in bft.interesting_configs():
            print(f"\n\nStarting test with configuration={c}", flush=True)
            config = bft.TestConfig(c['n'],
                                    c['f'],
                                    c['c'],
                                    c['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)

            with bft.BftTestNetwork(config) as bft_network:
                self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
                init_state = self.skvbc.initial_state()
                self.tracker = skvbc_history_tracker.SkvbcTracker(init_state)
                self.bft_network = bft_network
                self.status = Status(c)
                await bft_network.init()
                bft_network.start_all_replicas()
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(self.run_concurrent_ops, num_ops)
                    nursery.start_soon(self.crash_primary)

                await self.verify()

            time.sleep(2)

    async def verify(self):
        try:
            # Use a new client, since other clients may not be responsive due to
            # past failed responses.
            client = await self.skvbc.bft_network.new_client()
            last_block_id = await self.get_last_block_id(client)
            print(f'Last Block ID = {last_block_id}')
            missing_block_ids = self.tracker.get_missing_blocks(last_block_id)
            print(f'Missing Block IDs = {missing_block_ids}')
            blocks = await self.get_blocks(client, missing_block_ids)
            self.tracker.fill_missing_blocks(blocks)
            self.tracker.verify()
        except Exception as e:
            print(f'retries = {client.retries}')
            self.status.end_time = time.monotonic()
            print("HISTORY...")
            for i, entry in enumerate(self.tracker.history):
                print(f'Index = {i}: {entry}\n')
            print("BLOCKS...")
            print(f'{self.tracker.blocks}\n')
            print(str(self.status), flush=True)
            print("FAILURE...")
            raise(e)

    async def get_blocks(self, client, block_ids):
        blocks = {}
        for block_id in block_ids:
            retries = 12 # 60 seconds
            for i in range(0, retries):
                try:
                    msg = kvbc.SimpleKVBCProtocol.get_block_data_req(block_id)
                    blocks[block_id] = kvbc.SimpleKVBCProtocol.parse_reply(await client.read(msg))
                    break
                except trio.TooSlowError:
                    if i == retries - 1:
                        raise
            print(f'Retrieved block {block_id}')
        return blocks

    async def get_last_block_id(self, client):
        msg = kvbc.SimpleKVBCProtocol.get_last_block_req()
        return kvbc.SimpleKVBCProtocol.parse_reply(await client.read(msg))

    async def crash_primary(self):
        await trio.sleep(.5)
        self.bft_network.stop_replica(0)

    async def run_concurrent_ops(self, num_ops):
        max_concurrency = len(self.bft_network.clients) // 2
        write_weight = .70
        max_size = len(self.skvbc.keys) // 2
        sent = 0
        while sent < num_ops:
            clients = self.bft_network.random_clients(max_concurrency)
            async with trio.open_nursery() as nursery:
                for client in clients:
                    if random.random() < write_weight:
                        nursery.start_soon(self.send_write, client, max_size)
                    else:
                        nursery.start_soon(self.send_read, client, max_size)
            sent += len(clients)

    async def send_write(self, client, max_set_size):
        readset = self.readset(0, max_set_size)
        writeset = self.writeset(max_set_size)
        read_version = self.read_block_id()
        msg = self.skvbc.write_req(readset, writeset, read_version)
        seq_num = client.req_seq_num.next()
        client_id = client.client_id
        self.tracker.send_write(
            client_id, seq_num, readset, dict(writeset), read_version)
        try:
            serialized_reply = await client.write(msg, seq_num)
            self.status.record_client_reply(client_id)
            reply = self.skvbc.parse_reply(serialized_reply)
            self.tracker.handle_write_reply(client_id, seq_num, reply)
        except trio.TooSlowError:
            self.status.record_client_timeout(client_id)
            return

    async def send_read(self, client, max_set_size):
        readset = self.readset(1, max_set_size)
        msg = self.skvbc.read_req(readset)
        seq_num = client.req_seq_num.next()
        client_id = client.client_id
        self.tracker.send_read(client_id, seq_num, readset)
        try:
            serialized_reply = await client.read(msg, seq_num)
            self.status.record_client_reply(client_id)
            reply = self.skvbc.parse_reply(serialized_reply)
            self.tracker.handle_read_reply(client_id, seq_num, reply)
        except trio.TooSlowError:
            self.status.record_client_timeout(client_id)
            return

    def read_block_id(self):
        start = max(0, self.tracker.last_known_block - MAX_LOOKBACK)
        return random.randint(start, self.tracker.last_known_block)

    def readset(self, min_size, max_size):
        return self.skvbc.random_keys(random.randint(min_size, max_size))

    def writeset(self, max_size):
        writeset_keys = self.skvbc.random_keys(random.randint(0, max_size))
        writeset_values = self.skvbc.random_values(len(writeset_keys))
        return list(zip(writeset_keys, writeset_values))

if __name__ == '__main__':
    unittest.main()
