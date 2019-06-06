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

import bft_tester
import skvbc
import skvbc_linearizability

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
    path = os.path.join(builddir, "bftengine",
            "tests", "simpleKVBCTests", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli]

def interesting_configs():
    return [{'n': 4, 'f': 1, 'c': 0, 'num_clients': 4},
            {'n': 6, 'f': 1, 'c': 1, 'num_clients': 4},
            {'n': 11, 'f': 2, 'c': 2, 'num_clients': 6}
            ]

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
        for config in interesting_configs():
            self.config = config
            config = bft_tester.TestConfig(config['n'],
                                           config['f'],
                                           config['c'],
                                           config['num_clients'],
                                           key_file_prefix=KEY_FILE_PREFIX,
                                           start_replica_cmd=start_replica_cmd)

            with bft_tester.BftTester(config) as tester:
                init_state = tester.initial_state()
                self.tracker = skvbc_linearizability.SkvbcTracker(init_state)
                self.tester = tester
                await tester.init()
                tester.start_all_replicas()
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(self.run_concurrent_ops, num_ops)

                await self.verify()

    async def verify(self):
        last_block_id = await self.get_last_block_id()
        missing_block_ids = self.tracker.get_missing_blocks(last_block_id)
        blocks = await self.get_blocks(missing_block_ids)
        self.tracker.fill_missing_blocks(blocks)
        try:
            self.tracker.verify()
        except Exception as e:
            for i, entry in enumerate(self.tracker.history):
                print(f'Index = {i}: {entry}')
            print(e)
            raise(e)

    async def get_blocks(self, block_ids):
        client = self.tester.random_client()
        blocks = {}
        for block_id in block_ids:
            msg = self.protocol.get_block_data_req(block_id)
            blocks[block_id] = self.protocol.parse_reply(await client.read(msg))
        return blocks

    async def get_last_block_id(self):
        client = self.tester.random_client()
        msg = self.protocol.get_last_block_req()
        return self.protocol.parse_reply(await client.read(msg))

    async def run_concurrent_ops(self, num_ops):
        max_concurrency = len(self.tester.clients)//2
        write_weight = .70
        self.protocol = skvbc.SimpleKVBCProtocol()
        max_size = len(self.tester.keys)//2
        sent = 0
        while sent < num_ops:
            clients = self.tester.random_clients(max_concurrency)
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
        msg = self.protocol.write_req(readset, writeset, read_version)
        seq_num = client.req_seq_num.next()
        client_id = client.client_id
        self.tracker.send_write(
            client_id, seq_num, readset, dict(writeset), read_version)
        try:
            serialized_reply = await client.write(msg, seq_num)
            reply = self.protocol.parse_reply(serialized_reply)
            self.tracker.handle_write_reply(client_id, seq_num, reply)
        except trio.TooSlowError:
            return

    async def send_read(self, client, max_set_size):
        readset = self.readset(1, max_set_size)
        msg = self.protocol.read_req(readset)
        seq_num = client.req_seq_num.next()
        client_id = client.client_id
        self.tracker.send_read(client_id, seq_num, readset)
        try:
            serialized_reply = await client.read(msg, seq_num)
            reply = self.protocol.parse_reply(serialized_reply)
            self.tracker.handle_read_reply(client_id, seq_num, reply)
        except trio.TooSlowError:
            return

    def read_block_id(self):
        start = max(0, self.tracker.last_known_block - MAX_LOOKBACK)
        return random.randint(start, self.tracker.last_known_block)

    def readset(self, min_size, max_size):
        return self.tester.random_keys(random.randint(min_size, max_size))

    def writeset(self, max_size):
        writeset_keys = self.tester.random_keys(random.randint(0, max_size))
        writeset_values = self.tester.random_values(len(writeset_keys))
        return list(zip(writeset_keys, writeset_values))

if __name__ == '__main__':
    unittest.main()
