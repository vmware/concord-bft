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

import struct
import copy
import random
import trio
import time

from collections import namedtuple
from util.skvbc_exceptions import BadReplyError
from util import eliot_logging as log
from util import bft

WriteReply = namedtuple('WriteReply', ['success', 'last_block_id'])

CLIENT_REQ_GRACEFUL_END_TIMER=3

class SimpleKVBCProtocol:
    KV_LEN = 21  ## SimpleKVBC requies fixed size keys and values right now
    READ_LATEST = 0xFFFFFFFFFFFFFFFF

    READ = 1
    WRITE = 2
    GET_LAST_BLOCK = 3
    GET_BLOCK_DATA = 4
    LONG_EXEC_WRITE = 5

    """
    An implementation of the wire protocol for SimpleKVBC requests.
    SimpleKVBC requests are application data embedded inside sbft client
    requests.
    """

    def __init__(self, bft_network, tracker = None, pre_process=False):
        self.bft_network = bft_network
        self.tracker = tracker
        self.pre_exec_all = pre_process
        self.alpha = [i for i in range(65, 91)]
        self.alphanum = [i for i in range(48, 58)]
        self.alphanum.extend(self.alpha)
        self.keys = self._create_keys()

    @classmethod
    def write_req(cls, readset, writeset, block_id, long_exec=False):
        data = bytearray()
        # A conditional write request type
        if long_exec is True:
            data.append(cls.LONG_EXEC_WRITE)
        else:
            data.append(cls.WRITE)
        # SimpleConditionalWriteHeader
        data.extend(
            struct.pack("<QQQ", block_id, len(readset), len(writeset)))
        # SimpleKey[numberOfKeysInReadSet]
        for r in readset:
            data.extend(r)
        # SimpleKV[numberOfWrites]
        for kv in writeset:
            data.extend(kv[0])
            data.extend(kv[1])

        return data

    @classmethod
    def read_req(cls, readset, block_id=READ_LATEST):
        data = bytearray()
        data.append(cls.READ)
        # SimpleReadHeader
        data.extend(struct.pack("<QQ", block_id, len(readset)))
        # SimpleKey[numberOfKeysToRead]
        for r in readset:
            data.extend(r)
        return data

    @classmethod
    def get_last_block_req(cls):
        data = bytearray()
        data.append(cls.GET_LAST_BLOCK)
        return data

    @classmethod
    def get_block_data_req(cls, block_id):
        data = bytearray()
        data.append(cls.GET_BLOCK_DATA)
        data.extend(struct.pack("<Q", block_id))
        return data

    @classmethod
    def parse_reply(cls, data):
        reply_type = data[0]
        if reply_type == cls.WRITE:
            return cls.parse_write_reply(data[1:])
        elif reply_type == cls.READ:
            return cls.parse_read_reply(data[1:])
        elif reply_type == cls.GET_LAST_BLOCK:
            return cls.parse_get_last_block_reply(data[1:])
        else:
            raise BadReplyError

    @staticmethod
    def parse_write_reply(data):
        return WriteReply._make(struct.unpack("<?Q", data))

    @classmethod
    def parse_read_reply(cls, data):
        num_kv_pairs = struct.unpack("<Q", data[0:8])[0]
        data = data[8:]
        kv_pairs = {}
        for i in range(num_kv_pairs):
            kv_pairs[data[0:cls.KV_LEN]] = data[cls.KV_LEN:2 * cls.KV_LEN]
            if i + 1 != num_kv_pairs:
                data = data[2 * cls.KV_LEN:]
        return kv_pairs

    @staticmethod
    def parse_get_last_block_reply(data):
        return struct.unpack("<Q", data)[0]

    @staticmethod
    def parse_have_you_stopped_reply(data):
        with log.start_action(action_type="parse_have_you_stopped_reply"):
            return struct.unpack("<q", data)[0]

    def initial_state(self):
        """Return a dict with KV_LEN zero byte values for all keys"""
        with log.start_action(action_type="initial_state"):
            all_zeros = b''.join([b'\x00' for _ in range(0, self.KV_LEN)])
            return dict([(k, all_zeros) for k in self.keys])

    def random_value(self):
        return bytes(random.sample(self.alphanum, self.KV_LEN))

    def random_values(self, n):
        return [self.random_value() for _ in range(0, n)]

    def random_key(self):
        return random.choice(self.keys)

    def random_keys(self, max_keys):
        """Return a set of keys that is of size <= max_keys"""
        return set(random.choices(self.keys, k=max_keys))

    def unique_random_key(self):
        """
        Generate an uniquely random key in contrast to random_key() that selects
        from a list of pre-generated keys. Use a prefix of '1' so that every key
        is different than keys pre-generated by _create_keys().
        """
        unique_random = bytes(random.sample(self.alphanum, self.KV_LEN - 1))
        return b'1' + unique_random

    @classmethod
    def max_key(cls):
        """
        Return the maximum possible key according to the schema in _create_keys.
        """
        return b''.join([b'Z' for _ in range(0, cls.KV_LEN)])

    async def send_indefinite_write_requests(self, client=None, delay=.1):
        with log.start_action(action_type="send_indefinite_write_requests"):
            msg = self.write_req(
                [], [(self.random_key(), self.random_value())], 0)
            while True:
                if (not client):
                    client = self.bft_network.random_client()
                try:
                    await client.write(msg)
                except:
                    pass
                await trio.sleep(delay)

    async def assert_kv_write_executed(self, key, val):
        with log.start_action(action_type="assert_kv_write_executed"):
            config = self.bft_network.config

            client = self.bft_network.random_client()
            reply = await client.read(
                self.read_req([key])
            )
            kv_reply = self.parse_reply(reply)
            assert {key: val} == kv_reply, \
                f'Could not read original key-value in the case of n={config.n}, f={config.f}, c={config.c}.'

    async def wait_for_liveness(self):
        with trio.fail_after(seconds=30):
            while True:
                with trio.move_on_after(seconds=2 * bft.REQ_TIMEOUT_MILLI/1000):
                    try:
                        key, value = await self.send_write_kv_set()
                        await self.assert_kv_write_executed(key, value)
                    except (trio.TooSlowError, AssertionError) as e:
                        pass
                    else:
                        # success
                        return
                    await trio.sleep(0.1)

    async def prime_for_state_transfer(
            self, stale_nodes,
            checkpoints_num=2,
            persistency_enabled=True):
        with log.start_action(action_type="prime_for_state_transfer"):
            initial_nodes = self.bft_network.all_replicas(without=stale_nodes)
            self.bft_network.start_all_replicas()
            self.bft_network.stop_replicas(stale_nodes)
            client = self.bft_network.random_client()
            # Write a KV pair with a known value
            known_key = self.unique_random_key()
            known_val = self.random_value()
            known_kv = [(known_key, known_val)]
            reply = await self.send_write_kv_set(client, known_kv)
            assert reply.success
            # Fill up the initial nodes with data, checkpoint them and stop
            # them. Then bring them back up and ensure the checkpoint data is
            # there.
            await self.fill_and_wait_for_checkpoint(
                initial_nodes,
                num_of_checkpoints_to_add=checkpoints_num,
                verify_checkpoint_persistency=persistency_enabled)

            return client, known_key, known_val

    async def write_with_multiple_clients_for_state_transfer(
            self, stale_nodes,
            write_run_duration=10,
            persistency_enabled=True):
        with log.start_action(action_type="write_with_multiple_clients_for_state_transfer"):
            initial_nodes = self.bft_network.all_replicas(without=stale_nodes)
            self.bft_network.start_all_replicas()
            self.bft_network.stop_replicas(stale_nodes)
            client = self.bft_network.random_client()
            # Write a KV pair with a known value
            known_key = self.unique_random_key()
            known_val = self.random_value()
            known_kv = [(known_key, known_val)]
            reply = await self.send_write_kv_set(client, known_kv)
            assert reply.success
            # Fill up the initial nodes with data, checkpoint them and stop
            # them. Then bring them back up and ensure the checkpoint data is
            # there.
            await self.run_concurrent_txns_and_wait_for_checkpoint(
                initial_nodes,
                write_run_duration,
                verify_checkpoint_persistency=persistency_enabled)

            return client, known_key, known_val

    async def fill_and_wait_for_checkpoint(
            self, initial_nodes,
            num_of_checkpoints_to_add=2,
            verify_checkpoint_persistency=True,
            assert_state_transfer_not_started=True,
            without_clients=None):
        """
        A helper function used by tests to fill a window with data and then
        checkpoint it.

        The nodes are then stopped and restarted to ensure the checkpoint data
        was persisted.

        TODO: Make filling concurrent to speed up tests
        """
        with log.start_action(action_type="fill_and_wait_for_checkpoint"):
            client = self.bft_network.random_client(without_clients)
            checkpoint_before = await self.bft_network.wait_for_checkpoint(
                replica_id=random.choice(initial_nodes))
            # Write enough data to checkpoint and create a need for state transfer
            for i in range(1 + num_of_checkpoints_to_add * 150):
                key = self.random_key()
                val = self.random_value()
                reply = await self.send_write_kv_set(client, [(key, val)])
                assert reply.success
            
            await self.bft_network.wait_for_replicas_to_collect_stable_checkpoint(
                initial_nodes, checkpoint_before + num_of_checkpoints_to_add)

            await self.network_wait_for_checkpoint(
                initial_nodes,
                expected_checkpoint_num=lambda ecn: ecn == checkpoint_before + num_of_checkpoints_to_add,
                verify_checkpoint_persistency=verify_checkpoint_persistency,
                assert_state_transfer_not_started=assert_state_transfer_not_started)
    
    async def run_concurrent_txns_and_wait_for_checkpoint(
            self, initial_nodes,
            write_run_duration=10,
            verify_checkpoint_persistency=True,
            assert_state_transfer_not_started=True,
            without_clients=None):
        """
        A helper function used by tests to run txns for a duration and then
        checkpoint it.

        If persistency flag is set the nodes are then stopped and restarted 
        to ensure the checkpoint data was persisted.

        filling concurrent to speed up tests
        """
        with log.start_action(action_type="run_concurrent_txns_and_wait_for_checkpoint"):
            clients = self.bft_network.get_all_clients()
            async def write_random_kv(client):
                key = self.random_key()
                val = self.random_value()
                reply = await self.send_write_kv_set(client,[(key, val)])
                assert reply.success
            t_end = time.time() + write_run_duration

            with trio.move_on_after(write_run_duration+CLIENT_REQ_GRACEFUL_END_TIMER):
                while time.time() < t_end:
                    async with trio.open_nursery() as nursery:
                        for client in clients:
                            nursery.start_soon(write_random_kv, client)

            await self.bft_network.wait_for_replicas_to_checkpoint(initial_nodes)               
            await self.network_wait_for_checkpoint(
                initial_nodes,
                expected_checkpoint_num=None,
                verify_checkpoint_persistency=verify_checkpoint_persistency,
                assert_state_transfer_not_started=assert_state_transfer_not_started)

    async def network_wait_for_checkpoint(
            self, initial_nodes,
            expected_checkpoint_num=lambda ecn: ecn == 2,
            verify_checkpoint_persistency=True,
            assert_state_transfer_not_started=True):
        with log.start_action(action_type="network_wait_for_checkpoint"):
            if assert_state_transfer_not_started:
                await self.bft_network.assert_state_transfer_not_started_all_up_nodes(
                    up_replica_ids=initial_nodes)

            # Wait for initial replicas to take checkpoints (exhausting
            # the full window)
            await self.bft_network.wait_for_replicas_to_checkpoint(initial_nodes, expected_checkpoint_num)
            if expected_checkpoint_num == None:
                expected_checkpoint_num= await self.bft_network.wait_for_checkpoint(
                replica_id=random.choice(initial_nodes))

            if verify_checkpoint_persistency:
                # Stop the initial replicas to ensure the checkpoints get persisted
                self.bft_network.stop_replicas(initial_nodes)

                # Bring up the first 3 replicas and ensure that they have the
                # checkpoint data.
                [ self.bft_network.start_replica(i) for i in initial_nodes ]
                await self.bft_network.wait_for_replicas_to_checkpoint(initial_nodes, expected_checkpoint_num)

    async def assert_successful_put_get(self):
        """ Assert that we can get a valid put """
        with log.start_action(action_type="assert_successful_put_get"):
            client = self.bft_network.random_client()
            read_reply = await client.read(self.get_last_block_req())
            last_block = self.parse_reply(read_reply)

            # Perform an unconditional KV put.
            # Ensure that the block number increments.
            key = self.random_key()
            val = self.random_value()

            reply = await client.write(self.write_req([], [(key, val)], 0))
            reply = self.parse_reply(reply)
            assert reply.success
            assert last_block + 1 == reply.last_block_id

            # Retrieve the last block and ensure that it matches what's expected
            read_reply = await client.read(self.get_last_block_req())
            newest_block = self.parse_reply(read_reply)
            assert last_block + 1 == newest_block

            # Get the previous put value, and ensure it's correct
            read_req = self.read_req([key], newest_block)
            kvpairs = self.parse_reply(await client.read(read_req))
            assert {key: val} == kvpairs

    def _create_keys(self):
        """
        Create a sequence of KV store keys with length = 2*num_clients.
        The last character in each key becomes the previous value + 1. When the
        value reaches 'Z', a new character is appended and the sequence starts
        over again.

        Since all keys must be KV_LEN bytes long, they are extended with '.'
        characters.
        """
        with log.start_action(action_type="_create_keys"):
            num_clients = self.bft_network.config.num_clients
            if num_clients == 0:
                return []
            cur = bytearray("A", 'utf-8')
            keys = [b"A...................."]
            for i in range(1, 2 * num_clients):
                end = cur[-1]
                if chr(end) == 'Z':  # extend the key
                    cur.append(self.alpha[0])
                else:
                    cur[-1] = end + 1
                key = copy.deepcopy(cur)
                # Extend the key to be KV_LEN bytes
                key.extend([ord('.') for _ in range(self.KV_LEN - len(cur))])
                keys.append(bytes(key))

            return keys

    async def read_your_writes(self):
        with log.start_action(action_type="read_your_writes") as action:
            action.log(message_type="[READ-YOUR-WRITES] Starting 'read-your-writes' check...")
            client = self.bft_network.random_client()
            # Verify by "Read your write"
            # Perform write with the new primary
            last_block = self.parse_reply(
                await client.read(self.get_last_block_req()))
            action.log(message_type=f'[READ-YOUR-WRITES] Last block ID: #{last_block}')
            kv = [(self.keys[0], self.random_value()),
                  (self.keys[1], self.random_value())]

            reply = await self.send_write_kv_set(kv=kv)
            assert last_block + 1 == reply.last_block_id

            last_block = reply.last_block_id

            # Read the last write and check if equal
            # Get the kvpairs in the last written block
            action.log(message_type=f'[READ-YOUR-WRITES] Checking if the {kv} entry is readable...')
            data = await client.read(self.get_block_data_req(last_block))
            kv2 = self.parse_reply(data)
            assert kv2 == dict(kv)
            action.log(message_type=f'[READ-YOUR-WRITES] OK.')

    async def send_write_kv_set(self, client=None, kv=None, max_set_size=None, long_exec=False, assert_reply=True, raise_slowErrorIfAny=True):
        with log.start_action(action_type="send_write_kv_set") as action:
            readset = set()
            read_version = 0
            kv_input = True
            if client is None:
                client = self.bft_network.random_client()
            if kv is None and max_set_size is None:
                kv_input = False
                max_set_size = 0
                key = self.random_key()
                val = self.random_value()
                writeset = [(key, val)]
            elif kv is not None and max_set_size is None:
                max_set_size = 0
                kv_input = True
                writeset = kv
            elif kv is None and max_set_size is not None:
                writeset = self.writeset(max_set_size)
            if self.tracker is not None:
                max_read_set_size = 0 if self.tracker.no_conflicts else max_set_size
                read_version = self.tracker.read_block_id()
                readset = self.readset(0, max_read_set_size)
            reply = await self.send_kv_set(client, readset, writeset, read_version, long_exec, assert_reply, raise_slowErrorIfAny)
            action.log(message_type="[send_write_kv_set] OK")
            if kv_input is True:
                return reply
            else:
                return writeset[0][0],writeset[0][1]

    async def send_kv_set(self, client, readset, writeset, read_version, long_exec=False, reply_assert=True, raise_slowErrorIfAny=True):
        with log.start_action(action_type="send_kv_set"):
            msg = self.write_req(readset, writeset, read_version, long_exec)
            seq_num = client.req_seq_num.next()
            client_id = client.client_id
            if self.tracker is not None:
                self.tracker.send_write(client_id, seq_num, readset, dict(writeset), read_version)
            try:
                serialized_reply = await client.write(msg, seq_num, pre_process=self.pre_exec_all)
                reply = self.parse_reply(serialized_reply)
                if reply_assert is True:
                    assert reply.success
                if self.tracker is not None:
                    self.tracker.status.record_client_reply(client_id)
                    self.tracker.handle_write_reply(client_id, seq_num, reply)
                return reply
            except trio.TooSlowError:
                if self.tracker is not None:
                    self.tracker.status.record_client_timeout(client_id)
                if raise_slowErrorIfAny is True:
                    raise trio.TooSlowError
                else:
                    return

    async def send_read_kv_set(self, client, key, max_set_size=0):
        with log.start_action(action_type="send_read_kv_set"):
            readData = []
            if key is None:
                readData = self.readset(1, max_set_size)
            else:
                readData = [key]
            msg = self.read_req(readData)
            if client is None:
                client = self.bft_network.random_client()
            seq_num = client.req_seq_num.next()
            client_id = client.client_id
            if self.tracker is not None:
                self.tracker.send_read(client_id, seq_num, readData)
            try:
                serialized_reply = await client.read(msg, seq_num)
                reply = self.parse_reply(serialized_reply)
                if self.tracker is not None:
                    self.tracker.status.record_client_reply(client_id)
                    self.tracker.handle_read_reply(client_id, seq_num, reply)
                return reply
            except trio.TooSlowError:
                if self.tracker is not None:
                    self.tracker.status.record_client_timeout(client_id)
                return

    async def run_concurrent_batch_ops(self, num_ops, batch_size):
        with log.start_action(action_type="run_concurrent_batch_ops"):
            max_concurrency = len(self.bft_network.clients) // 2
            max_size = len(self.keys) // 2
            sent = 0
            write_count = 0
            clients = self.bft_network.random_clients(max_concurrency)
            with log.start_action(action_type="send_concurrent_ops"):
                while sent < num_ops:
                    async with trio.open_nursery() as nursery:
                        for client in clients:
                            client.config = client.config._replace(
                                retry_timeout_milli=500
                            )
                            nursery.start_soon(self.send_write_kv_set_batch, client, max_size, batch_size)
                            write_count += 1
                    sent += len(clients)
            return write_count
    
    async def run_concurrent_ops(self, num_ops, write_weight=.70):
        with log.start_action(action_type="run_concurrent_ops"):
            max_concurrency = len(self.bft_network.clients) // 2
            max_size = len(self.keys) // 2
            return await self.send_concurrent_ops(num_ops, max_concurrency, max_size, write_weight, create_conflicts=True)

    async def run_concurrent_conflict_ops(self, num_ops, write_weight=.70):
        if self.tracker is not None:
            if self.tracker.no_conflicts is True:
                log.log_message(message_type="call to run_concurrent_conflict_ops with no_conflicts=True,"
                                            " calling run_concurrent_ops instead")
                return await self.run_concurrent_ops(num_ops, write_weight)
        max_concurrency = len(self.bft_network.clients) // 2
        max_size = len(self.keys) // 2
        return await self.send_concurrent_ops(num_ops, max_concurrency, max_size, write_weight, create_conflicts=True)

    async def send_concurrent_ops(self, num_ops, max_concurrency, max_size, write_weight, create_conflicts=False):
        max_read_set_size = max_size
        if self.tracker is not None:
            max_read_set_size = 0 if self.tracker.no_conflicts else max_size
        sent = 0
        write_count = 0
        read_count = 0
        clients = self.bft_network.random_clients(max_concurrency)
        with log.start_action(action_type="send_concurrent_ops"):
            while sent < num_ops:
                readset = self.readset(0, max_read_set_size)
                writeset = self.writeset(0, readset)
                read_version = 0
                if self.tracker is not None:
                    read_version = self.tracker.read_block_id()
                async with trio.open_nursery() as nursery:
                    for client in clients:
                        if random.random() < write_weight:
                            if create_conflicts is False:
                                readset = self.readset(0, max_read_set_size)
                                writeset = self.writeset(max_size)
                                read_version = 0
                                if self.tracker is not None:
                                    read_version = self.tracker.read_block_id()
                            nursery.start_soon(self.send_kv_set, client, readset, writeset, read_version, False, False, False)
                            write_count += 1
                        else:
                            nursery.start_soon(self.send_read_kv_set, client, None, max_size)
                            read_count += 1
                sent += len(clients)
        return read_count, write_count

    async def send_indefinite_batch_writes(self, batch_size, time_interval=.01):
        max_size = len(self.keys) // 2
        while True:
            client = self.bft_network.random_client()
            async with trio.open_nursery() as nursery:
                try:
                    nursery.start_soon(self.send_write_kv_set_batch, client, max_size, batch_size)
                except:
                    pass
                await trio.sleep(time_interval)

    async def send_indefinite_ops(self, write_weight=.70, time_interval=.01):
        max_size = len(self.keys) // 2
        while True:
            client = self.bft_network.random_client()
            async with trio.open_nursery() as nursery:
                try:
                    if random.random() < write_weight:
                        nursery.start_soon(self.send_write_kv_set, client, None, max_size, False, False, False)
                    else:
                        nursery.start_soon(self.send_read_kv_set, client, None, max_size)
                except:
                    pass
                await trio.sleep(time_interval)

    async def send_write_kv_set_batch(self, client, max_set_size, batch_size, read_version = None, long_exec = False):
        msg_batch = []
        batch_seq_nums = []
        client_id = client.client_id
        if read_version is None:
            read_version=0
            if self.tracker is not None:
                read_version = self.tracker.read_block_id()
        for i in range(batch_size):
            max_read_set_size = max_set_size
            if self.tracker is not None:
                max_read_set_size = 0 if self.tracker.no_conflicts else max_set_size
            readset = self.readset(0, max_read_set_size)
            writeset = self.writeset(max_set_size)
            msg_batch.append(self.write_req(readset, writeset, read_version, long_exec))
            seq_num = client.req_seq_num.next()
            batch_seq_nums.append(seq_num)
            if self.tracker is not None:
                self.tracker.send_write(client_id, seq_num, readset, dict(writeset), read_version)
        
        with log.start_action(action_type="send_tracked_kv_set_batch"):
            try:
                replies = await client.write_batch(msg_batch, batch_seq_nums)
                if self.tracker is not None:
                    self.tracker.status.record_client_reply(client_id)
                for seq_num, reply_msg in replies.items():
                    reply = self.parse_reply(reply_msg.get_common_data())
                    if self.tracker is not None:
                        self.tracker.handle_write_reply(client_id, seq_num, reply)
            except trio.TooSlowError:
                if self.tracker is not None:
                    self.tracker.status.record_client_timeout(client_id)
                return
                
    def readset(self, min_size, max_size):
        return self.random_keys(random.randint(min_size, max_size))

    def writeset(self, max_size, keys=None):
        writeset_keys = self.random_keys(random.randint(0, max_size)) if keys is None else keys
        writeset_values = self.random_values(len(writeset_keys))
        return list(zip(writeset_keys, writeset_values))

class SkvbcClient:
    """A wrapper around bft_client that uses the SimpleKVBCProtocol"""

    def __init__(self, bft_client):
        self.client = bft_client

    async def write(self, readset, writeset, block_id=0):
        """Create an skvbc write message and send it via the bft client."""
        req = SimpleKVBCProtocol.write_req(readset, writeset, block_id)
        return SimpleKVBCProtocol.parse_reply(await self.client.write(req))

    async def read(self, readset, block_id=SimpleKVBCProtocol.READ_LATEST):
        """Create an skvbc read message and send it via the bft client."""
        req = SimpleKVBCProtocol.read_req(readset, block_id)
        return SimpleKVBCProtocol.parse_reply(await self.client.read(req))
