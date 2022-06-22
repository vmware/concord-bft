# Concord
#
# Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import copy
import random
import trio
import time

from collections import namedtuple

from util.consts import CHECKPOINT_SEQUENCES
from util.skvbc_exceptions import BadReplyError
from util import eliot_logging as log
from util import bft
from enum import Enum
from util import bft_network_partitioning as net

import os.path
import sys
sys.path.append(os.path.abspath("../../build/tests/apollo/util/"))
import skvbc_messages

WriteReply = namedtuple('WriteReply', ['success', 'last_block_id'])

class ExitPolicy(Enum):
    COUNT = 0
    TIME = 1

class SimpleKVBCProtocol:
    # Length to use for randomly generated key and value strings; this length is
    # effectively arbitrary as the SimpleKVBC protocol permits key and value
    # bytestrings of any length (usually up to some practical
    # implementation-imposed upper length limits).
    LENGTH_FOR_RANDOM_KVS = 21

    # Maximum value for a 64-bit unsigned integer.
    READ_LATEST = 0xFFFFFFFFFFFFFFFF

    """
    An implementation of the wire protocol for SimpleKVBC requests.
    SimpleKVBC requests are application data embedded inside sbft client
    requests.
    """

    def __init__(self, bft_network, tracker = None, pre_exec_all = None):
        """
        Initializer for SimpleKVBCProtocol, given a BftTestNetwork to connect to
        as bft_network, and optionally an SkvbcTracker to use as tracker and a
        Boolean value specifying whether to enable pre execution on all requests
        as pre_exec_all.

        Note pre_exec_all defaults to False if no tracker is given and to the
        tracker's value for pre_exec_all if a tracker is given, so passing an
        explicit value for pre_exec_all is unnecessary if a tracker is in use.
        """

        self.bft_network = bft_network
        self.tracker = tracker

        if (self.tracker is not None):
            if (pre_exec_all is not None):
                assert (self.tracker.pre_exec_all == pre_exec_all), \
                    "SimpleKVBCProtocol constructor was given a value for " \
                    "pre_exec_all conflicting with the value configured in " \
                    "the SkvbcTracker it was given."

            self.pre_exec_all = self.tracker.pre_exec_all
        else:
            if (pre_exec_all is None):
                self.pre_exec_all = False
            else:
                self.pre_exec_all = pre_exec_all

        self.alpha = [i for i in range(65, 91)]
        self.alphanum = [i for i in range(48, 58)]
        self.alphanum.extend(self.alpha)
        self.keys = self._create_keys()

    @classmethod
    def write_req(cls, readset, writeset, block_id, long_exec=False):
        write_request = skvbc_messages.SKVBCWriteRequest()
        write_request.read_version = block_id
        write_request.long_exec = long_exec
        write_request.readset = list(readset)
        write_request.writeset = writeset
        request = skvbc_messages.SKVBCRequest()
        request.request = write_request
        return request.serialize()

    @classmethod
    def read_req(cls, readset, block_id=READ_LATEST):
        read_request = skvbc_messages.SKVBCReadRequest()
        read_request.read_version = block_id
        read_request.keys = list(readset)
        request = skvbc_messages.SKVBCRequest()
        request.request = read_request
        return request.serialize()

    @classmethod
    def get_last_block_req(cls):
        request = skvbc_messages.SKVBCRequest()
        request.request = skvbc_messages.SKVBCGetLastBlockRequest()
        return request.serialize()

    @classmethod
    def get_block_data_req(cls, block_id):
        get_block_data_request = skvbc_messages.SKVBCGetBlockDataRequest()
        get_block_data_request.block_id = block_id
        request = skvbc_messages.SKVBCRequest()
        request.request = get_block_data_request
        return request.serialize()

    @classmethod
    def parse_reply(cls, data):
        (reply, offset) = skvbc_messages.SKVBCReply.deserialize(data)
        if isinstance(reply.reply, skvbc_messages.SKVBCWriteReply):
            return WriteReply(reply.reply.success, reply.reply.latest_block)
        elif isinstance(reply.reply, skvbc_messages.SKVBCReadReply):
            return dict(reply.reply.reads)
        elif isinstance(reply.reply, skvbc_messages.SKVBCGetLastBlockReply):
            return reply.reply.latest_block
        else:
            raise BadReplyError

    def initial_state(self):
        """
        Return a dict with empty byte strings (which is the value SKVBC will
        report for any key that has never been written to) for all keys
        """
        with log.start_action(action_type="initial_state"):
            empty_byte_string = b''
            return dict([(k, empty_byte_string) for k in self.keys])

    def random_value(self):
        return bytes(random.sample(self.alphanum, self.LENGTH_FOR_RANDOM_KVS))

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
        unique_random = bytes(random.sample(self.alphanum, self.LENGTH_FOR_RANDOM_KVS - 1))
        return b'1' + unique_random

    @classmethod
    def max_key(cls):
        """
        Return the maximum possible key according to the schema in _create_keys.
        """
        return b''.join([b'Z' for _ in range(0, cls.LENGTH_FOR_RANDOM_KVS)])

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
            initial_nodes, client, known_key, known_val = \
                await self.start_replicas_and_write_known_kv(stale_nodes)
            # Fill up the initial nodes with data, checkpoint them and stop
            # them. Then bring them back up and ensure the checkpoint data is
            # there.
            await self.fill_and_wait_for_checkpoint(
                initial_nodes,
                num_of_checkpoints_to_add=checkpoints_num,
                verify_checkpoint_persistency=persistency_enabled)

            return client, known_key, known_val

    async def start_replicas_and_write_with_multiple_clients(
            self, stale_nodes,
            write_run_duration=10,
            persistency_enabled=True):
        with log.start_action(action_type="write_with_multiple_clients_for_state_transfer"):
            initial_nodes, _, _, _ = \
                await self.start_replicas_and_write_known_kv(stale_nodes)
            max_concurrency = len(self.bft_network.clients)
            max_size = len(self.keys) // 2
            write_weight=.70
            # Fill up the initial nodes with data, checkpoint them and stop
            # them. Then bring them back up and ensure the checkpoint data is
            # there.
            await self.send_concurrent_ops(write_run_duration,max_concurrency,max_size,write_weight,
                False, ExitPolicy.TIME)
            await self.network_wait_for_checkpoint(
                initial_nodes,
                expected_checkpoint_num=None,
                verify_checkpoint_persistency=persistency_enabled,
                assert_state_transfer_not_started=True)

    async def start_replicas_and_write_known_kv(self, stale_nodes, rep_alredy_started=False):
            initial_nodes = self.bft_network.all_replicas(without=stale_nodes)
            if not rep_alredy_started:
                self.bft_network.start_all_replicas()
            self.bft_network.stop_replicas(stale_nodes)
            client = self.bft_network.random_client()
            # Write a KV pair with a known value
            known_key = self.unique_random_key()
            known_val = self.random_value()
            known_kv = [(known_key, known_val)]
            reply = await self.send_write_kv_set(client, known_kv)
            assert reply.success
            return initial_nodes, client, known_key, known_val

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
            for i in range(1 + num_of_checkpoints_to_add * CHECKPOINT_SEQUENCES):
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

            if expected_checkpoint_num is None:
                expected_checkpoint_num = await self.bft_network.wait_for_checkpoint(
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

        All keys are extended with '.' to LENGTH_FOR_RANDOM_KVS bytes with '.'
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
                # Extend the key to be LENGTH_FOR_RANDOM_KVS bytes
                key.extend([ord('.') for _ in range(self.LENGTH_FOR_RANDOM_KVS - len(cur))])
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

    async def send_write_kv_set(self, client=None, kv=None, max_set_size=None, long_exec=False, assert_reply=True,
                                raise_slowErrorIfAny=True, description=''):
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
        reply = await self.send_kv_set(client, readset, writeset, read_version, long_exec, assert_reply,
                                       raise_slowErrorIfAny, description=f'write {description}')
        if kv_input is True:
            return reply
        else:
            return writeset[0][0], writeset[0][1]

    async def send_kv_set(self, client, readset, writeset, read_version, long_exec=False, reply_assert=True,
                          raise_slowErrorIfAny=True, description='send_kv_set'):
        seq_num = client.req_seq_num.next()
        with log.start_action(action_type=description, seq=seq_num, client=client.client_id,
                              readset_size=len(readset), writeset_size=len(writeset)) as action:
            msg = self.write_req(readset, writeset, read_version, long_exec)
            client_id = client.client_id
            if self.tracker is not None:
                self.tracker.send_write(client_id, seq_num, readset, dict(writeset), read_version)
            try:
                serialized_reply = await client.write(msg, seq_num, pre_process=self.pre_exec_all)
                action.log(message_type=f"received reply", client=client.client_id, seq=client.req_seq_num.val())
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

    async def run_concurrent_ops(self, num_ops: int, write_weight: float = .70):
        return await self.send_concurrent_ops(exit_factor=num_ops,
                                              max_concurrency=len(self.bft_network.clients) // 2,
                                              max_size=len(self.keys) // 2,
                                              write_weight=write_weight, create_conflicts=True)

    async def run_concurrent_conflict_ops(self, num_ops, write_weight=.70):
        if self.tracker is not None:
            if self.tracker.no_conflicts is True:
                log.log_message(message_type="call to run_concurrent_conflict_ops with no_conflicts=True,"
                                            " calling run_concurrent_ops instead")
                return await self.run_concurrent_ops(num_ops, write_weight)
        max_concurrency = len(self.bft_network.clients) // 2
        max_size = len(self.keys) // 2
        return await self.send_concurrent_ops(num_ops, max_concurrency, max_size, write_weight, create_conflicts=True)

    async def send_concurrent_ops_with_isolated_replica(
            self,
            isolated_replica,
            run_duration):
        """
        Sending concurrent operation while isolated replic is kept blocked
        """
        clients = self.bft_network.get_all_clients()
        max_read_set_size = len(self.keys)
        read_version = 0
        readset = self.readset(0, max_read_set_size)
        writeset = self.writeset(0, readset)
        total_run_time = time.time()+run_duration
        initial_nodes = self.bft_network.all_replicas(without=isolated_replica)
        with log.start_action(action_type="send_concurrent_ops_with_isolated_replica"):
            with trio.move_on_after(run_duration+30):
                    with net.ReplicaSubsetIsolatingAdversary(self.bft_network, isolated_replica) as adversary:
                        adversary.interfere()
                        while(time.time() < total_run_time):
                                async with trio.open_nursery() as nursery:
                                    for client in clients:
                                        nursery.start_soon(self.send_kv_set, client, readset, writeset, read_version, False, False, False)
                        await self.bft_network.wait_for_replicas_to_checkpoint(initial_nodes)

    async def send_concurrent_ops(
            self,
            exit_factor,
            max_concurrency,
            max_size,
            write_weight,
            create_conflicts=False,
            exit_policy=ExitPolicy.COUNT):
        """
        exit_factor should be number of seconds to run if exit_policy is TIME
        and number of times to run if exit_policy is COUNT
        """
        max_read_set_size = max_size
        if self.tracker is not None:
            max_read_set_size = 0 if self.tracker.no_conflicts else max_size
        sent = 0
        write_count = 0
        read_count = 0
        total_run_duration = 0
        if exit_policy is ExitPolicy.TIME:
            total_run_duration = time.time() + exit_factor
        clients = self.bft_network.random_clients(max_concurrency)

        def exit_predicate():
            if  exit_policy is ExitPolicy.COUNT:
                return sent < exit_factor
            if  exit_policy is ExitPolicy.TIME:
                return time.time() < total_run_duration
            assert False, "Unknown exit policy"

        with log.start_action(action_type="send_concurrent_ops"):
            while exit_predicate():
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

    async def send_indefinite_ops(self, write_weight=.70, time_interval=.01, excluded_clients=None):
        max_size = len(self.keys) // 2
        while True:
            client = self.bft_network.random_client(without=excluded_clients)
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

    async def send_n_kvs_sequentially(self, kv_count: int, client: 'BftClient' = None):
        """
        Reaches a consensus over kv_count blocks, Waits for execution reply message after each block
        so that multiple client requests are not batched
        """
        client = client or self.bft_network.random_client()
        for i in range(kv_count):
            await self.send_write_kv_set(client=client, kv=[(b'A' * i, b'')], description=f'{i}')

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
