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

# Add the pyclient directory to $PYTHONPATH

import sys

import os
import copy
import os.path
import random
import subprocess
from collections import namedtuple
import tempfile
import trio

sys.path.append(os.path.abspath("../util/pyclient"))

import bft_config
import bft_client
import bft_metrics_client
import bft_metrics

from bft_test_exceptions import AlreadyRunningError, AlreadyStoppedError

TestConfig = namedtuple('TestConfig', [
    'n',
    'f',
    'c',
    'num_clients',
    'key_file_prefix',
    'start_replica_cmd'
])

MAX_MSG_SIZE = 64*1024 # 64k
REQ_TIMEOUT_MILLI = 5000
RETRY_TIMEOUT_MILLI = 250
METRICS_TIMEOUT_SEC = 5

# TODO: This is not generic, but is required for use by SimpleKVBC. In the
# future we will likely want to change how we determine the lengths of keys and
# values, make them parameterizable, or generate keys in the protocols rather
# than tester. For now, all keys and values must be 21 bytes.
KV_LEN = 21

class BftTester:
    """A library to help testing sbft with a SimpleKVBC state machine"""

    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        for client in self.clients.values():
            client.__exit__()
        self.metrics.__exit__()
        self.stop_all_replicas()
        os.chdir(self.origdir)

    def __init__(self, config):
        self.origdir = os.getcwd()
        self.config = config
        self.testdir = tempfile.mkdtemp()
        print("Running test in {}".format(self.testdir))
        self.builddir = os.path.abspath("../build")
        self.toolsdir = os.path.join(self.builddir, "tools")
        self.procs = {}
        self.replicas = [bft_config.Replica(i, "127.0.0.1", 3710 + 2*i)
                for i in range(0, self.config.n)]
        self.alpha = [i for i in range(65, 91)]
        self.alphanum = [i for i in range(48, 58)]
        self.alphanum.extend(self.alpha)
        self.keys = self._create_keys()
        os.chdir(self.testdir)
        self._generate_crypto_keys()
        self.clients = {}
        self.metrics = None

    def _generate_crypto_keys(self):
        keygen = os.path.join(self.toolsdir, "GenerateConcordKeys")
        args = [keygen, "-n", str(self.config.n), "-f", str(self.config.f), "-o",
               self.config.key_file_prefix]
        subprocess.run(args, check=True)

    def _create_keys(self):
        """
        Create a sequence of KV store keys with length = 2*num_clients.
        The last character in each key becomes the previous value + 1. When the
        value reaches 'Z', a new character is appended and the sequence starts
        over again.

        Since all keys must be KV_LEN bytes long, they are extended with '.'
        characters.
        """
        if self.config.num_clients == 0:
            return []
        cur = bytearray("A", 'utf-8')
        keys = [b"A...................."]
        for i in range(1, 2*self.config.num_clients):
            end = cur[-1]
            if chr(end) == 'Z': # extend the key
                cur.append(self.alpha[0])
            else:
                cur[-1] = end + 1
            key = copy.deepcopy(cur)
            # Extend the key to be KV_LEN bytes
            key.extend([ord('.') for _ in range(KV_LEN - len(cur))])
            keys.append(bytes(key))
        return keys

    def max_key(self):
        """
        Return the maximum possible key according to the schema in _create_keys.

        """
        return b''.join([b'Z' for _ in range(0, KV_LEN)])

    def initial_state(self):
        """Return a dict with KV_LEN zero byte values for all keys"""
        all_zeros = b''.join([b'\x00' for _ in range(0, KV_LEN)])
        return dict([(k, all_zeros) for k in self.keys])

    async def _create_clients(self):
        for client_id in range(self.config.n,
                               self.config.num_clients+self.config.n):
            config = self._bft_config(client_id)
            self.clients[client_id] = bft_client.UdpClient(config, self.replicas)

    async def new_client(self):
        client_id = max(self.clients.keys()) + 1
        config = self._bft_config(client_id)
        client = bft_client.UdpClient(config, self.replicas)
        self.clients[client_id] = client
        return client

    def _bft_config(self, client_id):
        return bft_config.Config(client_id,
                                 self.config.f,
                                 self.config.c,
                                 MAX_MSG_SIZE,
                                 REQ_TIMEOUT_MILLI,
                                 RETRY_TIMEOUT_MILLI)

    async def _init_metrics(self):
        metric_clients = {}
        for r in self.replicas:
            mr = bft_config.Replica(r.id, r.ip, r.port + 1000)
            metric_clients[r.id] = bft_metrics_client.MetricsClient(mr)
        self.metrics = bft_metrics.BftMetrics(metric_clients)

    async def init(self):
        """
        Perform all necessary async initialization.
        This must be called before using a KvbTester instance.
        """
        await self._create_clients()
        await self._init_metrics()

    def random_value(self):
        return bytes(random.sample(self.alphanum, KV_LEN))

    def random_values(self, n):
        return [self.random_value() for _ in range(0, n)]

    def random_client(self):
        return random.choice(list(self.clients.values()))

    def random_clients(self, max_clients):
        return set(random.choices(list(self.clients.values()), k=max_clients))

    def random_key(self):
        return random.choice(self.keys)

    def random_keys(self, max_keys):
        """Return a set of keys that is of size <= max_keys"""
        return set(random.choices(self.keys, k=max_keys))

    def start_all_replicas(self):
        [self.start_replica(i) for i in range(0, self.config.n)]

    def stop_all_replicas(self):
        """ Stop all running replicas"""
        for p in self.procs.values():
            p.kill()
            p.wait()

        self.procs = {}

    def start_replica(self, replica_id):
        """
        Start a replica if it isn't already started.
        Otherwise raise an AlreadyStoppedError.
        """
        if replica_id in self.procs:
            raise AlreadyRunningError
        cmd = self.config.start_replica_cmd(self.builddir, replica_id)
        self.procs[replica_id] = subprocess.Popen(cmd, close_fds=True)

    def stop_replica(self, replica):
        """
        Stop a replica if it is running.
        Otherwise raise an AlreadyStoppedError.
        """
        if replica not in self.procs:
            raise AlreadyStoppedError
        p = self.procs[replica]
        p.kill()
        p.wait()

        del self.procs[replica]

    async def send_indefinite_write_requests(self, client, msg):
        while True:
            try:
                await client.write(msg)
            except:
                pass
            await trio.sleep(.1)

    async def wait_for_fetching_state(self, replica_id):
        """
        Check metrics on fetching replic ato see if the replica is in a
        fetching state
        """
        with trio.fail_after(10): # seconds
            while True:
                with trio.move_on_after(.2): # seconds
                    if await self.is_fetching(replica_id):
                        return

    async def is_fetching(self, replica_id):
        """Return whether the current replica is fetching state"""
        key = ['bc_state_transfer', 'Statuses', 'fetching_state']
        state = await self.metrics.get(replica_id, *key)
        return state != "NotFetching"

    async def wait_for_state_transfer_to_start(self):
        """
        Retry checking every .5 seconds until state transfer starts at least one
        node. Stop trying, and fail the test after 30 seconds.
        """
        with trio.fail_after(30): # seconds
            async with trio.open_nursery() as nursery:
                for replica in self.replicas:
                    nursery.start_soon(self._wait_to_receive_st_msgs,
                                       replica,
                                       nursery.cancel_scope)

    async def _wait_to_receive_st_msgs(self, replica, cancel_scope):
        """
        Check metrics to see if state transfer started. If so cancel the
        concurrent coroutines in the request scope.
        """
        while True:
            with trio.move_on_after(.5): # seconds
                key = ['replica', 'Counters', 'receivedStateTransferMsgs']
                n = await self.metrics.get(replica.id, *key)
                if n > 0:
                    cancel_scope.cancel()

    async def wait_for_state_transfer_to_stop(self,
                                              up_to_date_node,
                                              stale_node):
        with trio.fail_after(30): # seconds
            # Get the lastExecutedSeqNumber from a started node
            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
            last_exec_seq_num = await self.metrics.get(up_to_date_node, *key)
            last_n = -1;
            while True:
                with trio.move_on_after(.5): # seconds
                    metrics = await self.metrics.get_all(stale_node)
                    n = self.metrics.get_local(metrics, *key)

                    # Debugging
                    if n != last_n:
                        last_n = n
                        checkpoint = ['bc_state_transfer',
                                'Gauges', 'last_stored_checkpoint']
                        on_transferring_complete = ['bc_state_transfer',
                                'Counters', 'on_transferring_complete']
                        print("wait_for_st_to_stop: last_exec_seq_num={} "
                              "last_stored_checkpoint={} "
                              "on_transferring_complete_count={}".format(
                                    n,
                                    self.metrics.get_local(metrics, *checkpoint),
                                    self.metrics.get_local(metrics,
                                        *on_transferring_complete)))
                    # Exit condition
                    if n >= last_exec_seq_num:
                       return

    async def is_state_transfer_complete(self, up_to_date_node, stale_node):
            # Get the lastExecutedSeqNumber from a reference node
            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
            last_exec_seq_num = await self.metrics.get(up_to_date_node, *key)
            n = await self.metrics.get(stale_node, *key)
            return n == last_exec_seq_num

    async def wait_for_replicas_to_checkpoint(self, replica_ids, checkpoint_num):
        """
        Wait for every replica in `replicas` to take a checkpoint.
        Check every .5 seconds and give fail after 30 seconds.
        """
        with trio.fail_after(30): # seconds
            async with trio.open_nursery() as nursery:
                for replica_id in replica_ids:
                    nursery.start_soon(self.wait_for_checkpoint, replica_id,
                            checkpoint_num)

    async def wait_for_checkpoint(self, replica_id, checkpoint_num):
        key = ['bc_state_transfer', 'Gauges', 'last_stored_checkpoint']
        while True:
            with trio.move_on_after(.5): # seconds
                n = await self.metrics.get(replica_id, *key)
                if n == checkpoint_num:
                    return

    async def assert_state_transfer_not_started_all_up_nodes(self, testcase):
        with trio.fail_after(METRICS_TIMEOUT_SEC):
            # Check metrics for all started nodes in parallel
            async with trio.open_nursery() as nursery:
                for r in self.replicas[0:3]:
                    nursery.start_soon(self._assert_state_transfer_not_started,
                                       testcase,
                                       r)

    async def _assert_state_transfer_not_started(self, testcase, replica):
        key = ['replica', 'Counters', 'receivedStateTransferMsgs']
        n = await self.metrics.get(replica.id, *key)
        testcase.assertEqual(0, n)

    async def assert_successful_put_get(self, testcase, protocol):
        """ Assert that we can get a valid put """
        client = self.random_client()
        read_reply = await client.read(protocol.get_last_block_req())
        last_block = protocol.parse_reply(read_reply)

        # Perform an unconditional KV put.
        # Ensure that the block number increments.
        key = self.random_key()
        val = self.random_value()

        reply = await client.write(protocol.write_req([], [(key, val)], 0))
        reply = protocol.parse_reply(reply)
        testcase.assertTrue(reply.success)
        testcase.assertEqual(last_block + 1, reply.last_block_id)

        # Retrieve the last block and ensure that it matches what's expected
        read_reply = await client.read(protocol.get_last_block_req())
        newest_block = protocol.parse_reply(read_reply)
        testcase.assertEqual(last_block+1, newest_block)

        # Get the previous put value, and ensure it's correct
        read_req = protocol.read_req([key], newest_block)
        kvpairs = protocol.parse_reply(await client.read(read_req))
        testcase.assertDictEqual({key: val}, kvpairs)

    async def wait_for(self, predicate, timeout, interval):
        """
        Wait for the given async predicate function to return true. Give up
        waiting for the async function to complete after interval (seconds) and retry
        until timeout (seconds) expires. Raise trio.TooSlowError when timeout expires.

        Important:
         * The given predicate function must be async
         * Retries may occur more frequently than interval if the predicate
           returns false before interval expires. This only matters in that it
           uses more CPU.
        """
        with trio.fail_after(timeout):
            while True:
                with trio.move_on_after(interval):
                    if await predicate():
                        return


