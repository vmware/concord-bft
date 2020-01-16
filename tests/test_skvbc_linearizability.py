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

import os.path
import random
import time
import unittest

import trio

from util import bft_network_partitioning as net
from util import skvbc as kvbc
from util import skvbc_history_tracker
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX

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


class SkvbcChaosTest(unittest.TestCase):

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_healthy(self, bft_network):
        """
        Run a bunch of concurrrent requests in batches and verify
        linearizability. The system is healthy and stable and no faults are
        intentionally generated.
        """
        num_ops = 500

        self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        init_state = self.skvbc.initial_state()
        self.bft_network = bft_network
        self.tracker = skvbc_history_tracker.SkvbcTracker(init_state, self.skvbc, self.bft_network)
        self.bft_network.start_all_replicas()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.run_concurrent_ops, num_ops)

        await self.tracker.fill_missing_blocks_and_verify()

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_while_dropping_packets(self, bft_network):
        """
         Run a bunch of concurrrent requests in batches and verify
         linearizability, while dropping a small amount of packets
         between all replicas.
         """
        num_ops = 500

        with net.PacketDroppingAdversary(
                bft_network, drop_rate_percentage=5) as adversary:
            self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            init_state = self.skvbc.initial_state()
            self.bft_network = bft_network
            self.tracker = skvbc_history_tracker.SkvbcTracker(init_state, self.skvbc, self.bft_network)
            bft_network.start_all_replicas()

            adversary.interfere()

            async with trio.open_nursery() as nursery:
                nursery.start_soon(self.run_concurrent_ops, num_ops)

            await self.tracker.fill_missing_blocks_and_verify()

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_wreak_havoc(self, bft_network):
        """
        Run a bunch of concurrrent requests in batches and verify
        linearizability. In this test we generate faults periodically and verify
        linearizability at the end of the run.
        """

        num_ops = 500

        self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        init_state = self.skvbc.initial_state()
        self.bft_network = bft_network
        self.tracker = skvbc_history_tracker.SkvbcTracker(init_state, self.skvbc, self.bft_network)
        self.bft_network.start_all_replicas()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.run_concurrent_ops, num_ops)
            nursery.start_soon(self.crash_primary)

        await self.tracker.fill_missing_blocks_and_verify()

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
                        nursery.start_soon(self.tracker.send_tracked_write, client, max_size)
                    else:
                        nursery.start_soon(self.tracker.send_tracked_read, client, max_size)
            sent += len(clients)

    async def crash_primary(self):
        await trio.sleep(.5)
        self.bft_network.stop_replica(0)

if __name__ == '__main__':
    unittest.main()
