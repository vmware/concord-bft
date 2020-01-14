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
from util.skvbc_history_tracker import SkvbcTracker as with_tracker

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
        self.tracker = skvbc_history_tracker.SkvbcTracker(init_state)
        self.bft_network = bft_network
        self.status = Status(bft_network.config)
        bft_network.start_all_replicas()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.tracker.run_concurrent_ops, self, num_ops)

        await self.tracker.verify_linearizability(self)

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
            self.tracker = skvbc_history_tracker.SkvbcTracker(init_state)
            self.bft_network = bft_network
            self.status = Status(bft_network.config)
            bft_network.start_all_replicas()

            adversary.interfere()

            async with trio.open_nursery() as nursery:
                nursery.start_soon(self.tracker.run_concurrent_ops, self, num_ops)

            await self.tracker.verify_linearizability(self)

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
        self.tracker = skvbc_history_tracker.SkvbcTracker(init_state)
        self.bft_network = bft_network
        self.status = Status(bft_network.config)
        bft_network.start_all_replicas()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.tracker.run_concurrent_ops, self, num_ops)
            nursery.start_soon(self.tracker.crash_primary, self.bft_network)

        await self.tracker.verify_linearizability(self)

if __name__ == '__main__':
    unittest.main()
