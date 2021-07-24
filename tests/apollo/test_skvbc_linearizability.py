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
import unittest
from os import environ

import trio

from util import skvbc as kvbc
from util.skvbc_history_tracker import verify_linearizability
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
             "-e", str(True),
            "-v", viewChangeTimeoutMilli
            ]


class SkvbcChaosTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    @verify_linearizability()
    async def test_healthy(self, bft_network, tracker,exchange_keys=True):
        """
        Run a bunch of concurrrent requests in batches and verify
        linearizability. The system is healthy and stable and no faults are
        intentionally generated.
        """
        num_ops = 500

        self.skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        self.bft_network = bft_network
        self.bft_network.start_all_replicas()
        await self.skvbc.run_concurrent_ops(num_ops)

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    @verify_linearizability()
    async def test_wreak_havoc(self, bft_network, tracker):
        """
        Run a bunch of concurrent requests in batches and verify
        linearizability. In this test we generate faults periodically and verify
        linearizability at the end of the run.
        """
        num_ops = 500

        self.skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        self.bft_network = bft_network
        self.bft_network.start_all_replicas()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.skvbc.run_concurrent_ops, num_ops)
            nursery.start_soon(self.crash_primary)

    async def crash_primary(self):
        await trio.sleep(.5)
        self.bft_network.stop_replica(0)

if __name__ == '__main__':
    unittest.main()
