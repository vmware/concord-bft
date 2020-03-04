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
import subprocess

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability

from test_skvbc import SkvbcTest
from test_skvbc_fast_path import SkvbcFastPathTest
from test_skvbc_linearizability import SkvbcChaosTest
from test_skvbc_view_change import SkvbcViewChangeTest

# Time consts
EIGHT_HOURS_IN_SECONDS = 8 * 60 * 60
ONE_HOUR_IN_SECONDS = 1 * 60 * 60


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli]


class SkvbcLongRunningTest(unittest.TestCase):

  
    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability
    async def test_stability(self, bft_network, tracker):
        bft_network.start_all_replicas()
        with trio.move_on_after(seconds=4*ONE_HOUR_IN_SECONDS):
            while True:
                await SkvbcTest().test_get_block_data\
                    (bft_network=bft_network, already_in_trio=True, tracker=tracker)
                self.start_all_replicas(bft_network)
                await trio.sleep(seconds=10)
                await SkvbcFastPathTest().test_fast_path_read_your_write\
                    (bft_network=bft_network, already_in_trio=True, tracker=tracker)
                self.start_all_replicas(bft_network)
                await trio.sleep(seconds=10)
                await SkvbcChaosTest().test_healthy\
                    (bft_network=bft_network, already_in_trio=True, tracker=tracker)
                self.start_all_replicas(bft_network)
                await trio.sleep(seconds=10)
                await SkvbcChaosTest().test_wreak_havoc\
                    (bft_network=bft_network, already_in_trio=True, tracker=tracker)
                self.start_all_replicas(bft_network)
                await trio.sleep(seconds=10)



    def start_replica(self, bft_network, replica_id):
        """
        Start a replica if it isn't already started.
        Otherwise raise an AlreadyStoppedError.
        """
        if replica_id in bft_network.procs:
            return
        cmd = bft_network.start_replica_cmd(replica_id)
        bft_network.procs[replica_id] = subprocess.Popen(cmd, close_fds=True)

    def start_all_replicas(self, bft_network):
        [self.start_replica(bft_network, i) for i in range(0, bft_network.config.n)]