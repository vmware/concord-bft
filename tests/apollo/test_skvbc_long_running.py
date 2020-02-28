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
import random
import unittest
import logging
import trio

from util import bft_network_partitioning as net
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability
from test_skvbc import SkvbcTest

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
    async def test_stability(self, bft_network):
        with trio.move_on_after(seconds=ONE_HOUR_IN_SECONDS):
            while True:
                await SkvbcTest().test_conflicting_write(bft_network=bft_network, already_in_trio=True)
                bft_network.stop_all_replicas()
