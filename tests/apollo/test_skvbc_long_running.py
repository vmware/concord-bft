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
import itertools
import os.path
import unittest
import trio
import time
from os import environ
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from test_skvbc import SkvbcTest
from test_skvbc_fast_path import SkvbcFastPathTest
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
            "-v", viewChangeTimeoutMilli,
            "-p",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcLongRunningTest(unittest.TestCase):


    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: n == 7)
    async def test_stability(self, bft_network):
        bft_network.start_all_replicas()
        start = time.time()
        with trio.move_on_after(seconds=ONE_HOUR_IN_SECONDS*72):
            for i in itertools.count():
                await SkvbcTest().test_get_block_data\
                    (bft_network=bft_network, already_in_trio=True)
                await trio.sleep(seconds=120)
                await SkvbcTest().test_conflicting_write\
                    (bft_network=bft_network, already_in_trio=True)
                await trio.sleep(seconds=120)
                end = time.time()
                if end - start >= ONE_HOUR_IN_SECONDS*2:
                    await SkvbcViewChangeTest().test_single_vc_only_primary_down \
                      (bft_network=bft_network, already_in_trio=True, disable_linearizability_checks=True)
                    await trio.sleep(seconds=180)
                    start = time.time()
