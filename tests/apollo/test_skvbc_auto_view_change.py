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

from util.test_base import ApolloTest
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "1000"
    autoPrimaryRotationTimeoutMilli = "5000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-a", autoPrimaryRotationTimeoutMilli
            ]


class SkvbcAutoViewChangeTest(ApolloTest):

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability()
    async def test_auto_vc_all_nodes_up_no_requests(self, bft_network, tracker):
        """
        This test aims to validate automatic view change
        in the absence of any client messages:
        1) Start a full BFT network
        2) Do nothing (wait for automatic view change to kick-in)
        3) Check that view change has occurred (necessarily, automatic view change)
        4) Perform a "read-your-writes" check in the new view
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        initial_primary = 0

        # do nothing - just wait for an automatic view change
        await bft_network.wait_for_view(
            replica_id=random.choice(
                bft_network.all_replicas(without={initial_primary})),
            expected=lambda v: v > initial_primary,
            err_msg="Make sure automatic view change has occurred."
        )

        await skvbc.read_your_writes()

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability()
    async def test_auto_vc_when_primary_down(self, bft_network, tracker):
        """
        This test aims to validate automatic view change
        when the primary is down
        1) Start a full BFT network
        2) Stop the initial primary replica
        3) Do nothing (wait for automatic view change to kick-in)
        4) Check that view change has occurred (necessarily, automatic view change)
        5) Perform a "read-your-writes" check in the new view
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        initial_primary = 0
        bft_network.stop_replica(initial_primary)

        # do nothing - just wait for an automatic view change
        await bft_network.wait_for_view(
            replica_id=random.choice(
                bft_network.all_replicas(without={initial_primary})),
            expected=lambda v: v > initial_primary,
            err_msg="Make sure automatic view change has occurred."
        )

        await skvbc.read_your_writes()

    @unittest.skip("Unstable because of BC-5101")
    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability()
    async def test_auto_vc_all_nodes_up_fast_path(self, bft_network, tracker):
        """
        This test aims to validate automatic view change
        while messages are being processed on the fast path
        1) Start a full BFT network
        2) Send a batch of write commands
        3) Make sure view change occurred at some point while processing the writes
        4) Check that all writes have been processed on the fast commit path
        5) Perform a "read-your-writes" check in the new view
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        initial_primary = 0

        for _ in range(150):
            await skvbc.send_write_kv_set()

        await bft_network.wait_for_view(
            replica_id=random.choice(
                bft_network.all_replicas(without={initial_primary})),
            expected=lambda v: v > initial_primary,
            err_msg="Make sure automatic view change has occurred."
        )

        await skvbc.assert_kv_write_executed(key, val)
        await bft_network.assert_fast_path_prevalent()

        await skvbc.read_your_writes()
