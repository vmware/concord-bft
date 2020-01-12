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

from test_skvbc_linearizability import KEY_FILE_PREFIX
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network


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
            "-a", autoPrimaryRotationTimeoutMilli]


class SkvbcAutoViewChangeTest(unittest.TestCase):

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_auto_vc_all_nodes_up_no_requests(self, bft_network):
        """
        This test aims to validate automatic view change
        in the absence of any client messages:
        1) Start a full BFT network
        2) Do nothing (wait for automatic view change to kick-in)
        3) Check that view change has occurred (necessarily, automatic view change)
        """
        bft_network.start_all_replicas()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        initial_primary = 0

        # do nothing - just wait for an automatic view change
        await bft_network.wait_for_view(replica_id=random.choice(
            bft_network.all_replicas(without={initial_primary})), expected=lambda v: v > initial_primary,
            err_msg="Make sure automatic view change has occurred.")

        self._read_your_writes(bft_network,skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_auto_vc_when_primary_down(self, bft_network):
        """
        This test aims to validate automatic view change
        when the primary is down
        1) Start a full BFT network
        2) Stop the initial primary replica
        3) Do nothing (wait for automatic view change to kick-in)
        4) Check that view change has occurred (necessarily, automatic view change)
        """
        bft_network.start_all_replicas()

        initial_primary = 0
        bft_network.stop_replica(initial_primary)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        # do nothing - just wait for an automatic view change
        await bft_network.wait_for_view(replica_id=random.choice(
            bft_network.all_replicas(without={initial_primary})), expected=lambda v: v > initial_primary,
            err_msg="Make sure automatic view change has occurred.")

        self._read_your_writes(bft_network, skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_auto_vc_all_nodes_up_fast_path(self, bft_network):
        """
        This test aims to validate automatic view change
        while messages are being processed on the fast path
        1) Start a full BFT network
        2) Send a batch of write commands
        3) Make sure view change occurred at some point while processing the writes
        4) Check that all writes have been processed on the fast commit path
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        initial_primary = 0

        for _ in range(150):
            key, val = await skvbc.write_known_kv()

        await bft_network.wait_for_view(replica_id=random.choice(
            bft_network.all_replicas(without={initial_primary})), expected=lambda v: v > initial_primary,
            err_msg="Make sure automatic view change has occurred.")

        await skvbc.assert_kv_write_executed(key, val)
        await bft_network.assert_fast_path_prevalent()

        self._read_your_writes(bft_network,skvbc)

    async def _read_your_writes(self,bft_network, skvbc):
        # Verify by "Read your write"
        # Perform write with the new primary
        client = bft_network.random_client()
        last_block = skvbc.parse_reply(
            await client.read(skvbc.get_last_block_req()))
        # Perform an unconditional KV put.
        # Ensure keys aren't identical
        kv = [(skvbc.keys[0], skvbc.random_value()),
              (skvbc.keys[1], skvbc.random_value())]

        reply = await client.write(skvbc.write_req([], kv, 0))
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        last_block = reply.last_block_id

        # Read the last write and check if equal
        # Get the kvpairs in the last written block
        data = await client.read(skvbc.get_block_data_req(last_block))
        kv2 = skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))