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

import trio

from util import blinking_replica
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
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli]


class SkvbcTest(unittest.TestCase):

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_state_transfer(self, bft_network):
        """
        Test that state transfer starts and completes.

        Stop one node, add a bunch of data to the rest of the cluster, restart
        the node and verify state transfer works as expected. In a 4 node
        cluster with f=1 we should be able to stop a different node after state
        transfer completes and still operate correctly.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        stale_node = random.choice(
            bft_network.all_replicas(without={0}))

        await skvbc.prime_for_state_transfer(
            stale_nodes={stale_node},
            persistency_enabled=False
        )
        bft_network.start_replica(stale_node)
        await bft_network.wait_for_state_transfer_to_start()
        await bft_network.wait_for_state_transfer_to_stop(0, stale_node)
        await skvbc.assert_successful_put_get(self)
        random_replica = random.choice(
            bft_network.all_replicas(without={0, stale_node}))
        bft_network.stop_replica(random_replica)
        await skvbc.assert_successful_put_get(self)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_get_block_data_with_blinking_replica(self, bft_network):
        """
        Test that the cluster continues working when one blinking replica
        By a blinking replic we mean a replica that goes up and down for random
        period of time
        """
        with blinking_replica.BlinkingReplica() as blinking:
            br = random.choice(
                bft_network.all_replicas(without={0}))
            bft_network.start_replicas(replicas=bft_network.all_replicas(without={br}))
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)

            blinking.start_blinking(bft_network.start_replica_cmd(br))

            for _ in range(300):
                # Perform an unconditional KV put.
                # Ensure keys aren't identical
                await skvbc.read_your_writes(self)


    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_get_block_data(self, bft_network, tracker):
        """
        Ensure that we can put a block and use the GetBlockData API request to
        retrieve its KV pairs.
        """
        bft_network.start_all_replicas()
        client = tracker.bft_network.random_client()
        last_block = tracker.skvbc.parse_reply(await client.read(tracker.skvbc.get_last_block_req()))

        # Perform an unconditional KV put.
        # Ensure keys aren't identical
        kv = [(tracker.skvbc.keys[0], tracker.skvbc.random_value()),
              (tracker.skvbc.keys[1], tracker.skvbc.random_value())]

        reply = await tracker.write_and_track_known_kv(kv, client)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        last_block = reply.last_block_id

        # Get the kvpairs in the last written block
        data = await client.read(tracker.skvbc.get_block_data_req(last_block))
        kv2 = tracker.skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))

        # Write another block with the same keys but (probabilistically)
        # different data
        kv3 = [(tracker.skvbc.keys[0], tracker.skvbc.random_value()),
               (tracker.skvbc.keys[1], tracker.skvbc.random_value())]
        reply = await tracker.write_and_track_known_kv(kv3, client)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        # Get the kvpairs in the previously written block
        data = await client.read(tracker.skvbc.get_block_data_req(last_block))
        kv2 = tracker.skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_conflicting_write(self, bft_network):
        """
        The goal is to validate that a conflicting write request does not
        modify the blockchain state. Verifying this can be done as follows:
        1) write a key K several times with different values
        (to ensure several versions in the blockchain)
        2) create a (conflicting) conditional write on K'
        based on an old version of K
        3) execute the conflicting write
        4) verify K' is not written to the blockchain
        """
        bft_network.start_all_replicas()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        key = skvbc.random_key()

        write_1 = skvbc.write_req(
            readset=[],
            writeset=[(key, skvbc.random_value())],
            block_id=0)

        write_2 = skvbc.write_req(
            readset=[],
            writeset=[(key, skvbc.random_value())],
            block_id=0)

        client = bft_network.random_client()

        await client.write(write_1)
        last_write_reply = \
            skvbc.parse_reply(await client.write(write_2))

        last_block_id = last_write_reply.last_block_id

        key_prime = skvbc.random_key()

        # this write is conflicting because the writeset (key_prime) is
        # based on an outdated version of the readset (key)
        conflicting_write = skvbc.write_req(
            readset=[key],
            writeset=[(key_prime, skvbc.random_value())],
            block_id=last_block_id - 1)

        write_result = \
            skvbc.parse_reply(await client.write(conflicting_write))
        successful_write = write_result.success

        self.assertTrue(not successful_write)

if __name__ == '__main__':
    unittest.main()
