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

from util import blinking_replica
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX


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
            "-e", str(True)
            ]


class SkvbcTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_state_transfer(self, bft_network,exchange_keys=True):
        """
        Test that state transfer starts and completes.

        Stop one node, add a bunch of data to the rest of the cluster, restart
        the node and verify state transfer works as expected. We should be able
        to stop a different set of f nodes after state transfer completes and
        still operate correctly.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        stale_node = random.choice(
            bft_network.all_replicas(without={0}))

        await skvbc.prime_for_state_transfer(
            stale_nodes={stale_node},
            checkpoints_num=3, # key-exchange channges the last executed seqnum
            persistency_enabled=False
        )
        bft_network.start_replica(stale_node)
        await bft_network.wait_for_state_transfer_to_start()
        await bft_network.wait_for_state_transfer_to_stop(0, stale_node)
        await skvbc.assert_successful_put_get(self)
        await bft_network.force_quorum_including_replica(stale_node)
        await skvbc.assert_successful_put_get(self)

    

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_request_missing_data_from_previous_window(self, bft_network, exchange_keys=True):
        """
        Test that a replica succeeds to ask for missing info from the former window

        1. Start all nodes and process 149 requests.
        2. Stop f nodes and process 2 more requests (this will cause to the remaining
            replicas to proceed beyond the checkpoint)
        3. The node should catchup without executing state transfer.
        """

        if exchange_keys:
            await bft_network.do_key_exchange()
        bft_network.start_all_replicas()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        stale_nodes = bft_network.random_set_of_replicas(bft_network.config.f, without={0})

        for i in range(151):
            await skvbc.write_known_kv()
            if i == 149:
                bft_network.stop_replicas(stale_nodes)

        with trio.fail_after(seconds=30):
            all_in_checkpoint = False
            while all_in_checkpoint is False:
                all_in_checkpoint = True
                for r in bft_network.all_replicas(without=stale_nodes):
                    if await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum") != 150:
                        all_in_checkpoint = False
                        break

        bft_network.start_replicas(stale_nodes)

        with self.assertRaises(trio.TooSlowError):
            await bft_network.wait_for_state_transfer_to_start()

        with trio.fail_after(seconds=30):
            all_in_checkpoint = False
            while all_in_checkpoint is False:
                all_in_checkpoint = True
                for r in stale_nodes:
                    if await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum") != 150:
                        all_in_checkpoint = False
                        break

        await skvbc.assert_successful_put_get(self)

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
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
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_get_block_data(self, bft_network,exchange_keys=True):
        """
        Ensure that we can put a block and use the GetBlockData API request to
        retrieve its KV pairs.
        """
    
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        last_block = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))

        # Perform an unconditional KV put.
        # Ensure keys aren't identical
        kv = [(skvbc.keys[0], skvbc.random_value()),
              (skvbc.keys[1], skvbc.random_value())]

        reply = await client.write(skvbc.write_req([], kv, 0))
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        last_block = reply.last_block_id

        # Get the kvpairs in the last written block
        data = await client.read(skvbc.get_block_data_req(last_block))
        kv2 = skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))

        # Write another block with the same keys but (probabilistically)
        # different data
        kv3 = [(skvbc.keys[0], skvbc.random_value()),
               (skvbc.keys[1], skvbc.random_value())]
        reply = await client.write(skvbc.write_req([], kv3, 0))
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        # Get the kvpairs in the previously written block
        data = await client.read(skvbc.get_block_data_req(last_block))
        kv2 = skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_conflicting_write(self, bft_network, rotate_keys=True):
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
