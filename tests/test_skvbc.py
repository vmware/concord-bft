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

import unittest
import trio
import os.path

from util import bft, skvbc

KEY_FILE_PREFIX = "replica_keys_"


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

    def setUp(self):
        self.protocol = skvbc.SimpleKVBCProtocol()

    def test_state_transfer(self):
        """
        Test that state transfer starts and completes.

        Stop one node, add a bunch of data to the rest of the cluster, restart
        the node and verify state transfer works as expected. In a 4 node
        cluster with f=1 we should be able to stop a different node after state
        transfer completes and still operate correctly.
        """
        trio.run(self._test_state_transfer)

    async def _test_state_transfer(self):
        config = bft.TestConfig(n=4,
                                f=1,
                                c=0,
                                num_clients=10,
                                key_file_prefix=KEY_FILE_PREFIX,
                                start_replica_cmd=start_replica_cmd)
        with bft.BftTestNetwork(config) as bft_network:
            await bft_network.init()
            [bft_network.start_replica(i) for i in range(3)]
            # Write enough data to create a need for state transfer
            # Run num_clients concurrent requests a bunch of times
            for i in range (100):
                async with trio.open_nursery() as nursery:
                    for client in bft_network.clients.values():
                        msg = self.protocol.write_req([],
                                [(bft_network.random_key(), bft_network.random_value())],
                                0)
                        nursery.start_soon(client.sendSync, msg, False)
            await bft_network.assert_state_transfer_not_started_all_up_nodes(self)
            bft_network.start_replica(3)
            await bft_network.wait_for_state_transfer_to_start()
            await bft_network.wait_for_state_transfer_to_stop(0, 3)
            await bft_network.assert_successful_put_get(self, self.protocol)
            bft_network.stop_replica(2)
            await bft_network.assert_successful_put_get(self, self.protocol)

    def test_get_block_data(self):
        """
        Ensure that we can put a block and use the GetBlockData API request to
        retrieve its KV pairs.
        """
        trio.run(self._test_get_block_data)

    async def _test_get_block_data(self):
        config = bft.TestConfig(n=4,
                                f=1,
                                c=0,
                                num_clients=10,
                                key_file_prefix=KEY_FILE_PREFIX,
                                start_replica_cmd=start_replica_cmd)
        with bft.BftTestNetwork(config) as bft_network:
            await bft_network.init()
            [bft_network.start_replica(i) for i in range(3)]
            p = self.protocol
            client = bft_network.random_client()
            last_block = p.parse_reply(await client.read(p.get_last_block_req()))

            # Perform an unconditional KV put.
            # Ensure keys aren't identical
            kv = [(bft_network.keys[0], bft_network.random_value()),
                  (bft_network.keys[1], bft_network.random_value())]

            reply = await client.write(p.write_req([], kv, 0))
            reply = p.parse_reply(reply)
            self.assertTrue(reply.success)
            self.assertEqual(last_block + 1, reply.last_block_id)

            last_block = reply.last_block_id

            # Get the kvpairs in the last written block
            data = await client.read(p.get_block_data_req(last_block))
            kv2 = p.parse_reply(data)
            self.assertDictEqual(kv2, dict(kv))

            # Write another block with the same keys but (probabilistically)
            # different data
            kv3 = [(bft_network.keys[0], bft_network.random_value()),
                   (bft_network.keys[1], bft_network.random_value())]
            reply = await client.write(p.write_req([], kv3, 0))
            reply = p.parse_reply(reply)
            self.assertTrue(reply.success)
            self.assertEqual(last_block + 1, reply.last_block_id)

            # Get the kvpairs in the previously written block
            data = await client.read(p.get_block_data_req(last_block))
            kv2 = p.parse_reply(data)
            self.assertDictEqual(kv2, dict(kv))

    def test_conflicting_write(self):
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
        trio.run(self._test_conflicting_write)

    async def _test_conflicting_write(self):
        config = bft.TestConfig(n=4,
                                f=1,
                                c=0,
                                num_clients=1,
                                key_file_prefix=KEY_FILE_PREFIX,
                                start_replica_cmd=start_replica_cmd)

        with bft.BftTestNetwork(config) as bft_network:
            await bft_network.init()
            bft_network.start_all_replicas()

            key = bft_network.random_key()

            write_1 = self.protocol.write_req(
                readset=[],
                writeset=[(key, bft_network.random_value())],
                block_id=0)

            write_2 = self.protocol.write_req(
                readset=[],
                writeset=[(key, bft_network.random_value())],
                block_id=0)

            client = bft_network.random_client()

            await client.write(write_1)
            last_write_reply = \
                self.protocol.parse_reply(await client.write(write_2))

            last_block_id = last_write_reply.last_block_id

            key_prime = bft_network.random_key()

            # this write is conflicting because the writeset (key_prime) is
            # based on an outdated version of the readset (key)
            conflicting_write = self.protocol.write_req(
                readset=[key],
                writeset=[(key_prime, bft_network.random_value())],
                block_id=last_block_id-1)

            write_result = \
                self.protocol.parse_reply(await client.write(conflicting_write))
            successful_write = write_result.success

            self.assertTrue(not successful_write)

if __name__ == '__main__':
    unittest.main()
