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
import random
import unittest
import trio
import os.path

from util import bft
from util import skvbc as kvbc

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
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                stale_node = random.choice(list(set(range(config.n)) - {0}))
                await skvbc.prime_for_state_transfer(
                    stale_nodes={stale_node},
                    persistency_enabled=False
                )
                bft_network.start_replica(stale_node)
                await bft_network.wait_for_state_transfer_to_start()
                await bft_network.wait_for_state_transfer_to_stop(0, stale_node)
                await skvbc.assert_successful_put_get(self)
                random_replica = random.choice(
                    list(set(range(config.n)) - {0, stale_node}))
                bft_network.stop_replica(random_replica)
                await skvbc.assert_successful_put_get(self)

    def test_get_block_data(self):
        """
        Ensure that we can put a block and use the GetBlockData API request to
        retrieve its KV pairs.
        """
        trio.run(self._test_get_block_data)

    async def _test_get_block_data(self):
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
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
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
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
                    block_id=last_block_id-1)

                write_result = \
                    skvbc.parse_reply(await client.write(conflicting_write))
                successful_write = write_result.success

                self.assertTrue(not successful_write)

if __name__ == '__main__':
    unittest.main()
