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

import bft_tester
import skvbc
import bft_client
import bft_metrics_client

KEY_FILE_PREFIX = "replica_keys_"


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    path = os.path.join(builddir, "bftengine",
            "tests", "simpleKVBCTests", "TesterReplica", "skvbc_replica")
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
        config = bft_tester.TestConfig(n=4,
                                       f=1,
                                       c=0,
                                       num_clients=10,
                                       key_file_prefix=KEY_FILE_PREFIX,
                                       start_replica_cmd=start_replica_cmd)
        with bft_tester.BftTester(config) as tester:
            await tester.init()
            [tester.start_replica(i) for i in range(3)]
            self.protocol = skvbc.SimpleKVBCProtocol()
            # Write enough data to create a need for state transfer
            # Run num_clients concurrent requests a bunch of times
            for i in range (100):
                async with trio.open_nursery() as nursery:
                    for client in tester.clients.values():
                        msg = self.protocol.write_req([],
                                [(tester.random_key(), tester.random_value())])
                        nursery.start_soon(client.sendSync, msg, False)
            await tester.assert_state_transfer_not_started_all_up_nodes(self)
            tester.start_replica(3)
            await tester.wait_for_state_transfer_to_start()
            await tester.wait_for_state_transfer_to_stop(0, 3)
            await self.assert_successful_put_get(tester)
            tester.stop_replica(2)
            await self.assert_successful_put_get(tester)

    async def assert_successful_put_get(self, tester):
        p = self.protocol
        client = tester.random_client()
        last_block = p.parse_reply(await client.read(p.get_last_block_req()))

        # Perform an unconditional KV put.
        # Ensure that the block number increments.
        key = tester.random_key()
        val = tester.random_value()

        reply = await client.write(p.write_req([], [(key, val)]))
        reply = p.parse_reply(reply)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        # Retrieve the last block and ensure that it matches what's expected
        newest_block = p.parse_reply(await client.read(p.get_last_block_req()))
        self.assertEqual(last_block+1, newest_block)

        # Get the previous put value, and ensure it's correct
        read_req = p.read_req([key], newest_block)
        kvpairs = p.parse_reply(await client.read(read_req))
        self.assertDictEqual({key: val}, kvpairs)

if __name__ == '__main__':
    unittest.main()
