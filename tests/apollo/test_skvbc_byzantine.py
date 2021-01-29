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
from os import environ

from util import blinking_replica
from util import eliot_logging as log
from util import skvbc as kvbc
from util.bft import KEY_FILE_PREFIX, with_trio
from util.bft import with_bft_network


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "ByzantineReplica", "byzantine_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-e", str(True),
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcByzantineTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario
    
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_get_block_data(self, bft_network, exchange_keys=True):
        """
        Ensure that we can put a block and use the GetBlockData API request to
        retrieve its KV pairs.
        """

        if exchange_keys:
            await bft_network.do_key_exchange()

        log.log_message("**** after exchange keys milestone")

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

if __name__ == '__main__':
    unittest.main()
