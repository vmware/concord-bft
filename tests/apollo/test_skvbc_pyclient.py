# Concord
# 
# Copyright (c) 2021 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

# Apollo test suite for testing certain behaviors of the pyclient implementation
# (i.e. bft_client.BftClient and its implementations from
# concord-bft/util/pyclient/bft_client.py). Note the test cases in this suite
# are re-implementations of a previous collection of tests that were originally
# implemented outside Apollo, and also note that this test suite does not
# necessarilly constitute comprehensive testing of the pyclient implementation.
#
# Note the standard Apollo infrastructure from tests/apollo/util/bft.py uses
# pyclient, so this test suite relies on that infrastructure to create Concord
# test clusters and pyclients connected to them.

import os.path
import sys
import trio
import unittest

sys.path.append(os.path.abspath("../../util/pyclient"))

from bft_client import MofNQuorum
from util import skvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each argument is an element in a list.
    """
    status_timer_milli = "500"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", \
        "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            ]

class SkvbcPyclientTest(unittest.TestCase):

    
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_timeout(self, bft_network):
        """
        Test that pyclient times out any read and/or write requests if no
        servers are running.
        """

        client = bft_network.random_client()
        protocol = skvbc.SimpleKVBCProtocol(bft_network)

        key = protocol.random_key()
        kv_pair = [(key, protocol.random_value())]
        with self.assertRaises(trio.TooSlowError, msg="A BFT Client failed " \
                "to time out a write request to a non-running cluster."):
            await client.write(protocol.write_req([], kv_pair, 0))
        with self.assertRaises(trio.TooSlowError, msg="A BFT Client failed " \
                "to time out a read request to a non-running cluster."):
            await client.read(protocol.read_req([key]))

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_read_written_value(self, bft_network):
        """
        Test that pyclient can write a value to a running Concord-BFT cluster
        and then read that value back.
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        protocol = skvbc.SimpleKVBCProtocol(bft_network)
        
        key = protocol.random_key()
        value = protocol.random_value()
        kv_pair = [(key, value)]
        await client.write(protocol.write_req([], kv_pair, 0))
        read_result = await client.read(protocol.read_req([key]))
        value_read = (protocol.parse_reply(read_result))[key]
        self.assertEqual(value, value_read, "A BFT Client failed to read a " \
                "key-value pair from a SimpleKVBC cluster matching the " \
                "key-value pair it wrote immediately prior to the read.")

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_retry(self, bft_network):
        """
        Test that pyclient retries a request before giving up if no servers are
        running.
        """

        client = bft_network.random_client()
        protocol = skvbc.SimpleKVBCProtocol(bft_network)

        kv_pair = [(protocol.random_key(), protocol.random_value())]
        self.assertEqual(client.retries, 0, "A BFT Client reported non-zero " \
                "number of retries before sending any requests.")
        with self.assertRaises(trio.TooSlowError, msg="A BFT Client failed " \
                "to time out a write request to a non-running cluster."):
            await client.write(protocol.write_req([], kv_pair, 0))
        self.assertGreater(client.retries, 0, "A BFT Client did not report " \
                "having made any retries even after attempting a write to a " \
                "non-running cluster.")

    @unittest.skip("Skip due to instability. Tracked in BC-9404")
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_primary_write(self, bft_network):
        """
        Test that pyclient learns the primary after a successful request and
        that sending a request to the primary only succeeds.
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        protocol = skvbc.SimpleKVBCProtocol(bft_network)

        key = protocol.random_key()
        value = protocol.random_value()
        self.assertIsNone(client.primary, "A BFT Client reported knowing the " \
                "current primary before sending any requests.")
        await client.write(protocol.write_req([], [(key, value)], 0))
        self.assertIsNotNone(client.primary, "A BFT Client reported not " \
                "knowing the current primary after making a write request.")
        
        read_result = await client.read(protocol.read_req([key]))
        expected_messages_sent = client.msgs_sent
        self.assertIsNotNone(client.primary, "A BFT Client reported not " \
                "knowing the current primary after making a read request.")
        value_read = (protocol.parse_reply(read_result))[key]
        self.assertEqual(value, value_read, "A BFT Client failed to read a " \
                "key-value pair from a SimpleKVBC cluster matching the " \
                "key-value pair it wrote immediately prior to the read.")

        value = protocol.random_value()
        await client.write(protocol.write_req([], [(key, value)], 1))
        expected_messages_sent += 1 # We expect this request will only be sent
                                    # to the primary.
        self.assertIsNotNone(client.primary, "A BFT Client reported not " \
                "knowing the current primary after making a write request.")
        self.assertEqual(expected_messages_sent, client.msgs_sent, "A BFT " \
                "Client reported having sent an unexpected number of " \
                "messages after a read request.")
        
        read_result = await client.read(protocol.read_req([key]))
        self.assertIsNotNone(client.primary, "A BFT Client reported not " \
                "knowing the current primary after making a read request.")
        value_read = (protocol.parse_reply(read_result))[key]
        self.assertEqual(value, value_read, "A BFT Client failed to read a " \
                "key-value pair from a SimpleKVBC cluster matching the " \
                "key-value pair it wrote immediately prior to the read.")

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_m_of_n_quorum(self, bft_network):
        """
        Test that pyclient successfully makes a read request when told to wait
        for an M of N quorum of a particular size.
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        protocol = skvbc.SimpleKVBCProtocol(bft_network)
        
        key = protocol.random_key()
        value = protocol.random_value()
        kv_pair = [(key, value)]
        await client.write(protocol.write_req([], kv_pair, 0))

        quorum = MofNQuorum.LinearizableQuorum(
            bft_network.config, bft_network.all_replicas())
        read_result = await client.read(protocol.read_req([key]), \
                                        m_of_n_quorum=quorum)
        value_read = (protocol.parse_reply(read_result))[key]
        self.assertEqual(value, value_read, "Using an M of N Quorum of M==2f+c+1, "
                "A BFT Client failed to read a key-value pair from a " \
                "SimpleKVBC cluster matching the key-value pair it wrote " \
                "immediately prior to the read.")

if __name__ == '__main__':
    unittest.main()
