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

from google.protobuf import duration_pb2 as duration_proto

import trio

import os.path
import sys
sys.path.append(os.path.abspath("../../build/tests/apollo/util/"))
import skvbc_messages

from util.test_base import ApolloTest
from util import skvbc as kvbc
from util.pyclient import bft_grpc_client as client
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

    if os.environ.get('BLOCKCHAIN_VERSION', default="1").lower() == "4" :
        blockchain_version = "4"
    else :
        blockchain_version = "1"

    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-V",blockchain_version,
            "-v", viewChangeTimeoutMilli,
            "-e", str(True)
            ]

class SkvbcFullNodeTest(ApolloTest):

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd, num_fn=1, selected_configs=lambda n, f, c: n == 6)
    async def test_read_write_request(self, bft_network):

        print(f"Running test_read_write_request")
        bft_network.start_all_replicas()
        bft_network.start_all_fns()

        await trio.sleep(5)

        bft_client = client.GrpcClient()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        key = skvbc.random_key()
        value = skvbc.random_value()

        print(f"Random Value {value}")
        kv_pair = [(key, value)]

        msg = skvbc.write_req([], kv_pair, 0)
        res = bft_client.sendRequest(msg)
        #print(f"Write Response Raw {res}  ")

        write_res = skvbc.parse_reply(res)
        #print(f"Write Response Parsed {write_res.success} {write_res.last_block_id}")

        msg_read = skvbc.read_req([key])
        read_res = bft_client.sendRequest(msg_read, duration_proto.Duration(seconds =5), True)
        #print(f"Read Response Raw {read_res} ")

        value_read_dict = skvbc.parse_reply(read_res)
        value_read = value_read_dict[key]

        print(f"Random Value Read {value_read}")

        self.assertEqual(value, value_read, "A BFT Client failed to read a " \
            "key-value pair from a SimpleKVBC cluster matching the " \
            "key-value pair it wrote immediately prior to the read.")

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd, num_fn=1, selected_configs=lambda n, f, c: n == 6)
    async def test_read_write_restart_fn(self, bft_network):

        print(f"Running test_read_write_restart_fn")
        bft_network.start_all_replicas()
        bft_network.start_all_fns()

        await trio.sleep(5)

        bft_client = client.GrpcClient()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        key = skvbc.random_key()
        value = skvbc.random_value()

        print(f"Random Value Before Restart {value}")
        kv_pair = [(key, value)]

        msg = skvbc.write_req([], kv_pair, 0)
        res = bft_client.sendRequest(msg)
        #print(f"Write Response Raw {res}  ")

        #write_res = skvbc.parse_reply(res)
        #print(f"Write Response Parsed {write_res.success} {write_res.last_block_id}")

        bft_network.stop_fn()

        bft_network.start_all_fns()
        await trio.sleep(5)
        bft_client = client.GrpcClient()

        msg_read = skvbc.read_req([key])
        read_res = bft_client.sendRequest(msg_read, duration_proto.Duration(seconds =5), True)
        #print(f"Read Response Raw {read_res} ")

        value_read_dict = skvbc.parse_reply(read_res)
        value_read = value_read_dict[key]

        print(f"Random Value Read After Restart{value_read}")

        self.assertEqual(value, value_read, "A BFT Client failed to read a " \
            "key-value pair from a SimpleKVBC cluster matching the " \
            "key-value pair it wrote immediately prior to the read.")

        skvbc.parse_reply(res)

        print("Response received")


