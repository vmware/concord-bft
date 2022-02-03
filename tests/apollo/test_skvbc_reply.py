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

import sys
import os.path
import random
import unittest
import trio

sys.path.append(os.path.abspath("../../util/pyclient"))

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
import bft_msgs

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

class SkvbcReplyTest(unittest.TestCase):

    @unittest.skip("Unstable test - BC-18035")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0 and n > 6)
    async def test_expected_replies_from_replicas(self, bft_network):

        """
        1. Launch a cluster
        2. Select a random client
        3. Send write request with an expected reply
        4. Expected result: The reply should be same as the expected reply sent with write client request message
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        
        key = skvbc.random_key()
        value = skvbc.random_value()
        kv_pair = [(key, value)]

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.INVALID_REQUEST)
        assert reply[1] == bft_msgs.OperationResult.INVALID_REQUEST, \
                        f"Expected Reply={bft_msgs.OperationResult.INVALID_REQUEST}; actual={reply[1]}"

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.NOT_READY)
        assert reply[1] == bft_msgs.OperationResult.NOT_READY, \
                        f"Expected Reply={bft_msgs.OperationResult.NOT_READY}; actual={reply[1]}"

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.TIMEOUT)
        assert reply[1] == bft_msgs.OperationResult.TIMEOUT, \
                        f"Expected Reply={bft_msgs.OperationResult.TIMEOUT}; actual={reply[1]}"

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.EXEC_DATA_TOO_LARGE)
        assert reply[1] == bft_msgs.OperationResult.EXEC_DATA_TOO_LARGE, \
                        f"Expected Reply={bft_msgs.OperationResult.EXEC_DATA_TOO_LARGE}; actual={reply[1]}"

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.EXEC_DATA_EMPTY)
        assert reply[1] == bft_msgs.OperationResult.EXEC_DATA_EMPTY, \
                        f"Expected Reply={bft_msgs.OperationResult.EXEC_DATA_EMPTY}; actual={reply[1]}"

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.CONFLICT_DETECTED)
        assert reply[1] == bft_msgs.OperationResult.CONFLICT_DETECTED, \
                        f"Expected Reply={bft_msgs.OperationResult.CONFLICT_DETECTED}; actual={reply[1]}"

        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.OVERLOADED)
        assert reply[1] == bft_msgs.OperationResult.OVERLOADED, \
                        f"Expected Reply={bft_msgs.OperationResult.OVERLOADED}; actual={reply[1]}"
    
        reply = await client.write_with_result(skvbc.write_req([], kv_pair, 0), pre_process=True, result=bft_msgs.OperationResult.INTERNAL_ERROR)
        assert reply[1] == bft_msgs.OperationResult.INTERNAL_ERROR, \
                        f"Expected Reply={bft_msgs.OperationResult.INTERNAL_ERROR}; actual={reply[1]}"

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0 and n > 6)
    async def test_conflict_detected_from_replicas(self, bft_network):
    
        """
        1. Launch a cluster
        2. Select a random client
        3. Send a write request
        4. Send another conflicting write request
        5. Expected result: The received reply should be CONFLICT_DETECTED
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
        last_write_reply = await client.write(write_2)
        last_write_reply = skvbc.parse_reply(last_write_reply)
        last_block_id = last_write_reply.last_block_id

        key_prime = skvbc.random_key()

        # this write is conflicting because the writeset (key_prime) is
        # based on an outdated version of the readset (key)
        conflicting_write = skvbc.write_req(
            readset=[key],
            writeset=[(key_prime, skvbc.random_value())],
            block_id=last_block_id - 1)

        reply = await client.write_with_result(conflicting_write, result=bft_msgs.OperationResult.CONFLICT_DETECTED);
        assert reply[1] == bft_msgs.OperationResult.CONFLICT_DETECTED, \
                        f"Expected Reply={bft_msgs.OperationResult.CONFLICT_DETECTED}; actual={reply[1]}"


if __name__ == '__main__':
    unittest.main()