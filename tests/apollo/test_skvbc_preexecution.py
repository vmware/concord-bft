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
import unittest

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util import skvbc as kvbc


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).

    Note each arguments is an element in a list.
    """

    status_timer_milli = "500"
    view_change_timeout_milli = "10000"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            "-v", view_change_timeout_milli,
            "-p"
            ]


class SkvbcPreExecutionTest(unittest.TestCase):

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs = lambda n, f, c: n == 7)
    async def test_single_pre_process_request(self, bft_network):
        """
        Ensure that we can create a block using pre-execution feature and retrieve its KV pairs through
        GetBlockData API request.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        kv = [(skvbc.keys[0], skvbc.random_value()),
              (skvbc.keys[1], skvbc.random_value())]
        reply = await client.write(skvbc.write_req([], kv, 0), pre_process=True)
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)
