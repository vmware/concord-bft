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
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX


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
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcCheckpointTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_creation(self, bft_network):
        """
        Test the creation of checkpoints (independently of state transfer or view change)

        Start all replicas, then send a sufficient number of client requests to trigger the
        checkpoint protocol. Then make sure a checkpoint is created and agreed upon by all replicas.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )
        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=0)

        self.assertEqual(checkpoint_after, 1 + checkpoint_before)
