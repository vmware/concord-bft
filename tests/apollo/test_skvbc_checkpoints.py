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

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_propagation_after_crashed_replicas_comeback(self, bft_network):
        """
        Here we bring down a total of f replicas, verify checkpoint creation and
        propagation in that scenario.

        1) Given a BFT network, we make sure all nodes are up
        2) Crash f replicas, excluding the current primary
        3) Send sufficient number of client requests to trigger checkpoint protocol
        4) Make sure checkpoint is created
        4) Bring the f replicas up
        5) Make sure checkpoint is agreed upon by the crashed replicas
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        self.assertEqual(len(bft_network.procs), n,
                         "Make sure all replicas are up initially.")

        current_primary = await bft_network.get_current_primary()

        checkpoint_before_primary = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        crashed_replicas = await self._crash_replicas_without_primary(
            bft_network=bft_network,
            nb_crashing=f
        )
        self.assertFalse(current_primary in crashed_replicas)

        self.assertGreaterEqual(
            len(bft_network.procs), 2 * f + 2 * c + 1,
            "Make sure enough replicas are up to allow a successful checkpoint creation")

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without=crashed_replicas),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )

        checkpoint_after_primary = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        # verify checkpoint creation after f replicas crash
        self.assertEqual(checkpoint_after_primary, 1 + checkpoint_before_primary)

        await self._bring_up_crashed_replicas(bft_network, crashed_replicas)

        stale_node = random.choice(list(crashed_replicas))

        checkpoint_after_stale = await bft_network.wait_for_checkpoint(
            replica_id=stale_node,
            expected_checkpoint_num=checkpoint_after_primary)

        # verify checkpoint propagation to stale node after it comes back up
        self.assertEqual(checkpoint_after_stale, checkpoint_after_primary)

    async def _crash_replicas_without_primary(
            self, bft_network, nb_crashing):
        current_primary = await bft_network.get_current_primary()

        crashed_replicas = set()

        crash_candidates = bft_network.all_replicas(
            without={current_primary})
        random.shuffle(crash_candidates)
        for i in range(nb_crashing):
            bft_network.stop_replica(crash_candidates[i])
            crashed_replicas.add(crash_candidates[i])

        return crashed_replicas

    async def _bring_up_crashed_replicas(
            self, bft_network, crashed_replicas):

        for replica in crashed_replicas:
            bft_network.start_replica(replica)
