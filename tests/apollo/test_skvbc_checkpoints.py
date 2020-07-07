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

import trio

from util import bft_network_partitioning as net

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
        5) Bring the f replicas up
        6) Make sure checkpoint is agreed upon by all the crashed replicas
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

        crashed_replicas = await self._crash_replicas(
            bft_network=bft_network,
            nb_crashing=f,
            exclude_replicas={current_primary}
        )
        self.assertFalse(current_primary in crashed_replicas)

        self.assertGreaterEqual(
            len(bft_network.procs), 2 * f + c + 1,
            "Make sure enough replicas are up to allow a successful checkpoint creation")

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without=crashed_replicas),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )

        checkpoint_after_primary = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        # verify checkpoint creation by the primary after f replicas crash
        self.assertEqual(checkpoint_after_primary, 1 + checkpoint_before_primary)

        bft_network.start_replicas(crashed_replicas)

        # verify checkpoint propagation to all the stale nodes after they come back up
        for crashed_replica in crashed_replicas:
            checkpoint_stale = await bft_network.wait_for_checkpoint(
                replica_id=crashed_replica,
                expected_checkpoint_num=checkpoint_after_primary)

            self.assertEqual(checkpoint_stale, checkpoint_after_primary)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_propagation_amid_dropping_packets(self, bft_network):
        """
        Here the adversary randomly drops 5% of packets between all replicas,
        we then trigger a checkpoint, verify checkpoint creation and propagation.
        1) Given a BFT network, make sure all nodes are up
        2) Introduce the packet dropping adversary
        3) Send sufficient number of client requests to trigger checkpoint protocol
        4) Make sure checkpoint is propagated to all the nodes
        """
        with net.PacketDroppingAdversary(bft_network, drop_rate_percentage=5) as adversary:
            bft_network.start_all_replicas()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)

            n = bft_network.config.n
            self.assertEqual(len(bft_network.procs), n,
                             "Make sure all replicas are up initially.")

            current_primary = await bft_network.get_current_primary()

            checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=current_primary)

            adversary.interfere()

            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(),
                checkpoint_num=1,
                verify_checkpoint_persistency=False
            )

            # verify checkpoint propagation to all the nodes
            for replica in bft_network.all_replicas():
                checkpoint_after = await bft_network.wait_for_checkpoint(
                    replica_id=replica,
                    expected_checkpoint_num=checkpoint_before + 1)

                self.assertEqual(checkpoint_after, checkpoint_before + 1)

    @staticmethod
    async def _crash_replicas(bft_network, nb_crashing, exclude_replicas=None):
        crash_replicas = bft_network.random_set_of_replicas(nb_crashing, without=exclude_replicas)

        bft_network.stop_replicas(crash_replicas)

        return crash_replicas
