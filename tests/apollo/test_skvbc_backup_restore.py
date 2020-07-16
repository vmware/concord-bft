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

import trio

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).

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
            "-p",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcBackupRestoreTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_checkpoint_propagation_after_restarting_replicas(self, bft_network):
        """
        Here we trigger a checkpoint, restart all replicas in a random order with 10s delay in-between,
        both while stopping and starting. We verify checkpoint persisted upon restart and then trigger
        another checkpoint. We make sure checkpoint is propagated to all the replicas.
        1) Given a BFT network, we make sure all nodes are up
        2) Send sufficient number of client requests to trigger checkpoint protocol
        3) Stop all replicas in a random order (with 10s delay in between)
        4) Start all replicas in a random order (with 10s delay in between)
        5) Make sure the initial view is stable
        6) Send sufficient number of client requests to trigger another checkpoint
        7) Make sure checkpoint propagates to all the replicas
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n

        self.assertEqual(len(bft_network.procs), n, "Make sure all replicas are up initially.")

        current_primary = await bft_network.get_current_primary()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )

        # stop n replicas in a random order with a delay of 10s in between
        stopped_replicas = await self._stop_random_replicas_with_delay(bft_network, n)

        # start stopped replicas in a random order with a delay of 10s in between
        await self._start_random_replicas_with_delay(bft_network, stopped_replicas)

        # verify checkpoint persistence
        await bft_network.wait_for_replicas_to_checkpoint(stopped_replicas, checkpoint_before + 1)

        # verify current view is stable
        for replica in bft_network.all_replicas():
            await bft_network.wait_for_view(
                replica_id=replica,
                expected=lambda v: v == current_primary,
                err_msg="Make sure view is stable after all replicas started."
            )

        # create second checkpoint and wait for checkpoint propagation
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )

    @staticmethod
    async def _stop_random_replicas_with_delay(bft_network, replica_set_size, delay=10, exclude_replicas=None):
        random_replicas = bft_network.random_set_of_replicas(size=replica_set_size, without=exclude_replicas)
        for replica in random_replicas:
            bft_network.stop_replica(replica)
            await trio.sleep(delay)
        return list(random_replicas)

    @staticmethod
    async def _start_random_replicas_with_delay(bft_network, stopped_replicas, delay=10):
        random.shuffle(stopped_replicas)
        for replica in stopped_replicas:
            bft_network.start_replica(replica)
            await trio.sleep(delay)
