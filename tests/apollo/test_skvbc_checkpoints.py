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
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX, skip_for_tls


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
            "-f", '0' # disable time service until intermittent failures are fixed for this test
            ]


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

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False
        )

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
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False
        )

        checkpoint_after_primary = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        # verify checkpoint creation by the primary after f replicas crash
        self.assertEqual(checkpoint_after_primary, 1 + checkpoint_before_primary)

        bft_network.start_replicas(crashed_replicas)

        # verify checkpoint propagation to all the stale nodes after they come back up
        await bft_network.wait_for_replicas_to_checkpoint(
            crashed_replicas,
            expected_checkpoint_num=lambda ecn: ecn == checkpoint_after_primary)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_propagation_after_crashed_replicas_comeback_after_vc(self, bft_network):
        """
        Here we bring down a total of f - 1 replicas, create checkpoint, trigger
        view change by bringing down the primary (total f replicas down).
        We then verify checkpoint propagation in the new view.
        1) Given a BFT network, we make sure all nodes are up
        2) Crash f - 1 replicas, excluding the initial primary, and next view primary
        3) Send sufficient number of client requests to trigger checkpoint protocol
        4) Make sure checkpoint is created
        5) Crash the initial primary & send a batch of write requests
        6) Verify view change has occurred
        7) Bring the f crashed replicas up
        8) Verify checkpoint propagation to all the nodes
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        self.assertEqual(len(bft_network.procs), n,
                         "Make sure all replicas are up initially.")

        initial_primary = await bft_network.get_current_primary()

        expected_next_primary = initial_primary + 1

        checkpoint_init_primary_before = await bft_network.wait_for_checkpoint(replica_id=initial_primary)

        # crash f-1 replicas, crash initial_primary after checkpoint creation to trigger view change
        crashed_replicas = await self._crash_replicas(
            bft_network=bft_network,
            nb_crashing=f - 1,
            exclude_replicas={expected_next_primary, initial_primary}
        )
        self.assertFalse(initial_primary in crashed_replicas or expected_next_primary in crashed_replicas)

        self.assertGreaterEqual(
            len(bft_network.procs), 2 * f + 2 * c + 1,
            "Make sure enough replicas are up to allow a successful view change!")

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(without=crashed_replicas),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False
        )

        # verify checkpoint creation at the initial primary after f - 1 replicas down
        checkpoint_init_primary_after = await bft_network.wait_for_checkpoint(replica_id=initial_primary)
        self.assertEqual(checkpoint_init_primary_after, 1 + checkpoint_init_primary_before)

        await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == initial_primary,
            err_msg="Make sure we are in the initial view before crashing the primary."
        )

        # trigger a view change by crashing the initial primary and sending a batch of write requests
        bft_network.stop_replica(initial_primary)
        await self._send_random_writes(skvbc)

        crashed_replicas.add(initial_primary)

        # wait for view change
        for replica in bft_network.all_replicas(without=crashed_replicas):
            await bft_network.wait_for_view(
                replica_id=replica,
                expected=lambda v: v == expected_next_primary,
                err_msg="Make sure view change has been triggered."
            )

        # start crashed replicas including the initial primary
        bft_network.start_replicas(crashed_replicas)

        # verify view stabilization among all the stale nodes after they come back up
        for crashed_replica in crashed_replicas:
            await bft_network.wait_for_view(
                replica_id=crashed_replica,
                expected=lambda v: v == expected_next_primary,
                err_msg=f"Make sure view change has been triggered for stale replica: {crashed_replica}."
            )

        # verify checkpoint propagation to all the nodes after they come back up
        await bft_network.wait_for_replicas_to_checkpoint(
            bft_network.all_replicas(),
            expected_checkpoint_num=lambda ecn: ecn >= checkpoint_init_primary_after)

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_propagation_after_f_non_primaries_isolated(self, bft_network):
        """
        Here we isolate f non primary replicas, trigger a checkpoint, as well as verify
        checkpoint creation and propagation to isolated replicas after the adversary is gone.
        1) Given a BFT network, make sure all nodes are up
        2) Isolate f non primary replicas both from other replicas and clients
        3) Send sufficient number of client requests to trigger checkpoint protocol
        4) Make sure checkpoint is propagated to all the nodes
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n
        f = bft_network.config.f

        self.assertEqual(len(bft_network.procs), n,
                         "Make sure all replicas are up initially.")

        current_primary = await bft_network.get_current_primary()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=current_primary)

        isolated_replicas = bft_network.random_set_of_replicas(f, without={current_primary})

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            adversary.interfere()

            # send sufficient number of client requests to trigger checkpoint protocol
            # verify checkpoint creation by all replicas except isolated replicas
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without=isolated_replicas),
                num_of_checkpoints_to_add=1,
                verify_checkpoint_persistency=False
            )

        # Once the adversary is gone, the isolated replicas should be able reach the checkpoint
        await bft_network.wait_for_replicas_to_checkpoint(
            isolated_replicas,
            expected_checkpoint_num=lambda ecn: ecn == checkpoint_before + 1)

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_propagation_after_primary_isolation(self, bft_network):
        """
        Here we isolate primary, verify view change, trigger a checkpoint,
        verify checkpoint creation and propagation in the scenario.
        1) Given a BFT network, make sure all nodes are up
        2) Isolate the primary
        3) Send a batch of write requests to trigger view change, verify view change
        4) Send sufficient number of client requests to trigger checkpoint protocol
        5) Make sure checkpoint is propagated to all the nodes except the isolated primary
           in the new view
        """
        bft_network.start_all_replicas()
        with net.PrimaryIsolatingAdversary(bft_network) as adversary:
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)

            n = bft_network.config.n
            self.assertEqual(len(bft_network.procs), n,
                             "Make sure all replicas are up initially.")

            initial_primary = 0

            await bft_network.wait_for_view(
                replica_id=initial_primary,
                expected=lambda v: v == initial_primary,
                err_msg="Make sure we are in the initial view "
                        "before isolating the primary."
            )

            await adversary.interfere()

            expected_next_primary = initial_primary + 1

            # send a batch of write requests to trigger view change
            await self._send_random_writes(skvbc)

            # verify view change has been triggered for all the nodes except the initial primary
            for replica in bft_network.all_replicas(without={initial_primary}):
                current_view = await bft_network.wait_for_view(
                    replica_id=replica,
                    expected=lambda v: v == expected_next_primary,
                    err_msg="Make sure view change has been triggered."
                )

                self.assertEqual(current_view, expected_next_primary)

            # send sufficient number of client requests to trigger checkpoint protocol
            # verify checkpoint propagation to all the nodes except the the initial primary
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without={initial_primary}),
                num_of_checkpoints_to_add=1,
                verify_checkpoint_persistency=False
            )

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_propagation_after_f_nodes_including_primary_isolated(self, bft_network):
        """
        Here we isolate f replicas including the primary, trigger a view change and
        then a checkpoint. We then verify checkpoint creation and propagation to isolated replicas
        after the adversary is gone.
        1) Given a BFT network, make sure all nodes are up
        2) Isolate f replicas including the primary both from other replicas and clients
        3) Send a batch of write requests to trigger a view change
        4) Send sufficient number of client requests to trigger checkpoint protocol
        5) Make sure checkpoint is propagated to all the nodes in the new view
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        n = bft_network.config.n
        f = bft_network.config.f

        self.assertEqual(len(bft_network.procs), n,
                         "Make sure all replicas are up initially.")

        initial_primary = await bft_network.get_current_primary()
        expected_next_primary = initial_primary + 1

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=initial_primary)

        isolated_replicas = bft_network.random_set_of_replicas(f - 1, without={initial_primary, expected_next_primary})
        isolated_replicas.add(initial_primary)

        with net.ReplicaSubsetIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            adversary.interfere()

            # send a batch of write requests to trigger view change
            await self._send_random_writes(skvbc)

            # verify view change has been triggered for all the non isolated nodes
            for replica in bft_network.all_replicas(without=isolated_replicas):
                current_view = await bft_network.wait_for_view(
                    replica_id=replica,
                    expected=lambda v: v == expected_next_primary,
                    err_msg="Make sure view change has been triggered."
                )

                self.assertEqual(current_view, expected_next_primary)

            # send sufficient number of client requests to trigger checkpoint protocol
            # verify checkpoint creation by all replicas except isolated replicas
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without=isolated_replicas),
                num_of_checkpoints_to_add=1,
                verify_checkpoint_persistency=False
            )

        # Once the adversary is gone, the isolated replicas should be able enter the new view
        for isolated_replica in isolated_replicas:
            current_view = await bft_network.wait_for_view(
                replica_id=isolated_replica,
                expected=lambda v: v == expected_next_primary,
                err_msg="Make sure view change has been triggered."
            )

            self.assertEqual(current_view, expected_next_primary)

        # Once the adversary is gone, the isolated replicas should be able reach the checkpoint
        await bft_network.wait_for_replicas_to_checkpoint(
            isolated_replicas,
            expected_checkpoint_num=lambda ecn: ecn == checkpoint_before + 1)

    @staticmethod
    async def _crash_replicas(bft_network, nb_crashing, exclude_replicas=None):
        crash_replicas = bft_network.random_set_of_replicas(nb_crashing, without=exclude_replicas)

        bft_network.stop_replicas(crash_replicas)

        return crash_replicas

    @staticmethod
    async def _send_random_writes(skvbc):
        try:
            with trio.move_on_after(seconds=1):  # seconds
                await skvbc.send_indefinite_write_requests()
        except trio.TooSlowError:
            pass
