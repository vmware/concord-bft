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

import trio

import unittest

from util import bft
from util import skvbc as kvbc
from util import bft_network_partitioning as net

KEY_FILE_PREFIX = "replica_keys_"

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "3000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli]

class SkvbcViewChangeTest(unittest.TestCase):

    def test_single_vc_only_primary_down(self):
        """
        The goal of this test is to validate the most basic view change
        scenario - a single view change when the primary replica is down.

        1) Given a BFT network, we trigger parallel writes.
        2) Make sure the initial view is preserved during those writes.
        3) Stop the primary replica and send a batch of write requests.
        4) Verify the BFT network eventually transitions to the next view.
        """
        trio.run(self._test_single_vc_only_primary_down)

    async def _test_single_vc_only_primary_down(self):
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                initial_primary = 0
                expected_next_primary = 1

                await self._send_random_writes(skvbc)

                await bft_network.wait_for_view_change(
                    replica_id=initial_primary,
                    expected=lambda v: v == initial_primary,
                    err_msg="Make sure we are in the initial view "
                            "before crashing the primary."
                )

                bft_network.stop_replica(initial_primary)

                await self._send_random_writes(skvbc)

                await bft_network.wait_for_view_change(
                    replica_id=random.choice(bft_network.all_replicas(without={0})),
                    expected=lambda v: v == expected_next_primary,
                    err_msg="Make sure view change has been triggered."
                )

    def test_single_vc_primary_isolated(self):
        """
        The goal of this test is to check the view change
        workflow in case the primary is up, but its outgoing
        communication is intercepted by an adversary.

        1) Given a BFT network,
        2) Insert an adversary that isolates the primary's outgoing communication
        3) Send a batch of write requests.
        4) Verify the BFT network eventually transitions to the next view.
        """
        trio.run(self._test_single_vc_primary_isolated)

    async def _test_single_vc_primary_isolated(self):
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network, \
                    net.PrimaryIsolatingAdversary(bft_network) as adversary:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                initial_primary = 0
                await bft_network.wait_for_view_change(
                    replica_id=initial_primary,
                    expected=lambda v: v == initial_primary,
                    err_msg="Make sure we are in the initial view "
                            "before isolating the primary."
                )

                await adversary.interfere()
                expected_next_primary = 1

                await self._send_random_writes(skvbc)

                await bft_network.wait_for_view_change(
                    replica_id=random.choice(bft_network.all_replicas(without={0})),
                    expected=lambda v: v == expected_next_primary,
                    err_msg="Make sure view change has been triggered."
                )

    def test_single_vc_with_f_replicas_down(self):
        """
        Here we "step it up" a little bit, bringing down a total of f replicas
        (including the primary), verifying a single view change in this scenario.

        1) Given a BFT network, we make sure all nodes are up.
        2) crash f replicas, including the current primary.
        3) Trigger parallel requests to start the view change.
        4) Verify the BFT network eventually transitions to the next view.
        """
        trio.run(self._test_single_vc_with_f_replicas_down)

    async def _test_single_vc_with_f_replicas_down(self):
        for bft_config in bft.interesting_configs():
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                self.assertEqual(len(bft_network.procs), config.n,
                                 "Make sure all replicas are up initially.")
                initial_primary = 0
                expected_next_primary = 1

                crashed_replicas = await self._crash_replicas_including_primary(
                    bft_network=bft_network,
                    nb_crashing=config.f,
                    primary=initial_primary
                )

                self.assertGreaterEqual(
                    len(bft_network.procs),
                    2 * config.f + 2 * config.c + 1,
                    "Make sure enough replicas are up to allow a successful view change")

                await self._send_random_writes(skvbc)

                await bft_network.wait_for_view_change(
                    replica_id=random.choice(bft_network.all_replicas(without=crashed_replicas)),
                    expected=lambda v: v == expected_next_primary,
                    err_msg="Make sure view change has been triggered."
                )

    def test_multiple_vc_slow_path(self):
        """
        In this test we aim to validate a sequence of view changes,
        maintaining the slow commit path. To do so, we need to crash
        at least c+1 replicas, including the primary.

        1) Given a BFT network, we make sure all nodes are up.
        2) Repeat the following several times:
            2.1) Crash c+1 replicas (including the primary)
            2.2) Send parallel requests to start the view change.
            2.3) Verify the BFT network eventually transitions to the next view.
        3) Make sure the slow path was prevalent during all view changes
        """
        trio.run(self._test_multiple_vc_slow_path)

    async def _test_multiple_vc_slow_path(self):
        # Here, we require that c < f because:
        # A) for view change we need at least n-f = 2f+2c+1 replicas
        # B) to ensure transition to the slow path, we need to crash at least c+1 replicas.
        # Combining A) and B) yields n-(c+1) >= 2f+2c+1, equivalent to c < f
        for bft_config in bft.interesting_configs(lambda n, f, c: c < f):
            config = bft.TestConfig(n=bft_config['n'],
                                    f=bft_config['f'],
                                    c=bft_config['c'],
                                    num_clients=bft_config['num_clients'],
                                    key_file_prefix=KEY_FILE_PREFIX,
                                    start_replica_cmd=start_replica_cmd)
            with bft.BftTestNetwork(config) as bft_network:
                await bft_network.init()
                bft_network.start_all_replicas()
                skvbc = kvbc.SimpleKVBCProtocol(bft_network)

                current_primary = 0
                for _ in range(3):
                    self.assertEqual(len(bft_network.procs), config.n,
                                     "Make sure all replicas are up initially.")

                    crashed_replicas = await self._crash_replicas_including_primary(
                        bft_network=bft_network,
                        nb_crashing=config.c+1,
                        primary=current_primary
                    )

                    self.assertGreaterEqual(
                        len(bft_network.procs),
                        2 * config.f + 2 * config.c + 1,
                        "Make sure enough replicas are up to allow a successful view change")

                    await self._send_random_writes(skvbc)

                    stable_replica = random.choice(
                        bft_network.all_replicas(without=crashed_replicas))

                    view = await bft_network.wait_for_view_change(
                        replica_id=stable_replica,
                        expected=lambda v: v > current_primary,
                        err_msg="Make sure a view change has been triggered."
                    )
                    current_primary = view
                    [bft_network.start_replica(i) for i in crashed_replicas]

                await bft_network.wait_for_slow_path_to_be_prevalent(
                    replica_id=current_primary)

    async def _send_random_writes(self, skvbc):
        with trio.move_on_after(seconds=1):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(skvbc.send_indefinite_write_requests)

    async def _crash_replicas_including_primary(
            self, bft_network, nb_crashing, primary):
        crashed_replicas = set()

        bft_network.stop_replica(primary)
        crashed_replicas.add(primary)

        crash_candidates = bft_network.all_replicas(without={primary})
        random.shuffle(crash_candidates)
        for i in range(nb_crashing - 1):
            bft_network.stop_replica(crash_candidates[i])
            crashed_replicas.add(crash_candidates[i])

        return crashed_replicas
