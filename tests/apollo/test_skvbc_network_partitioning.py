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

from util import bft_network_partitioning as net

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability


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


class SkvbcNetworkPartitioningTest(unittest.TestCase):

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_while_dropping_packets(self, bft_network, tracker):
        """
         Run a bunch of concurrent requests in batches and verify
         linearizability, while dropping a small amount of packets
         between all replicas.
         """
        num_ops = 500

        with net.PacketDroppingAdversary(
                bft_network, drop_rate_percentage=5) as adversary:
            self.skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            self.bft_network = bft_network
            bft_network.start_all_replicas()

            adversary.interfere()

            await tracker.run_concurrent_ops(num_ops)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability
    async def test_single_vc_primary_isolated(self, bft_network, tracker):
        """
        The goal of this test is to check the view change
        workflow in case the primary is up, but its outgoing
        communication is intercepted by an adversary.

        1) Given a BFT network,
        2) Insert an adversary that isolates the primary's outgoing communication
        3) Send a batch of write requests.
        4) Verify the BFT network eventually transitions to the next view.
        5) Perform a "read-your-writes" check in the new view
        """
        with net.PrimaryIsolatingAdversary(bft_network) as adversary:
            bft_network.start_all_replicas()

            initial_primary = 0
            await bft_network.wait_for_view(
                replica_id=initial_primary,
                expected=lambda v: v == initial_primary,
                err_msg="Make sure we are in the initial view "
                        "before isolating the primary."
            )

            await adversary.interfere()
            expected_next_primary = 1

            await self._send_random_writes(tracker)

            await bft_network.wait_for_view(
                replica_id=random.choice(bft_network.all_replicas(without={0})),
                expected=lambda v: v == expected_next_primary,
                err_msg="Make sure view change has been triggered."
            )

            await self._wait_for_read_your_writes_success(tracker)

            await tracker.run_concurrent_ops(100)

    async def _wait_for_read_your_writes_success(self, tracker):
        with trio.fail_after(seconds=60):
            while True:
                with trio.move_on_after(seconds=5):
                    try:
                        await tracker.tracked_read_your_writes()
                    except Exception:
                        continue
                    else:
                        break

    async def _send_random_writes(self, tracker):
        with trio.move_on_after(seconds=1):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.send_indefinite_tracked_ops, 1)
