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

from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc
from util import eliot_logging as log

NUM_OPS = 20

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    view_change_timeout_milli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", view_change_timeout_milli,
            "-e", str(True)
            ]

class SkvbcCommitPathTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_fast_path_is_default(self, bft_network, tracker, exchange_keys=True):
        """
        This test aims to check that the fast commit path is prevalent
        in the normal, synchronous case (no failed replicas, no network partitioning).

        First we write a series of K/V entries and check that in the process
        we have stayed on the fast path.

        Finally the decorator verifies the KV execution.
        """

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)

        # Initially all replicas are running on the fast path
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)


    @unittest.skip("This is a transition covered in test_commit_path_transitions and is kept as a manual testing option.")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_fast_to_slow_path(self, bft_network, tracker):
        """
        This test aims to check the correct transitions from fast to slow commit path.

        First we write a series of K/V entries making sure we stay on the fast path.

        Once the first series of K/V writes have been processed we bring down C + 1
        replicas (more than what the fast path can tolerate), which should trigger a transition to the slow path.

        We send a new series of K/V writes and make sure they
        have been processed using the slow commit path.

        Finally the decorator verifies the KV execution.
        """

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)

        # Initially all replicas are running on the fast path
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

        # Crash C+1 replicas excluding the primary - this ensures that the slow path will be used
        # without forcing a view change
        crash_targets = random.sample(bft_network.all_replicas(without={0}), bft_network.config.c + 1)
        bft_network.stop_replicas(crash_targets)

        await bft_network.wait_for_slow_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_commit_path_transitions(self, bft_network, tracker):
        """
        This test aims to check the correct transitions from fast to slow and back to fast commit path.

        First we write a series of K/V entries making sure we stay on the fast path.

        Once the first series of K/V writes have been processed we bring down C + 1
        replicas (more than what the fast path can tolerate), which should trigger a transition to the slow path.

        We send a new series of K/V writes and make sure they
        have been processed using the slow commit path.

        Then we recover the crashed nodes and expect the fast path to be eventually restored.

        Finally the decorator verifies the KV execution.
        """

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)

        # Initially all replicas are running on the fast path
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

        # Crash C+1 replicas excluding the primary - this ensures that the slow path will be used
        # without forcing a view change
        crash_targets = random.sample(bft_network.all_replicas(without={0}), bft_network.config.c + 1)
        bft_network.stop_replicas(crash_targets)

        await bft_network.wait_for_slow_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

        # Recover crashed replicas and check that the fast path is recovered
        bft_network.start_replicas(crash_targets)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

    @with_trio
    @with_bft_network(start_replica_cmd,
                      num_clients=4,
                      selected_configs=lambda n, f, c: c >= 1 and n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_fast_path_resilience_to_crashes(self, bft_network, tracker):
        """
        In this test we check the fast path's resilience when "c" nodes fail.

        We write a series of K/V entries making sure the fast path is prevalent despite the crashes.

        Finally the decorator verifies the KV execution.
        """

        bft_network.start_all_replicas()

        crash_targets = random.sample(bft_network.all_replicas(without={0}), bft_network.config.c)
        bft_network.stop_replicas(crash_targets)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)


    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_fast_path_after_view_change(self, bft_network, tracker):
        """
        This test validates the BFT engine's ability to restore the fast path
        after a view change due to crashed primary.

        First we write a batch of K/V entries and check those entries have been processed via the fast commit path.

        We stop the primary and send a single write requests to trigger a view change.

        We bring the primary back up.

        We make sure the fast path is eventually maintained.

        Finally the decorator verifies the KV execution.
        """

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        # Initially all replicas are running on the fast path
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

        # Stop the primary
        bft_network.stop_replica(0)

        # Send a write request to trigger a view change
        with trio.move_on_after(seconds=3):
            await skvbc.send_write_kv_set()

        randRep = random.choice(
                bft_network.all_replicas(without={0}))

        log.log_message(f'wait_for_view - Random replica {randRep}')

        await bft_network.wait_for_view(
            replica_id=randRep,
            expected=lambda v: v > 0,
            err_msg="Make sure view change has occurred."
        )

        # Restore the crashed primary
        bft_network.start_replica(0)

        # Make sure that the fast path is maintained eventually
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=NUM_OPS, write_weight=1), threshold=NUM_OPS)

