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
import time
import unittest

import trio

from util.consts import EVALUATION_PERIOD_SEQUENCES
from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, ConsensusPathType
from util.skvbc_history_tracker import verify_linearizability
from util import skvbc as kvbc
from util import eliot_logging as log
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from util.skvbc import SimpleKVBCProtocol
    from util.pyclient.bft_client import BftClient
    from util.bft import BftTestNetwork
    from util.skvbc_history_tracker import SkvbcTracker


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
    # There is no need to rotate keys as it is unrelated to path transition and takes time
    ROTATE_KEYS = False

    async def wait_for_stable_state(self, skvbc: 'SimpleKVBCProtocol', timeout_secs: int,
                                    sleep_time: float = 1, client: 'BftClient' = None):
        with trio.fail_after(timeout_secs):
            while True:
                try:
                    await skvbc.send_n_kvs_sequentially(1, client)
                    break
                except:
                    await trio.sleep(sleep_time)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=ROTATE_KEYS)
    @verify_linearizability()
    async def test_fast_path_is_default(self, bft_network: 'BftTestNetwork', tracker: 'SkvbcTracker'):
        """
        This test aims to check that the fast commit path is prevalent
        in the normal, synchronous case (no failed replicas, no network partitioning).

        First we write a series of K/V entries and check that in the process
        we have stayed on the fast path.

        Finally the decorator verifies the KV execution.
        """
        op_count = 5
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        await bft_network.wait_for_consensus_path(path_type=ConsensusPathType.OPTIMISTIC_FAST,
                                                  run_ops=lambda: skvbc.send_n_kvs_sequentially(op_count),
                                                  threshold=op_count)

    @unittest.skip(
        "This is a transition covered in test_commit_path_transitions and is kept as a manual testing option.")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=ROTATE_KEYS)
    @verify_linearizability()
    async def test_fast_to_slow_path(self, bft_network: 'BftTestNetwork', tracker: 'SkvbcTracker'):
        """
        This test aims to check the correct transitions from fast to slow commit path.

        First we write a series of K/V entries making sure we stay on the fast path.

        Once the first series of K/V writes have been processed we bring down C + 1
        replicas (more than what the fast path can tolerate), which should trigger a transition to the slow path.

        We send a new series of K/V writes and make sure they
        have been processed using the slow commit path.

        Finally the decorator verifies the KV execution.
        """
        # Need less than EVALUATION_PERIOD_SEQUENCES messages so that the commit path will not be set to
        # FAST_WITH_THRESHOLD when c>0
        op_count = int(EVALUATION_PERIOD_SEQUENCES * 0.1)
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        # Crash C+1 replicas excluding the primary - this ensures that the slow path will be used
        # without forcing a view change
        crash_targets = random.sample(bft_network.all_replicas(without={0}), bft_network.config.c + 1)
        bft_network.stop_replicas(crash_targets)

        await bft_network.wait_for_consensus_path(path_type=ConsensusPathType.SLOW,
                                                  run_ops=lambda: skvbc.send_n_kvs_sequentially(op_count),
                                                  threshold=op_count)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6 and c > 0, rotate_keys=ROTATE_KEYS)
    @verify_linearizability()
    async def test_fast_to_fast_with_threshold_path(self, bft_network: 'BftTestNetwork', tracker: 'SkvbcTracker'):
        """
        This test check the transitions from fast to fast_with_threshold commit path.
        First we write a series of K/V entries making sure we stay on the fast path.

        Once the first series of K/V writes have been executed we bring down a single replica,
        which should trigger a transition to the fast_with_threshold path.

        We send a new series of K/V writes and make sure they
        have been executed using the fast_with_threshold commit path.
        """

        primary_num = 0
        fast_ops = 5
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        ops_for_transition = int(EVALUATION_PERIOD_SEQUENCES * 1.1)

        # Initially all replicas are running on the fast path
        await bft_network.wait_for_consensus_path(path_type=ConsensusPathType.OPTIMISTIC_FAST,
                                                  run_ops=lambda: skvbc.send_n_kvs_sequentially(fast_ops),
                                                  threshold=fast_ops)

        crash_targets = random.sample(bft_network.all_replicas(without={primary_num}), bft_network.config.c)
        bft_network.stop_replicas(crash_targets)

        await bft_network.wait_for_consensus_path(path_type=ConsensusPathType.FAST_WITH_THRESHOLD,
                                                  run_ops=lambda: skvbc.send_n_kvs_sequentially(ops_for_transition),
                                                  threshold=1)

        slow_commits = await bft_network.commit_count_by_path(ConsensusPathType.SLOW)
        assert slow_commits > 0, f"No {ConsensusPathType.SLOW.value} commits on transition from " \
                                 f"{ConsensusPathType.OPTIMISTIC_FAST.value} to " \
                                 f"{ConsensusPathType.FAST_WITH_THRESHOLD.value}, slow count {slow_commits}"

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=ROTATE_KEYS)
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
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        num_ops = 5

        # Initially all replicas are running on the fast path
        await bft_network.wait_for_consensus_path(
            path_type=ConsensusPathType.OPTIMISTIC_FAST,
            run_ops=lambda: skvbc.send_n_kvs_sequentially(num_ops),
            threshold=num_ops)

        # Crash C+1 replicas excluding the primary - this ensures that the slow path will be used
        # without forcing a view change
        crash_targets = random.sample(bft_network.all_replicas(without={0}), bft_network.config.c + 1)
        bft_network.stop_replicas(crash_targets)

        await bft_network.wait_for_consensus_path(
            path_type=ConsensusPathType.SLOW,
            run_ops=lambda: skvbc.send_n_kvs_sequentially(num_ops),
            threshold=num_ops)

        # Recover crashed replicas and check that the fast path is recovered
        bft_network.start_replicas(crash_targets)

        await bft_network.wait_for_consensus_path(
            path_type=ConsensusPathType.OPTIMISTIC_FAST,
            run_ops=lambda: skvbc.send_n_kvs_sequentially(EVALUATION_PERIOD_SEQUENCES),
            threshold=5)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=ROTATE_KEYS)
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
        num_ops = 5

        await bft_network.wait_for_consensus_path(
            path_type=ConsensusPathType.OPTIMISTIC_FAST,
            run_ops=lambda: skvbc.send_n_kvs_sequentially(num_ops),
            threshold=num_ops)

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

        await self.wait_for_stable_state(skvbc, timeout_secs=10)

        # View change recovers
        await bft_network.wait_for_consensus_path(
            path_type=ConsensusPathType.OPTIMISTIC_FAST,
            run_ops=lambda: skvbc.send_n_kvs_sequentially(int(1.1 * EVALUATION_PERIOD_SEQUENCES)),
            threshold=num_ops)
