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

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability
from util import eliot_logging as log
from util import skvbc as kvbc


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
            "-e", str(True),
            "-v", viewChangeTimeoutMilli
            ]


class SkvbcSlowPathTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    def setUp(self):
        # Whenever a replica goes down, all messages initially go via the slow path.
        # However, when an "evaluation period" elapses (set at 64 sequence numbers),
        # the system should return to the fast path.
        self.evaluation_period_seq_num = 64

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: c == 0 and n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_persistent_slow_path(self, bft_network, tracker):
        """
        Start a full BFT network with c=0 then bring one replica down.

        Write a batch of K/V entries and track them using the tracker from the decorator.

        Then we check that messages from now on are processed on the slow commit path.
        (note that this is not the case for c>0 where the BFT network eventually
        returns to the fast commit path)

        Finally, we check if a these entries were executed and readable.
        """

        bft_network.start_all_replicas()

        unstable_replicas = bft_network.all_replicas(without={0})
        bft_network.stop_replica(
            replica_id=random.choice(unstable_replicas))
        await bft_network.wait_for_slow_path_to_be_prevalent(
            run_ops=lambda: tracker.skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd,
                      num_clients=4, selected_configs=lambda n, f, c: n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_slow_to_fast_path_transition(self, bft_network, tracker,exchange_keys=True):
        """
        This test aims to check that the system correctly restores
        the fast path once all failed nodes are back online.

        First we bring down a non-primary replica, and make sure
        a batch of K/V entries is processed on the slow path while we track those entries
        using the tracker from the decorator.

        Once the first batch of K/V writes have been processed, we bring the
        failed replica back up, which should restore the system's ability to
        process requests via the fast commit path.

        We send a new batch of K/V writes and make sure they
        have been processed using the fast path.

        Finally we check if a known K/V has been executed and readable.
        """

        run_ops = lambda: tracker.skvbc.run_concurrent_ops(num_ops=20, write_weight=1)
        bft_network.start_all_replicas()

        unstable_replicas = bft_network.all_replicas(without={0})
        crashed_replica = random.choice(unstable_replicas)
        bft_network.stop_replica(crashed_replica)

        await bft_network.wait_for_slow_path_to_be_prevalent(run_ops=run_ops, threshold=20)

        bft_network.start_replica(crashed_replica)

        await bft_network.wait_for_fast_path_to_be_prevalent(run_ops=run_ops, threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_slow_path_view_change(self, bft_network, tracker):
        """
        This test validates the BFT engine's transition to the slow path
        when the primary goes down. This effectively triggers a view change in the slow path.

        First we write a batch of K/V entries and track them using the tracker from the decorator.

        We check those entries have been processed via the fast commit path.

        We stop the primary and send a indefinite batch of tracked read & write requests,
        triggering slow path & view change.

        We bring the primary back up.

        We make sure the second batch of requests have been processed via the slow path.
        """
        bft_network.start_all_replicas()

        num_ops = 5
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: tracker.skvbc.run_concurrent_ops(num_ops=num_ops, write_weight=1), threshold=num_ops)

        bft_network.stop_replica(0)

        # trigger the view change
        await tracker.skvbc.run_concurrent_ops(num_ops)

        randRep = random.choice(
                bft_network.all_replicas(without={0}))

        log.log_message(f'wait_for_view - Random replica {randRep}')

        await bft_network.wait_for_view(
            replica_id=randRep,
            expected=lambda v: v > 0,
            err_msg="Make sure view change has occurred."
        )

        nb_fast_paths_to_ignore = await bft_network.num_of_fast_path_requests(randRep)
        nb_slow_paths_to_ignore = await bft_network.num_of_slow_path_requests(randRep)

        with trio.move_on_after(seconds=5):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.skvbc.send_indefinite_ops, 1)

        bft_network.start_replica(0)

        await bft_network.assert_slow_path_prevalent(
            nb_fast_paths_to_ignore=nb_fast_paths_to_ignore,
            nb_slow_paths_to_ignore=nb_slow_paths_to_ignore,
            replica_id=randRep)
