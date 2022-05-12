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

from util.test_base import ApolloTest
from util import bft_network_partitioning as net
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, skip_for_tls
from util.skvbc_history_tracker import verify_linearizability
from util import eliot_logging as log

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
            "-e", str(True)
            ]


class SkvbcViewChangeTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_request_block_not_written_primary_down(self, bft_network):
        """
        The goal of this test is to validate that a block wasn't written,
        if the primary fell down.

        1) Given a BFT network, we trigger one write.
        2) Make sure the initial view is preserved during this write.
        3) Stop the primary replica and send a batch of write requests.
        4) Verify the BFT network eventually transitions to the next view.
        5) Validate that there is no new block written.
        """

        bft_network.start_all_replicas()

        client = bft_network.random_client()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        initial_primary = 0
        expected_next_primary = 1

        key_before_vc, value_before_vc = await skvbc.send_write_kv_set()

        await skvbc.assert_kv_write_executed(key_before_vc, value_before_vc)

        await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == initial_primary,
            err_msg="Make sure we are in the initial view "
                    "before crashing the primary."
        )

        last_block = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))

        bft_network.stop_replica(initial_primary)
        try:
            with trio.move_on_after(seconds=1):  # seconds
                await skvbc.send_indefinite_write_requests()

        except trio.TooSlowError:
            pass
        finally:

            await bft_network.wait_for_view(
                replica_id=random.choice(bft_network.all_replicas(without={0})),
                expected=lambda v: v == expected_next_primary,
                err_msg="Make sure view change has been triggered."
            )

            new_last_block = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))
            self.assertEqual(new_last_block, last_block)



    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    @verify_linearizability()
    async def test_single_vc_only_primary_down(self, bft_network, tracker,exchange_keys=True):
        """
        The goal of this test is to validate the most basic view change
        scenario - a single view change when the primary replica is down.

        1) Given a BFT network, we trigger parallel writes.
        2) Make sure the initial view is preserved during those writes.
        3) Stop the primary replica and send a batch of write requests.
        4) Verify the BFT network eventually transitions to the next view.
        5) Perform a "read-your-writes" check in the new view
        """

        await self._single_vc_with_consecutive_failed_replicas(
            bft_network,
            tracker,
            num_consecutive_failing_primaries=1
        )

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    @verify_linearizability()
    async def test_single_vc_with_f_replicas_down(self, bft_network, tracker):
        """
        Here we "step it up" a little bit, bringing down a total of f replicas
        (including the primary), verifying a single view change in this scenario.

        1) Given a BFT network, we make sure all nodes are up.
        2) crash f replicas, including the current primary.
        3) Trigger parallel requests to start the view change.
        4) Verify the BFT network eventually transitions to the next view.
        5) Perform a "read-your-writes" check in the new view
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        self.assertEqual(len(bft_network.procs), n,
                         "Make sure all replicas are up initially.")
        initial_primary = 0
        expected_next_primary = 1

        crashed_replicas = await self._crash_replicas_including_primary(
            bft_network=bft_network,
            nb_crashing=f,
            primary=initial_primary,
            except_replicas={expected_next_primary}
        )
        self.assertFalse(expected_next_primary in crashed_replicas)

        self.assertGreaterEqual(
            len(bft_network.procs), 2 * f + 2 * c + 1,
            "Make sure enough replicas are up to allow a successful view change")

        await self._send_random_writes(skvbc)

        await bft_network.wait_for_view(
            replica_id=random.choice(bft_network.all_replicas(without=crashed_replicas)),
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure view change has been triggered."
        )

        await self._wait_for_read_your_writes_success(skvbc)

        await skvbc.run_concurrent_ops(10)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=True)
    @verify_linearizability()
    async def test_crashed_replica_catch_up_after_view_change(self, bft_network, tracker):
        """
        In this test scenario we want to make sure that a crashed replica
        that missed a view change will catch up and start working within
        the new view once it is restarted:
        1) Start all replicas
        2) Send a batch of concurrent reads/writes, to make sure the initial view is stable
        3) Choose a random non-primary and crash it
        4) Crash the current primary & trigger view change
        5) Make sure the new view is agreed & activated among all live replicas
        6) Start the crashed replica
        7) Make sure the crashed replica works in the new view

        Note: this scenario requires f >= 2, because at certain moments we have
        two simultaneously crashed replicas (the primary and the non-primary that is
        missing the view change).
        """

        bft_network.start_all_replicas()

        initial_primary = 0
        expected_next_primary = 1
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.run_concurrent_ops(num_ops=10)

        unstable_replica = random.choice(
            bft_network.all_replicas(without={initial_primary, expected_next_primary}))

        await bft_network.wait_for_view(
            replica_id=unstable_replica,
            expected=lambda v: v == initial_primary,
            err_msg="Make sure the unstable replica works in the initial view."
        )

        log.log_message(message_type=f"Crash replica #{unstable_replica} before the view change.")
        bft_network.stop_replica(unstable_replica)

        # trigger a view change
        bft_network.stop_replica(initial_primary)
        await self._send_random_writes(skvbc)

        await bft_network.wait_for_view(
            replica_id=random.choice(bft_network.all_replicas(
                without={initial_primary, unstable_replica})),
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure view change has been triggered."
        )

        # waiting for the active window to be rebuilt after the view change
        await trio.sleep(seconds=5)

        # restart the unstable replica and make sure it works in the new view
        bft_network.start_replica(unstable_replica)
        await skvbc.run_concurrent_ops(num_ops=10)


        await bft_network.wait_for_view(
            replica_id=unstable_replica,
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure the unstable replica works in the new view."
        )

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=True)
    @verify_linearizability()
    async def test_restart_replica_after_view_change(self, bft_network, tracker):
        """
        This test makes sure that a replica can be safely restarted after a view change:
        1) Start all replicas
        2) Send a batch of concurrent reads/writes, to make sure the initial view is stable
        3) Crash the current primary & trigger view change
        4) Make sure the new view is agreed & activated among all live replicas
        5) Choose a random non-primary and restart it
        6) Send a batch of concurrent reads/writes
        7) Make sure the restarted replica is alive and that it works in the new view
        """
        bft_network.start_all_replicas()
        initial_primary = 0
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.run_concurrent_ops(num_ops=10)

        bft_network.stop_replica(initial_primary)
        await self._send_random_writes(skvbc)

        await bft_network.wait_for_view(
            replica_id=random.choice(
                bft_network.all_replicas(without={initial_primary})),
            expected=lambda v: v == initial_primary + 1,
            err_msg="Make sure a view change is triggered."
        )
        current_primary = initial_primary + 1

        bft_network.start_replica(initial_primary)

        # waiting for the active window to be rebuilt after the view change
        await trio.sleep(seconds=5)

        unstable_replica = random.choice(
            bft_network.all_replicas(without={current_primary, initial_primary}))
        log.log_message(message_type=f"Restart replica #{unstable_replica} after the view change.")

        bft_network.stop_replica(unstable_replica)
        bft_network.start_replica(unstable_replica)
        await trio.sleep(seconds=5)

        await skvbc.run_concurrent_ops(num_ops=10)

        await bft_network.wait_for_view(
            replica_id=unstable_replica,
            expected=lambda v: v == current_primary,
            err_msg="Make sure the unstable replica works in the new view."
        )

        await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == current_primary,
            err_msg="Make sure the initial primary activates the new view."
        )

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=True)
    @verify_linearizability()
    async def test_synchronize_replica_with_higher_view(self, bft_network, tracker):
        """
        In this test we aim to validate that once restarted, a crashed replica will catch up
        to the view that the other replicas are in after multiple
        view changes were done while the replica was offline.

        1) Start all replicas
        2) Send a batch of concurrent reads/writes, to make sure the initial view is stable
        3) Choose a random non-primary and crash it
        4) Crash the current primary & trigger view change
        5) Make sure the new view is agreed & activated among all live replicas
        6) Start the previous primary replica and make sure that it works in the new view
        7) Crash the new primary & trigger view change again
        8) Start the crashed replica
        9) Make sure the crashed replica works in the latest view

        Note: this scenario requires f >= 2, because at certain moments we have
        two simultaneously crashed replicas (the primary and the non-primary that is
        missing the view change).

        """

        bft_network.start_all_replicas()

        initial_primary = 0
        expected_next_primary = 1
        expected_last_primary = 2

        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.run_concurrent_ops(num_ops=10)

        unstable_replica = random.choice(
            bft_network.all_replicas(without={initial_primary, expected_next_primary, expected_last_primary}))

        await bft_network.wait_for_view(
            replica_id=unstable_replica,
            expected=lambda v: v == initial_primary,
            err_msg="Make sure the unstable replica works in the initial view."
        )

        log.log_message(message_type=f"Crash replica #{unstable_replica} before the view change.")
        bft_network.stop_replica(unstable_replica)

        # trigger a view change
        bft_network.stop_replica(initial_primary)
        await skvbc.run_concurrent_ops(num_ops=10)

        await bft_network.wait_for_view(
            replica_id=random.choice(bft_network.all_replicas(
                without={initial_primary, unstable_replica})),
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure view change has been triggered."
        )

        # waiting for the active window to be rebuilt after the view change
        await trio.sleep(seconds=5)

        # restart the initial primary replica and make sure it works in the new view
        bft_network.start_replica(initial_primary)
        await skvbc.run_concurrent_ops(num_ops=10)

        await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure the initial primary replica works in the new view."
        )

        # waiting for the active window in the initial primary to be rebuilt after the view change
        await trio.sleep(seconds=5)

        # crash the new primary to trigger a view change
        bft_network.stop_replica(expected_next_primary)
        await skvbc.run_concurrent_ops(num_ops=10)

        await bft_network.wait_for_view(
            replica_id=random.choice(bft_network.all_replicas(
                without={expected_next_primary, unstable_replica})),
            expected=lambda v: v == expected_last_primary,
            err_msg="Make sure view change has been triggered."
        )

        # waiting for the active window to be rebuilt after the view change
        await trio.sleep(seconds=5)

        # restart the unstable replica and make sure it works in the latest view
        bft_network.start_replica(unstable_replica)
        await skvbc.run_concurrent_ops(num_ops=10)

        await bft_network.wait_for_view(
            replica_id=unstable_replica,
            expected=lambda v: v == expected_last_primary,
            err_msg="Make sure the unstable replica works in the latest view."
        )

    @unittest.skip("unstable scenario")
    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: c < f)
    @verify_linearizability()
    async def test_multiple_vc_slow_path(self, bft_network, tracker):
        """
        In this test we aim to validate a sequence of view changes,
        maintaining the slow commit path. To do so, we need to crash
        at least c+1 replicas, including the primary.

        1) Given a BFT network, we make sure all nodes are up.
        2) Repeat the following several times:
            2.1) Crash c+1 replicas (including the primary)
            2.2) Send parallel requests to start the view change.
            2.3) Verify the BFT network eventually transitions to the next view.
            2.4) Perform a "read-your-writes" check in the new view
        3) Make sure the slow path was prevalent during all view changes

        Note: we require that c < f because:
        A) for view change we need at least n-f = 2f+2c+1 replicas
        B) to ensure transition to the slow path, we need to crash at least c+1 replicas.
        Combining A) and B) yields n-(c+1) >= 2f+2c+1, equivalent to c < f
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        current_primary = 0
        for _ in range(2):
            self.assertEqual(len(bft_network.procs), n,
                             "Make sure all replicas are up initially.")

            expected_next_primary = current_primary + 1
            crashed_replicas = await self._crash_replicas_including_primary(
                bft_network=bft_network,
                nb_crashing=c+1,
                primary=current_primary,
                except_replicas={expected_next_primary}
            )
            self.assertFalse(expected_next_primary in crashed_replicas)

            self.assertGreaterEqual(
                len(bft_network.procs), 2 * f + 2 * c + 1,
                "Make sure enough replicas are up to allow a successful view change")

            await self._send_random_writes(skvbc)

            stable_replica = random.choice(
                bft_network.all_replicas(without=crashed_replicas))

            view = await bft_network.wait_for_view(
                replica_id=stable_replica,
                expected=lambda v: v >= expected_next_primary,
                err_msg="Make sure a view change has been triggered."
            )
            current_primary = view
            [bft_network.start_replica(i) for i in crashed_replicas]

        await skvbc.read_your_writes()

        await bft_network.wait_for_view(
            replica_id=current_primary,
            err_msg="Make sure all ongoing view changes have completed."
        )

        await skvbc.read_your_writes()

        #check after test is fixed
        await bft_network.assert_slow_path_prevalent()

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs = lambda n,f,c : f >= 2, rotate_keys=True)
    @verify_linearizability()
    async def test_single_vc_current_and_next_primaries_down(self, bft_network, tracker):
        """
        The goal of this test is to validate the skip view scenario, where
        both the primary and the expected next primary have failed. In this
        case the first view change does not happen, causing timers in the
        replicas to expire and a view change to v+2 is initiated.

        1) Given a BFT network, we trigger parallel writes.
        2) Make sure the initial view is preserved during those writes.
        3) Stop the primary and next primary replica and send a batch of write requests.
        4) Verify the BFT network eventually transitions to the expected view (v+2).
        5) Perform a "read-your-writes" check in the new view.
        6) We have to filter only configurations which support more than 2 faulty replicas.
        """

        await self._single_vc_with_consecutive_failed_replicas(
            bft_network,
            tracker,
            num_consecutive_failing_primaries = 2
        )

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_replica_asks_to_leave_view(self, bft_network):
        """
        This test makes sure that a single isolated replica will ask for a view change, but it doesn't occur:
        1) Start all replicas
        2) Isolate a single replica for long enough to trigger a ReplicaAsksToLeaveViewMsg
        3) Let the replica rejoin the consensus
        4) Make sure a view change does not happen and the isolated replica
        rejoins the fast path in the existing view
        """

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        n = bft_network.config.n

        self.assertEqual(len(bft_network.procs), n,
                         "Make sure all replicas are up initially.")

        initial_primary = await bft_network.get_current_primary()

        isolated_node = random.choice(
            bft_network.all_replicas(without={initial_primary}))
        isolated_replicas = set([isolated_node])

        ask_to_leave_msg_count = await bft_network.get_metric(
            isolated_node, bft_network, "Gauges", "sentReplicaAsksToLeaveViewMsg")
        self.assertEqual(ask_to_leave_msg_count, 0,
                         "Make sure the replica is ok before isolating it.")

        with net.ReplicaSubsetTwoWayIsolatingAdversary(bft_network, isolated_replicas) as adversary:
            adversary.interfere()
            try:
                client = bft_network.random_client()
                client.primary = None
                for _ in range(5):
                    msg = skvbc.write_req(
                        [], [(skvbc.random_key(), skvbc.random_value())], 0)
                    await client.write(msg)
            except:
                pass

            await bft_network.wait_for_replica_to_ask_for_view_change(replica_id=isolated_node)

        # ensure the isolated replica has sent AsksToLeaveView requests after ending it's isolation
        await self._wait_for_replica_to_ask_to_leave_view_due_to_status(
            bft_network=bft_network, node=isolated_node)

        num_fast_req = 10
        async def write_req():
            for _ in range(num_fast_req):
                await skvbc.send_write_kv_set()

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: write_req(), threshold=num_fast_req)

        current_primary = await bft_network.get_current_primary()
        self.assertEqual(initial_primary, current_primary,
                         "Make sure we are still on the initial view.")

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @verify_linearizability()
    async def test_recovering_of_replicas_with_initiated_view_change(self, bft_network, tracker):
        """
        The goal of this test is to validate fast path's recovery after stopping all replicas during an ongoing view change.
        The test is valid only when c = 0.
        Initially N - (F + 1) replicas are started. In order to initiate a view change (because of 3f + 2 * 0 + 1 - f - 1 = 2f),
        the clients run concurrent operations, which trigger the view change, however it will not be completed (because of 2f < 2f + 1).

        1) Given a BFT network start N - (F + 1) replicas.
        2) Send some requests to trigger view change from replicas
        4) Wait for view change to be initiated
        5) Stop all previously running replicas
        6) Start all of the replicas
        7) Wait for fast path to be prevalent
        """

        replicas_to_start = bft_network.config.n - (bft_network.config.f + 1)
        replicas = random.sample(bft_network.all_replicas(), replicas_to_start)

        # start replicas
        [bft_network.start_replica(i) for i in replicas]
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        # write to trigger vc
        await skvbc.run_concurrent_ops(10)

        # wait for replicas to go to higher view (View 1 in this case)
        await bft_network.wait_for_replicas_to_reach_view(replicas, 1)

        # stop replicas
        [bft_network.stop_replica(i) for i in replicas]

        bft_network.start_all_replicas()

        # wait for replicas to go to higher view after the restart (View 1 in this case)
        await bft_network.wait_for_replicas_to_reach_view(bft_network.all_replicas(), 1)

        await bft_network.wait_for_view(
            replica_id=random.choice(
            bft_network.all_replicas()),
            expected=lambda v: v == 1,
            err_msg="Make sure view change has happened"
        )

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @verify_linearizability()
    async def test_recovering_of_replicas_with_failed_quorum_view_change(self, bft_network, tracker):
        """
        We need to verify the system correctly handles the case where we have F+1 replicas unavailable (stopped).
         In this scenario the system will temporarily lose liveness, but we need to verify that once the stopped replicas are up again the system will recover and process client requests again.

        1. Starts all replicas.
        2. Send Client requests to verify system is working correctly.
        3. Stop F+1 Replicas excluding the primary.
        4. Send additional client requests that won’t be processed due to lack of quorum,
         but will trigger the live replicas to move to a higher view.
        5. Verify the 2F remaining replicas move to the next view.
         Note - we cannot use bft_network.wait_for_view since it waits for a quorum (2F+1)
          of replicas to enter the view and we will only have 2F.
           We can use something similar to what we have in Get Replica View.
            There is also the more generic getter that was added later metric getter.
        6. Start the previously stopped replicas.
        7. Verify the system recovers and all replicas enter the new view (here we can use bft_network.wait_for_view).
        """

        # start replicas
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        # write to trigger vc
        await skvbc.run_concurrent_ops(10)

        initial_primary = await bft_network.get_current_primary()
        expected_view = 0
        await bft_network.wait_for_replicas_to_reach_view(bft_network.all_replicas(), expected_view)

        to_stop = random.sample(
            bft_network.all_replicas(without={initial_primary}), bft_network.config.f + 1)

        # stop replicas
        [bft_network.stop_replica(i) for i in to_stop]

        # write to trigger vc
        await skvbc.run_concurrent_ops(10)

        running_replicas = []
        for i in bft_network.all_replicas():
            is_in_to_stop = False
            for j in to_stop:
                if (i == j):
                    is_in_to_stop = True
                    break
            if (not is_in_to_stop):
                running_replicas.append(i)

        for r in running_replicas:
            expected_next_view  = 0
            while expected_next_view  == 0:
                expected_next_view  = await self._get_gauge(r, bft_network, 'view')
                await trio.sleep(seconds=0.1)
            self.assertEqual(expected_next_view , 1, "Replica failed to reach expected view")

        [bft_network.start_replica(i) for i in to_stop]

        expected_view = expected_view + 1

         # write to trigger vc
        await skvbc.run_concurrent_ops(10)

        # wait for replicas to go to higher view (View 1 in this case)
        await bft_network.wait_for_replicas_to_reach_view(bft_network.all_replicas(), expected_view)

    async def _single_vc_with_consecutive_failed_replicas(
            self,
            bft_network,
            tracker,
            num_consecutive_failing_primaries):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        initial_primary = await bft_network.get_current_primary()
        initial_view = await bft_network.get_current_view()
        replcas_to_stop = [ v for v in range(initial_primary,
                                             initial_primary + num_consecutive_failing_primaries) ]

        expected_final_view = initial_view + num_consecutive_failing_primaries

        await self._send_random_writes(skvbc)

        await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == initial_view,
            err_msg="Make sure we are in the initial view "
                    "before crashing the primary."
        )

        for replica in replcas_to_stop:
            bft_network.stop_replica(replica)

        await self._send_random_writes(skvbc)

        await bft_network.wait_for_view(
            replica_id=random.choice(bft_network.all_replicas(without=set(replcas_to_stop))),
            expected=lambda v: v == expected_final_view,
            err_msg="Make sure view change has been triggered."
        )

        await self._wait_for_read_your_writes_success(skvbc)

        await skvbc.run_concurrent_ops(10)

    async def _wait_for_read_your_writes_success(self, skvbc):
        with trio.fail_after(seconds=60):
            while True:
                with trio.move_on_after(seconds=5):
                    try:
                        await skvbc.read_your_writes()
                    except Exception:
                        continue
                    else:
                        break

    async def _send_random_writes(self, skvbc, retry_for_seconds=5):
        """
        Try to send random writes for `retry_for_seconds` in total. Do that via
        nested `move_on_after()` calls, because the BFT client that sends the tracked
        ops will terminate the test if it times out. Therefore, make sure we never let
        it timeout by calling it for 1 second at a time and assuming its timeout is more
        than 1 second.

        Above is useful, because if replicas haven't started yet and we give the BFT
        client 1 second to send a request, some replicas might not even observe it and
        never trigger view change. Doing that in a loop for `retry_for_seconds` makes
        the race condition less likely.

        TODO: We might want to rewrite the tests so that they are not dependent on timing.
        """
        with trio.move_on_after(retry_for_seconds):
            while True:
                with trio.move_on_after(seconds=1):
                    async with trio.open_nursery() as nursery:
                        nursery.start_soon(skvbc.send_indefinite_ops, 1)

    async def _crash_replicas_including_primary(
            self, bft_network, nb_crashing, primary, except_replicas=None):
        if except_replicas is None:
            except_replicas = set()

        crashed_replicas = set()

        bft_network.stop_replica(primary)
        crashed_replicas.add(primary)

        crash_candidates = bft_network.all_replicas(
            without=except_replicas.union({primary}))
        random.shuffle(crash_candidates)
        for i in range(nb_crashing - 1):
            bft_network.stop_replica(crash_candidates[i])
            crashed_replicas.add(crash_candidates[i])

        return crashed_replicas

    async def _wait_for_replica_to_ask_to_leave_view_due_to_status(self, bft_network, node):
        with trio.fail_after(seconds=30):
            while True:
                ask_to_leave_msg_count = await bft_network.get_metric(
                    node, bft_network, "Counters", "sentReplicaAsksToLeaveViewMsgDueToStatus")
                if ask_to_leave_msg_count > 0:
                    break
                else:
                    await trio.sleep(.5)

    async def _get_gauge(self, replica_id, bft_network, gauge):
        with trio.fail_after(seconds=30):
            while True:
                with trio.move_on_after(seconds=1):
                    try:
                        key = ['replica', 'Gauges', gauge]
                        value = await bft_network.metrics.get(replica_id, *key)
                    except KeyError:
                        # metrics not yet available, continue looping
                        log.log_message(message_type=f"KeyError! '{gauge}' not yet available.")
                    else:
                        return value
