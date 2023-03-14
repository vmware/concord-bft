
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

from util.consts import CHECKPOINT_SEQUENCES
from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX, skip_for_tls
from util import bft_network_partitioning as net
from util import eliot_logging as log

from util import skvbc as kvbc

def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", view_change_timeout_milli
            ]


def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper


class SkvbcChaoticStartupTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @unittest.skip("After CheckpointMsg-s forwarding, in this situation the late Replica initiates State Transfer.")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_inactive_window_catchup_up_to_gap(self, bft_network):
        """
        In this test we check the catchup from Inactive Window when we have a gap related to the Peers.
        The situation can happen if the catching up Replica's last Stable SeqNo is 3 Checkpoints behind its Peers, but
        its Last Executed is only 2 Checkpoints behind.
        Steps to recreate:
        1) Start all replicas.
        2) Isolate 1 Replica from all but the Primary. We will call it Late Replica.
        3) Advance all replicas beyond the first Stable Checkpoint. The Late Replica won't be able to collect a
           Stable Checkpoint.
        4) Stop the Late Replica and advance all others 2 more Checkpoints.
        5) Start the late Replica and verify it catches up to the end of its Working Window from the Inactive Windows of
           its Peers.
        """

        late_replica = 1
        primary = 0

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        first_stable_checkpoint_to_reach = 1
        checkpoints_to_advance_after_first = 2
        num_reqs_after_first_checkpoint = 4

        async def write_req(num_req=1):
            for _ in range(num_req):
                await skvbc.send_write_kv_set()

        with net.ReplicaOneWayTwoSubsetsIsolatingAdversary(
                bft_network, {late_replica},
                bft_network.all_replicas(without={primary, late_replica})) as adversary:
            adversary.interfere()

            # create checkpoint and wait for checkpoint propagation
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without={late_replica}),
                num_of_checkpoints_to_add=first_stable_checkpoint_to_reach,
                verify_checkpoint_persistency=False
            )

            await bft_network.wait_for_replicas_to_collect_stable_checkpoint(
                bft_network.all_replicas(without={late_replica}),
                first_stable_checkpoint_to_reach)

            await write_req(num_reqs_after_first_checkpoint)

            # Wait for late_replica to reach num_reqs_after_first_checkpoint past the 1-st Checkpoint
            with trio.fail_after(seconds=30):
                while True:
                    last_exec = await bft_network.get_metric(late_replica, bft_network, 'Gauges', "lastExecutedSeqNum")
                    log.log_message(message_type=f"replica = {late_replica}; lase_exec = {last_exec}")
                    if last_exec == CHECKPOINT_SEQUENCES + num_reqs_after_first_checkpoint:
                        break
                    await trio.sleep(seconds=0.3)

            bft_network.stop_replica(late_replica)

            # create 2 checkpoints and wait for checkpoint propagation
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without={late_replica}),
                num_of_checkpoints_to_add=checkpoints_to_advance_after_first,
                verify_checkpoint_persistency=False
            )

            await bft_network.wait_for_replicas_to_collect_stable_checkpoint(
                bft_network.all_replicas(without={late_replica}),
                first_stable_checkpoint_to_reach + checkpoints_to_advance_after_first)

            bft_network.start_replica(late_replica)
            with trio.fail_after(seconds=30):
            
                late_replica_catch_up = False
                while not late_replica_catch_up:
                    for replica_id in bft_network.get_live_replicas():
                        last_stable = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastStableSeqNum")
                        last_exec = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastExecutedSeqNum")
                        log.log_message(message_type=f"replica = {replica_id}; last_stable = {last_stable}; lase_exec = {last_exec}")
                        if replica_id == late_replica and last_exec == 2*CHECKPOINT_SEQUENCES:
                            late_replica_catch_up = True

                    await write_req()
                    await trio.sleep(seconds=3)

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_inactive_window(self, bft_network):
        """
        The goal of this test is to verify full catch up of a Replica only from the Inactive Window.
        1) Start all Replicas without Replica 1, which will later catch up from the Primary's Inactive Window.
        2) Advance all Replicas to 1 sequence number beyond the first stable and verify they have all collected
           Stable Checkpoints.
        3) Start and isolate the late Replica 1 form all others except the Primary. This way it will not be able
           to start State Transfer and will only be able to catch up from the Primary's Inactive Window.
        4) Verify that Replica 1 has managed to catch up.
        """

        late_replica = 1

        bft_network.start_replicas(bft_network.all_replicas(without={late_replica}))
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        stable_checkpoint_to_reach = 1
        num_reqs_to_catch_up = 151

        async def write_req(num_req=1):
            for _ in range(num_req):
                await skvbc.send_write_kv_set()

        # create checkpoint and wait for checkpoint propagation
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.get_live_replicas(),
            num_of_checkpoints_to_add=stable_checkpoint_to_reach,
            verify_checkpoint_persistency=False
        )

        await bft_network.wait_for_replicas_to_collect_stable_checkpoint(bft_network.get_live_replicas(),
                                                                         stable_checkpoint_to_reach)

        with trio.fail_after(seconds=30):
            with net.ReplicaOneWayTwoSubsetsIsolatingAdversary(bft_network, {1}, {6, 5, 4, 3, 2}) as adversary:
                adversary.interfere()

                bft_network.start_replica(late_replica)

                late_replica_catch_up = False
                while not late_replica_catch_up:
                    for replica_id in bft_network.all_replicas():
                        last_stable = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastStableSeqNum")
                        last_exec = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastExecutedSeqNum")
                        log.log_message(message_type=f"replica = {replica_id}; last_stable = {last_stable}; lase_exec = {last_exec}")
                        if replica_id == late_replica and last_exec >= num_reqs_to_catch_up:
                            late_replica_catch_up = True

                    await write_req()
                    await trio.sleep(seconds=3)

    @unittest.skip("Edge case scenario - not part of CI until intermittent failures are analysed")
    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_view_change_with_f_replicas_collected_stable_checkpoint(self, bft_network):
        """
        The goal of this test is to leave the system with F Replicas that have collected a Stable Checkpoint and to
        cause a View Change. In this way we get a misalignment in the Restrictions of the previous View and we get in an
        indefinite View Change scenario.
        1) Start all Replicas.
        2) Move all Replicas to 1 SeqNo prior to the stable Checkpoint.
        3) Stop Replicas 1 and 2.
        4) Isolate Replica 3 from 6, 5 and 4 only in one direction - 3 will be able to send messages to all, but won't
           receive from 6, 5 and 4. this way 3 won't be able to collect a Stable Checkpoint.
           Do the same for 6, isolating in the same manner from 3, 4 and 5
           Do the same for 4, isolating in the same manner from 3, 5 and 6
           This way only 0 and 5 will collect a Stable Checkpoint for SeqNo 150.
        5) With the isolation scenario, send Client Requests until F replicas collect a Stable Checkpoint.
           Only Replicas 0 and 5 will collect.
        6) We stop Replicas 0, 5 and 6 and start 1 and 2. This way we will cause View Change and we will have only 2
           Replicas with a Stable Checkpoint (5 and 0).
        7) Start Replicas 5 and 0. Within this state the system must be able to finalize a View Change,
           because we have (N - 1) live Replicas, but we have only F that have collected a Stable Checkpoint
           that are live.
        """

        # step 1
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        num_reqs_before_first_stable = 149

        async def write_req(num_req=1):
            for _ in range(num_req):
                await skvbc.send_write_kv_set()

        await write_req(num_reqs_before_first_stable)

        # step 2
        while True:
            last_exec_seqs = []
            for replica_id in bft_network.all_replicas():
                last_stable = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastStableSeqNum")
                last_exec = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastExecutedSeqNum")
                log.log_message(message_type=f"replica = {replica_id}; last_stable = {last_stable};\
                                               last_exec = {last_exec}")
                last_exec_seqs.append(last_exec)
            if sum(x == num_reqs_before_first_stable for x in last_exec_seqs) == bft_network.config.n:
                break
            else:
                last_exec_seqs.clear()

        # step 3
        bft_network.stop_replica(1)
        bft_network.stop_replica(2)

        last_stable_seqs = []

        # step 4
        with net.ReplicaOneWayTwoSubsetsIsolatingAdversary(bft_network, {3}, {6, 5, 4}) as adversary:
            adversary.add_rule({6}, {3, 4, 5})
            adversary.add_rule({4}, {3, 5, 6})
            adversary.interfere()

            while True:
                for replica_id in bft_network.get_live_replicas():
                    last_stable = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastStableSeqNum")
                    last_exec = await bft_network.get_metric(replica_id, bft_network, 'Gauges', "lastExecutedSeqNum")
                    log.log_message(message_type=f"replica = {replica_id}; last_stable = {last_stable};\
                                                   lase_exec = {last_exec}")
                    last_stable_seqs.append(last_stable)
                if sum(x == num_reqs_before_first_stable + 1 for x in last_stable_seqs) == bft_network.config.f:
                    # step 5 completed
                    break
                else:
                    last_stable_seqs.clear()
                    await write_req()
                    await trio.sleep(seconds=3)

            # step 6
            bft_network.stop_replica(0)
            bft_network.stop_replica(6)
            bft_network.stop_replica(5)
            bft_network.start_replica(1)
            bft_network.start_replica(2)

            # Send a Client Request to trigger View Change
            with trio.move_on_after(seconds=3):
                await write_req()

            bft_network.start_replica(5)
            bft_network.start_replica(0)

        # Send a Client Request to trigger View Change
        with trio.move_on_after(seconds=3):
            await write_req()

        # step 7
        await bft_network.wait_for_view(
            replica_id=3,
            expected=lambda v: v == 1,
            err_msg="Make sure a view change happens from 0 to 1"
        )

        await skvbc.wait_for_liveness()


    # @unittest.skipIf(environ.get('BUILD_COMM_TCP_TLS', "").lower() == "true", "Unstable on CI (TCP/TLS only)")
    @unittest.skip("Disabled due to BC-6816")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_missed_two_view_changes(self, bft_network, skvbc, constant_load):
        """
        The purpose of this test is to verify that if a Replica's View is behind the peers by more than 1
        it manages to catch up properly and to join and participate in the View its peers are working in.
        1) Start all replicas and store the current View they are in.
        2) Stop Replica 2 which we will later bring back
        3) Isolate Replica 0 and verify View Change happens
        4) Isolate Replica 1 and verify View Change happens. This time we are going to go to View = 3,
           because we previously stopped Replica 2.
        5) Start Replica 2.
        6) Verify Fast Path of execution is restored.
        """

        late_replica = 2
        connected_replica = 3  # This Replica will always be connected to the peers during the test.
        num_req = 10

        async def write_req():
            for _ in range(num_req):
                await skvbc.send_write_kv_set()

        bft_network.start_all_replicas()
        await write_req()

        current_view = await bft_network.wait_for_view(
            replica_id=0,
            err_msg="Make sure view is stable after all replicas are started."
        )

        bft_network.stop_replica(late_replica)

        for isolated_replica, views_to_advance in [(0, 1), (1, 2)]:
            with net.ReplicaSubsetTwoWayIsolatingAdversary(bft_network, {isolated_replica}) as adversary:
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

                # Wait for View Change initiation to happen
                with trio.fail_after(60):
                    while True:
                        view_of_connected_replica = await self._get_gauge(connected_replica, bft_network, "currentActiveView")
                        if view_of_connected_replica == current_view + views_to_advance:
                            break
                        await trio.sleep(0.2)

            view = await bft_network.wait_for_view(
                replica_id=connected_replica,
                expected=lambda v: v > current_view,
                err_msg=f"Make sure current View is higher than {current_view}"
            )
            current_view = view

        constant_load.cancel()
        bft_network.start_replica(late_replica)

        # Make sure the current view is stable
        await bft_network.wait_for_view(
            replica_id=0,
            expected=lambda v: v == current_view,
            err_msg="Make sure view is stable after all Replicas are connected."
        )

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: write_req(), threshold=num_req)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_delayed_replicas_start_up(self, bft_network, skvbc, constant_load):
        """
        The goal is to make sure that if replicas are started in a random
        order, with delays in-between, and with constant load (request sent every 1s),
        the BFT network eventually stabilizes its view and correctly processes requests
        from this point onwards.
        1) Start sending constant requests, even before the BFT network is started
        2) Create a random replica start order
        3) Use the order defined above to start all replicas with delays in-between
        4) Make sure at least one view change was agreed (because not enough replicas for quorum initially)
        5) Make sure the agreed view is stable after all replicas are started
        6) Make sure the system correctly processes requests in the new view
        """

        replicas_starting_order = bft_network.all_replicas()
        random.shuffle(replicas_starting_order)

        try:
            # Delayed replica start-up...
            for r in replicas_starting_order:
                bft_network.start_replica(r)
                await trio.sleep(seconds=10)

            current_view = await bft_network.get_current_view()

            client = bft_network.random_client()
            current_block = skvbc.parse_reply(
                await client.read(skvbc.get_last_block_req()))

            with trio.move_on_after(seconds=60):
                while True:
                    # Make sure the current view is stable
                    await bft_network.wait_for_view(
                        replica_id=0,
                        expected=lambda v: v == current_view,
                        err_msg="Make sure view is stable after all replicas are started."
                    )
                    await trio.sleep(seconds=5)

                    # Make sure the system is processing requests
                    last_block = skvbc.parse_reply(
                        await client.read(skvbc.get_last_block_req()))
                    self.assertTrue(last_block > current_block)
                    current_block = last_block

            current_primary = current_view % bft_network.config.n
            non_primaries = bft_network.all_replicas(without={current_primary})
            restarted_replica = random.choice(non_primaries)
            log.log_message(message_type=f"Restart replica #{restarted_replica} to make sure its view is persistent...")

            bft_network.stop_replica(restarted_replica)
            await trio.sleep(seconds=5)
            bft_network.start_replica(restarted_replica)

            await trio.sleep(seconds=5)

            # Stop sending requests, and make sure the restarted replica
            # is up-and-running and participates in consensus
            constant_load.cancel()

            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
            last_executed_seq_num = await bft_network.metrics.get(current_primary, *key)

            with trio.fail_after(seconds=30):
                while True:
                    last_executed_seq_num_restarted = await bft_network.metrics.get(restarted_replica, *key)
                    if last_executed_seq_num_restarted >= last_executed_seq_num:
                        break

        except Exception as e:
            log.log_message(message_type=f"Delayed replicas start-up failed for start-up order: {replicas_starting_order}")
            raise e
        else:
            log.log_message(message_type=f"Delayed replicas start-up succeeded for start-up order: {replicas_starting_order}. "
                  f"The BFT network eventually stabilized in view #{current_view}.")

    @unittest.skip("edge scenario - not part of CI")
    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("20000"),
                      selected_configs=lambda n, f, c: f >= 2)
    @with_constant_load
    async def test_f_staggered_replicas_requesting_vc(self, bft_network, skvbc, constant_load):
        """
        The goal of this test is to verify correct behaviour in the situation where
        a subset of F replicas not big enough to reach quorum for execution starts early,
        accepts client requests and initiates view change. The correct behaviour in this
        case will be to for the system to make progress in the slow path after the remaining
        2F+1 replicas are up. After execution is confirmed in the slow path, we stop one
        replica, but not the current or next Primary to verify View Change will happen due
        to inability to reach quorums for execution in the current View. When the new View
        is active we verify the system is able to make progress.

        1) We use a decorator providing constant load of client requests from the start of the test.
        2) Given a BFT network, we start F out of all N replicas.
        3) We wait for them to initiate View Change.
        4) Then we start the remaining 2F+1 replicas.
        5) We verify the system is making progress until a Checkpoint is reached.
        6) Then we verify the early F replicas that initiated View Change catchup by State Transfer.
        7) After this we stop 1 replica, but not the initial or expected next primary.
        8) We verify View Change happens and the system is able to execute requests.
        """

        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_view = initial_primary = 0
        expected_next_view = expected_next_primary = 1

        # take a random set containing F replicas out of all N without the initial primary
        early_replicas = bft_network.random_set_of_replicas(f, without={initial_primary})

        log.log_message(message_type=f"STATUS: Starting F={f} replicas.")
        bft_network.start_replicas(replicas=early_replicas)
        log.log_message(message_type="STATUS: Wait for early replicas to initiate View Change.")
        await self._wait_for_less_than_f_plus_one_replicas_to_initiate_viewchange(early_replicas, bft_network)
        log.log_message(message_type="STATUS: Early replicas initiated View Change.")

        late_replicas = bft_network.all_replicas(without=early_replicas)

        log.log_message(message_type=f"STATUS: Starting the remaining {n-f} replicas.")
        bft_network.start_replicas(late_replicas)

        view = await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == initial_view,
            err_msg="Make sure we are in the initial view "
        )

        self.assertTrue(initial_view == view)

        await self._wait_for_replicas_to_generate_checkpoint(bft_network, skvbc, initial_primary, late_replicas)

        # Verify replicas that have initiated View Change catch up on state
        await bft_network.wait_for_state_transfer_to_start()
        for r in early_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_primary,
                                                              r,
                                                              stop_on_stable_seq_num=True)
        log.log_message(message_type="STATUS: Early replicas that have initiated View Change catch up on state.")

        # Stop one of the later started replicas, but not the initial Primary
        # in order to verify that View Change will happen due to inability to
        # reach quorums in the slow path. Also don't stop next primary, because
        # in this test we check a single view change.
        replica_to_stop = random.choice(bft_network.all_replicas(without=early_replicas | {initial_primary,
                                                                                           expected_next_primary}))
        log.log_message(message_type=f'STATUS: Stopping one of the later replicas with ID={replica_to_stop}')
        bft_network.stop_replica(replica_to_stop)

        # Wait for View Change to happen.
        view = await bft_network.wait_for_view(
            replica_id=expected_next_primary,
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure we are in the next view "
        )

        self.assertTrue(expected_next_view == view)
        await self._wait_for_processing_window_after_view_change(expected_next_primary, bft_network)

        await self._verify_replicas_are_in_view(view, bft_network.all_replicas(without={replica_to_stop}), bft_network)

        await self._wait_for_replicas_to_generate_checkpoint(bft_network, skvbc, expected_next_primary, bft_network.all_replicas(without={replica_to_stop}))

    @unittest.skip("edge scenario - not part of CI")
    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("20000"),
                      selected_configs=lambda n, f, c: f >= 2)
    @with_constant_load
    async def test_f_minus_one_staggered_replicas_requesting_vc(self, bft_network, skvbc, constant_load):
        """
        In this test we check that if f-1 replicas have started to run and initiated viewchange, then the system is
        still able to make progress. To make sure the system still have 2f+1 active replicas, we wait for the early
        replicas to complete their state transfer before stopping the primary.

        For that we perform the following steps:
        1. start f - 1 replicas
        2. wait for those replicas to initiate view change
        3. start the rest 2f + 2 replicas
        4. make sure the system is able to make progress
        5. stop the current primary
        6. wait for the system to complete a view change
        7. make sure the system is able to make progress
        """
        n = bft_network.config.n
        f = bft_network.config.f
        c = bft_network.config.c

        initial_view = initial_primary = 0
        expected_next_view = expected_next_primary = 1

        # We start by choosing a random set of f - 1 replicas.
        excluded = {initial_primary}
        early_replicas = bft_network.random_set_of_replicas(f - 1, without=excluded)
        log.log_message(message_type="STATUS: Early replicas are: ")
        log.log_message(message_type=early_replicas)
        log.log_message(message_type=f"STATUS: Starting F={f - 1} replicas.")
        bft_network.start_replicas(replicas=early_replicas)
        await self._wait_for_less_than_f_plus_one_replicas_to_initiate_viewchange(early_replicas, bft_network)
        log.log_message(message_type="STATUS: Early replicas started and initiated View Change.")


        late_replicas = bft_network.all_replicas(without=early_replicas)
        log.log_message(message_type=f"STATUS: Starting the remaining {n - f + 1} replicas.")
        bft_network.start_replicas(late_replicas)

        view = await bft_network.wait_for_view(
            replica_id=initial_primary,
            expected=lambda v: v == initial_view,
            err_msg="Make sure we are in the initial view "
        )
        self.assertTrue(initial_view == view)

        await self._wait_for_replicas_to_generate_checkpoint(bft_network, skvbc, initial_primary, late_replicas)

        # Verify replicas that have initiated View Change catch up on state
        await bft_network.wait_for_state_transfer_to_start()
        for r in early_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_primary,
                                                                  r,
                                                                  stop_on_stable_seq_num=True)
        log.log_message(message_type="STATUS: Early replicas that have initiated View Change catch up on state.")

        # We stop the current primary and let the system to install a new view, while assuming the identity of
        # the next primary
        bft_network.stop_replica(initial_primary)

        # We wait enough time to be sure that the earliest background client request is timed out such that the replicas
        # will initiate view change before apollo's metric client will time out
        view_change_timer = await self._get_gauge(expected_next_primary, bft_network, "viewChangeTimer")
        await trio.sleep(seconds= view_change_timer / 1000)

        view = await bft_network.wait_for_view(
            replica_id=expected_next_primary,
            expected=lambda v: v == expected_next_view,
            err_msg="Make sure we are in the next view "
        )

        self.assertTrue(expected_next_view == view)
        await self._wait_for_processing_window_after_view_change(expected_next_primary, bft_network)

        await self._verify_replicas_are_in_view(view, bft_network.all_replicas(without={initial_primary}), bft_network)

        await self._wait_for_replicas_to_generate_checkpoint(bft_network, skvbc, expected_next_primary, bft_network.all_replicas(without={initial_primary}))

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("20000"),
                      selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def stuck_view_change_bug_recreation(self, bft_network, skvbc, constant_load):
        """
        Test inspired by failure of a subset of replicas to enter a New View after View Change
        due to insufficient ViewChange messages and previous no resend of ViewChange messages
        on Status requests. To recreate the following steps are executed:
        1) Start all 7 replicas.
        2) Setup an adversary that blocks all incoming msgs to replicas 2, 3, 4, 5 and 6 from other replicas, but not
           from clients.
        3) In this setup the system cannot execute client requests, so all replicas will initiate View Change.
        4) Only replicas 0 and 1 will have sufficient View Change msgs to enter View 1.
        5) Replica 1 will send New View message to all.
        6) Drop the Adversary and verify that Replicas that didn't have enough View Change msgs manage to
           gather them and correctly enter the New View -> 1
        """

        initial_view = initial_primary = 0
        expected_next_view = expected_next_primary = 1

        with net.ReplicaSubsetOneWayIsolatingAdversary(
                bft_network,
                bft_network.all_replicas(without={initial_primary, expected_next_primary})
        ) as adversary:
            adversary.interfere()

            bft_network.start_all_replicas()

            # await replicas to initiate View Change from 0 to 1
            for r in bft_network.all_replicas():
                view_of_replica = 0
                while view_of_replica == 0:
                    view_of_replica = await self._get_gauge(r, bft_network, 'view')
                    await trio.sleep(seconds=0.1)

        # Wait for View Change to happen.
        view = await bft_network.wait_for_view(
            replica_id=expected_next_primary,
            expected=lambda v: v == expected_next_view,
            err_msg="Make sure we are in the next view "
        )

        self.assertTrue(expected_next_view == view)

    async def _verify_replicas_are_in_view(self, view, replicas, bft_network):
        for r in replicas:
            active_view_of_replica = await self._get_gauge(r, bft_network, 'currentActiveView')
            view_of_replica = await self._get_gauge(r, bft_network, 'view')
            self.assertTrue(active_view_of_replica == view_of_replica)  # verify Replica is not requesting View Change
            self.assertTrue(active_view_of_replica == view)  # verify Replica is in the current view

    async def _wait_for_replicas_to_generate_checkpoint(self, bft_network, skvbc, replica_to_read_from, initial_nodes, checkpoint_num=1, verify_checkpoint_persistency=False, assert_state_transfer_not_started=False):
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=replica_to_read_from)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=initial_nodes,
            num_of_checkpoints_to_add=checkpoint_num,
            verify_checkpoint_persistency=verify_checkpoint_persistency,
            assert_state_transfer_not_started=assert_state_transfer_not_started
        )
        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=replica_to_read_from)
        # Verify the system is able to make progress
        self.assertTrue(checkpoint_after > checkpoint_before)

    async def _wait_for_less_than_f_plus_one_replicas_to_initiate_viewchange(self, replicas, bft_network):
        num_of_replicas = len(replicas)
        self.assertTrue(num_of_replicas <= bft_network.config.f)
        for r in replicas:
            active_view_of_replica = 0
            view_of_replica = 0
            while active_view_of_replica == view_of_replica:
                active_view_of_replica = await self._get_gauge(r, bft_network, 'currentActiveView')
                view_of_replica = await self._get_gauge(r, bft_network, 'view')
                await trio.sleep(seconds=0.1)

    async def _wait_for_processing_window_after_view_change(self, primary_id, bft_network):
        with trio.fail_after(seconds=20):
            last_exec_seq_num = await self._get_gauge(primary_id, bft_network, "lastExecutedSeqNum")
            conc_level = await self._get_gauge(primary_id, bft_network, "concurrencyLevel")
            prim_last_used_seq_num = await self._get_gauge(primary_id, bft_network, "primaryLastUsedSeqNum")
            while prim_last_used_seq_num >= last_exec_seq_num + conc_level:
                await trio.sleep(seconds=1)
                last_exec_seq_num = await self._get_gauge(primary_id, bft_network, "lastExecutedSeqNum")
                conc_level = await self._get_gauge(primary_id, bft_network, "concurrencyLevel")

    @classmethod
    async def _get_gauge(cls, replica_id, bft_network, gauge):
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
