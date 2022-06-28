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
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX, skip_for_tls
from util.skvbc_history_tracker import verify_linearizability
from util import eliot_logging as log

viewChangeTimeoutSec = 5

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    global viewChangeTimeoutSec
    statusTimerMilli = "100"
    viewChangeTimeoutMilli = "{}".format(viewChangeTimeoutSec*1000)
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-e", str(True)
            ]

class foo:
    def log_message(self, var):
        print(f"{var}")

class SkvbcRestartRecoveryTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @staticmethod
    def _advance_current_next_primary(new_view, num_replicas):
        _current_primary = new_view % num_replicas
        _next_primary = (_current_primary + 1) % num_replicas
        return _current_primary, _next_primary

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_restarting_replica_with_client_load(self, bft_network):
        """
        The goal of this test is to restart a replica multiple times while the system is processing
        Client Operations to verify the restarted replica recovery and the system's return to
        fast path of processing client requests.
        Scenario:
        1) For 1 minute send client operations.
        2) While sending client operations we restart multiple times 1 randomly selected Replica
           (not the Primary).
        3) After every restart we verify that the system will eventually return to the Fast Path.
        """

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        primary_replica = 0

        # Pick one replica to restart multiple times while the system is processing client requests
        replica_to_restart = random.choice(
            bft_network.all_replicas(without={primary_replica}))

        # uncomment for live tracking of log messages from the test
        # log = foo()

        for v in range(300):
            async with trio.open_nursery() as nursery:
                # Start the sending of client operations in the background.
                nursery.start_soon(skvbc.send_indefinite_ops)
                while True:
                    log.log_message(f"Stop replica {replica_to_restart} and wait for system to move to slow path")
                    bft_network.stop_replica(replica_to_restart, True)
                    latest_slow_paths = total_slow_paths = await bft_network.num_of_slow_path_requests(primary_replica)
                    with trio.fail_after(seconds=15):
                        while latest_slow_paths - total_slow_paths == 0:
                            await trio.sleep(seconds=0.1)
                            latest_slow_paths = await bft_network.num_of_slow_path_requests(primary_replica)
                    log.log_message(f"Start replica {replica_to_restart} and wait for system to move to fast path")
                    bft_network.start_replica(replica_to_restart)
                    latest_fast_paths = total_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    with trio.fail_after(seconds=15):
                        while latest_fast_paths - total_fast_paths == 0:
                            await trio.sleep(seconds=0.1)
                            latest_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)

        # Before the test ends we verify the Fast Path is prevalent,
        # no matter the restarts we performed on the selected replica.
        log.log_message("wait for fast path to be prevalent")
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)
        log.log_message("fast path prevailed")

    async def _restarting_replica_during_system_is_in_view_change(self, bft_network, restart_next_primary):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        current_primary = 0
        next_primary = 1
        view = 0

        # Perform multiple view changes and restart 1 replica while the replicas are agreeing the new View
        while view < 100:
            # Pick one replica to restart while the others are agreeing the next View.
            # We want a replica other than the current primary, which will be restarted to trigger the view change
            # and we want also the restarted replica to be different from the next primary
            excluded_replcias = {current_primary, next_primary}
            if restart_next_primary:
                excluded_replcias.add(next_primary + 1 % bft_network.config.n)
            replica_to_restart = random.choice(
                bft_network.all_replicas(without=excluded_replcias))
            log.log_message(f"Initiating View Change by stopping replica {current_primary} in view {view}")
            # Stop the current primary.
            bft_network.stop_replica(current_primary, True)
            if restart_next_primary:
                # Stop the next primary.
                log.log_message(f"Stopping the next primary: replica {next_primary} in view {view}")
                bft_network.stop_replica(next_primary, True)
            # Run client operations to trigger view Change
            await skvbc.run_concurrent_ops(5)

            wait_period = viewChangeTimeoutSec
            # If we restart the next primary, wait for the second view increment to happen in replicas.
            if restart_next_primary:
                wait_period *= 2
            log.log_message(f"waiting for {wait_period}s")
            # Wait for replicas to start initiating ReplicaAsksToLeaveView msgs followed by ViewChange msgs.
            await trio.sleep(seconds=wait_period)
            log.log_message(f"Restarting replica during View Change {replica_to_restart}")
            # Restart the previously selected replica while the others are performing the view change.
            bft_network.stop_replica(replica_to_restart, True)
            bft_network.start_replica(replica_to_restart)
            await trio.sleep(seconds=1)
            # Starting previously stopped replicas.
            log.log_message(f"Starting replica {current_primary}")
            bft_network.start_replica(current_primary)
            if restart_next_primary:
                log.log_message(f"Starting replica {next_primary}")
                bft_network.start_replica(next_primary)

            old_view = view

            # Wait for quorum of replicas to move to a higher view
            with trio.fail_after(seconds=40):
                while view == old_view:
                    log.log_message(f"waiting for vc current view={view}")
                    await skvbc.run_concurrent_ops(1)
                    with trio.move_on_after(seconds=5):
                        view = await bft_network.get_current_view()
                    await trio.sleep(seconds=1)

            # Update the values of the current_primary and next_primary according to the new view the system is in
            current_primary, next_primary = self._advance_current_next_primary(view, bft_network.config.n)

            log.log_message(f"view is {view}")

            log.log_message("Checking for replicas in state transfer:")
            # Check for replicas in state transfer and wait for them to catch
            # up before moving on to checking the system returns on Fast Path
            await self._await_replicas_in_state_transfer(log, bft_network, skvbc, current_primary)

            # After all replicas have caught up, check if there has been
            # additional view change while state transfer was completed
            view_after_st = await bft_network.get_current_view()
            log.log_message(f"view_after_st={view_after_st}")
            if view != view_after_st:
                log.log_message(f"during ST we got a View Change from view {view} to {view_after_st}")
                view = view_after_st
                # Update the values of the current_primary and next_primary according to the new view the system is in
                current_primary, next_primary = self._advance_current_next_primary(view, bft_network.config.n)

            log.log_message("wait for fast path to be prevalent")
            # Make sure fast path is prevalent before moving to another round ot the test
            await bft_network.wait_for_fast_path_to_be_prevalent(
                run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=1)
            log.log_message("fast path prevailed")

        # Before the test ends we verify the Fast Path is prevalent, no matter
        # the restarts we performed while the system was in view change.
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=False)
    async def test_restarting_replica_during_system_is_in_view_change(self, bft_network):
        """
        The goal of this test is trigger multiple View Changes and restart 1 non-next Primary replica
        in order to test restart recovery during the replicas performing a View Change.
        """

        await self._restarting_replica_during_system_is_in_view_change(bft_network, False)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=False)
    async def test_restarting_replica_during_system_is_in_view_change_next_primary_down(self, bft_network):
        """
        The goal of this test is trigger multiple View Changes with the current and next Primaries down and restart 1
        replica in order to test restart recovery during the replicas performing a View Change with
        multiple view increments.
        """

        await self._restarting_replica_during_system_is_in_view_change(bft_network, True)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=False)
    async def test_restarting_f_replicas_for_view_change(self, bft_network):
        """
        The goal of this test is to restart F replicas including the current Primary multiple times
        in order to verify that the system is able to recover correctly - agree on a view and process
        once again client requests on Fast Path.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        current_primary = 0
        next_primary = 1
        view = 0

        # uncomment for live tracking of log messages from the test
        # log = foo()

        # Perform multiple view changes by restarting F replicas where the Primary is included
        while view < 100:
            # Pick F-1 replicas to be restarted excluding the next primary.
            # We will unconditionally insert the current primary in this
            # sequence to trigger the view change
            excluded_replcias = {current_primary, next_primary}
            replica_to_restart = random.sample(
                bft_network.all_replicas(without=excluded_replcias), bft_network.config.f-1)
            replica_to_restart = [current_primary] + replica_to_restart
            log.log_message(f"Initiating View Change by stopping replicas {replica_to_restart} in view {view}")

            for replica in replica_to_restart:
                log.log_message(f"Stopping replica {replica} in view {view}")
                bft_network.stop_replica(replica, True)

            await skvbc.run_concurrent_ops(10)

            wait_period = viewChangeTimeoutSec
            log.log_message(f"waiting for {wait_period}s")
            # Wait for replicas to start initiating ReplicaAsksToLeaveView msgs followed by ViewChange msgs.
            await trio.sleep(seconds=wait_period)
            # Starting previously stopped replicas.
            for replica in replica_to_restart:
                log.log_message(f"Starting replica {replica}")
                bft_network.start_replica(replica)

            old_view = view

            # Wait for quorum of replicas to move to a higher view
            with trio.fail_after(seconds=40):
                while view == old_view:
                    log.log_message(f"waiting for vc current view={view}")
                    await skvbc.run_concurrent_ops(1)
                    with trio.move_on_after(seconds=5):
                        view = await bft_network.get_current_view()
                    await trio.sleep(seconds=1)

            # Update the values of the current_primary and next_primary according to the new view the system is in
            current_primary, next_primary = self._advance_current_next_primary(view, bft_network.config.n)

            log.log_message(f"view is {view}")

            log.log_message("Checking for replicas in state transfer:")
            # Check for replicas in state transfer and wait for them to catch
            # up before moving on to checking the system returns on Fast Path
            await trio.sleep(seconds=1)
            await self._await_replicas_in_state_transfer(log, bft_network, skvbc, current_primary)

            # After all replicas have caught up, check if there has been
            # additional view change while state transfer was completed
            view_after_st = await bft_network.get_current_view()
            log.log_message(f"view_after_st={view_after_st}")
            if view != view_after_st:
                log.log_message(f"during ST we got a View Change from view {view} to {view_after_st}")
                view = view_after_st
                # Update the values of the current_primary and next_primary according to the new view the system is in
                current_primary, next_primary = self._advance_current_next_primary(view, bft_network.config.n)

            log.log_message("wait for fast path to be prevalent")
            # Make sure fast path is prevalent before moving to another round ot the test
            await bft_network.wait_for_fast_path_to_be_prevalent(
                run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=1)
            log.log_message("fast path prevailed")

        # Before the test ends we verify the Fast Path is prevalent, no matter
        # the restarts we performed while the system was in view change.
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @with_constant_load
    async def test_recovering_fast_path(self, bft_network, skvbc, constant_load):
        """
        The Apollo test, which should be part of the test_skvbc_restart_recovery suite needs to implement the following steps:

        1. Start all Replicas and Introduce constantly running Client requests in the background.
        2. Verify that the system is processing client requests on Fast Path.
        3. Restart the Primary to initiate View Change.
        4. Verify the View Change succeeded.
        5. Wait for the system to recover Fast commit path again (Here we will have to set a time limit for which we expect the system to recover to Fast path. We can start with 1 minute interval.)
        6. Stop 1 Non Primary replica to transition the system to Slow path.
        7. Verify the system is making progress on Slow path.
        8. Restart the Primary to initiate View Change.
        9. Start the Replica we stopped in step 6 and verify the View Change succeeded.
        10. Goto Step 2.
        """
        # start replicas
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]

        # log = foo()

        loop_count = 0
        while (loop_count < 100):
            loop_count = loop_count + 1

            view = await bft_network.get_current_view()

            primary = await bft_network.get_current_primary()
            bft_network.stop_replica(primary)
            await trio.sleep(seconds=10)
            bft_network.start_replica(primary)

            await bft_network.wait_for_replicas_to_reach_at_least_view(replicas_ids=bft_network.all_replicas(), expected_view=view + 1, timeout=60)

            await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20, timeout=180)

            view = await bft_network.get_current_view()

            non_primary = primary
            bft_network.stop_replica(non_primary)

            primary = await bft_network.get_current_primary()

            await bft_network.wait_for_slow_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20, replica_id=primary, timeout=180)

            bft_network.stop_replica(primary)
            bft_network.start_replica(primary)

            await bft_network.wait_for_replicas_to_reach_at_least_view(replicas_ids=bft_network.all_replicas(), expected_view=view + 1, timeout=60)

            bft_network.start_replica(non_primary)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @verify_linearizability()
    async def test_restart_f_replicas_verify_fastpath(self, bft_network, tracker):
        """
        1. Start all Replicas.
        2. Chose F non Primary Replicas to stop.
        3. Advance the others 5 Checkpoints.
        4. Bring back the previously stopped F replicas.
        5. Wait for State Transfer to finish on all.
        6. Verify the system is processing on Fast Path.
        7. Goto Step 2.
        """
        # start replicas
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]

        #log = foo()
        loop_counter = 0
        while (loop_counter < 100):
            loop_counter = loop_counter + 1

            primary = await bft_network.get_current_primary()
            replicas_to_restart = random.sample(
                bft_network.all_replicas(without={primary}), bft_network.config.f)

            [bft_network.stop_replica(i) for i in replicas_to_restart]

            skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without = set(replicas_to_restart)),
                num_of_checkpoints_to_add=5,
                verify_checkpoint_persistency=False,
                assert_state_transfer_not_started=False
            )

            [bft_network.start_replica(i) for i in replicas_to_restart]
            await trio.sleep(seconds=10)
            await self._await_replicas_in_state_transfer(log, bft_network, skvbc, primary)

            await bft_network.wait_for_fast_path_to_be_prevalent(
                run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @verify_linearizability()
    async def test_recovering_of_primary_with_initiated_view_change(self, bft_network, tracker):
        """
        The Apollo test, which should be part of the test_skvbc_restart_recovery suite needs to implement the following steps:
        1) Start all Replicas.
        2) Stop the expected next Primary.
        3) Advance the system 5 Checkpoints.
        4) Stop the current Primary.
        5) Send Client operations in order to trigger View Change.
        6) Bring back up all the previously stopped replicas.
        7) Verify View Change was successful.
        8) Goto Step 2.
        """

        # start replicas
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]

        loop_counter = 0
        while (loop_counter < 100):
            loop_counter = loop_counter + 1
            log.log_message(f"Loop run {loop_counter}")
            # loop start
            view = await bft_network.get_current_view()

            primary = await bft_network.get_current_primary()
            next_primary = (primary + 1) % bft_network.config.n
            # Stop the expected next Primary.
            bft_network.stop_replica(next_primary)

            # Advance the system 5 Checkpoints.
            skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without = {next_primary}),
                num_of_checkpoints_to_add=5,
                verify_checkpoint_persistency=False,
                assert_state_transfer_not_started=False
            )

            # Stop the current Primary
            bft_network.stop_replica(primary)

            # Send Client operations in order to trigger View Change.
            await skvbc.run_concurrent_ops(10)

            #Bring back up all the previously stopped replicas.
            bft_network.start_replica(primary)
            bft_network.start_replica(next_primary)

            # Wait for quorum of replicas to move to a higher view
            with trio.fail_after(seconds=40):
                old_view = view
                while view == old_view:
                    log.log_message(f"waiting for vc current view={view}, old_view={old_view}")
                    await skvbc.run_concurrent_ops(1)
                    with trio.move_on_after(seconds=5):
                        view = await bft_network.get_current_view()
                    await trio.sleep(seconds=1)

            log.log_message("Checking for replicas in state transfer:")
            # Check for replicas in state transfer and wait for them to catch
            # up before moving on to checking the system returns on Fast Path
            primary = await bft_network.get_current_primary()
            log.log_message(f"Current primary after start {primary}")

            await self._await_replicas_in_state_transfer(log, bft_network, skvbc, primary)

            await bft_network.wait_for_replicas_to_reach_at_least_view(replicas_ids=bft_network.all_replicas(), expected_view=view, timeout=60)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @verify_linearizability()
    async def test_view_change_with_non_primary_replica_in_state_transfer(self, bft_network, tracker):
        """
        The Apollo test, which should be part of the test_skvbc_restart_recovery suite implements the following steps:
        1) Start all Replicas.
        2) Chose a random non-primary replica to stop (also avoid the expected next Primary in order to perform a single
           view increment).
        3) Advance the system 5 Checkpoints.
        4) Stop the Primary and send Client operations to trigger View Change.
        5) Start all the previously stopped Replicas.
        6) Verify View Change was successful.
        7) Wait for State Transfer to finish and for all replicas to participate in Fast Path.
        8) Goto Step 2.
        """

        # start replicas
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]

        loop_counter = 0
        while (loop_counter < 100):
            loop_counter = loop_counter + 1
            log.log_message(f"Loop run {loop_counter}")
            # loop start
            view = await bft_network.get_current_view()

            primary = await bft_network.get_current_primary()
            next_primary = (primary + 1) % bft_network.config.n
            # Stop a random replica (current and next Primary excluded).
            replica_to_restart = random.choice(
                bft_network.all_replicas(without={primary, next_primary}))
            bft_network.stop_replica(replica_to_restart)

            # Advance the system 5 Checkpoints.
            skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
            await skvbc.fill_and_wait_for_checkpoint(
                initial_nodes=bft_network.all_replicas(without={replica_to_restart}),
                num_of_checkpoints_to_add=5,
                verify_checkpoint_persistency=False,
                assert_state_transfer_not_started=False
            )

            # Stop the current Primary
            bft_network.stop_replica(primary)

            # Send Client operations in order to trigger View Change.
            await skvbc.run_concurrent_ops(10)

            #Bring back up all the previously stopped replicas.
            bft_network.start_replica(replica_to_restart)
            bft_network.start_replica(primary)

            # Wait for quorum of replicas to move to a higher view
            with trio.fail_after(seconds=40):
                old_view = view
                while view == old_view:
                    log.log_message(f"waiting for vc current view={view}, old_view={old_view}")
                    await skvbc.run_concurrent_ops(1)
                    with trio.move_on_after(seconds=5):
                        view = await bft_network.get_current_view()
                    await trio.sleep(seconds=1)

            log.log_message("Checking for replicas in state transfer:")
            # Check for replicas in state transfer and wait for them to catch
            # up before moving on to checking the system returns on Fast Path
            await self._await_replicas_in_state_transfer(log, bft_network, skvbc, primary)

            log.log_message("wait for fast path to be prevalent")
            await bft_network.wait_for_fast_path_to_be_prevalent(
                run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)
            log.log_message("fast path prevailed")

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: c == 0, rotate_keys=True)
    @verify_linearizability()
    async def test_view_change_with_isolated_replicas(self, bft_network, tracker):
        """
        test View Changes with multiple View increments, where the
        isolated F-1 expected next primaries will not be able to step in as
        primaries, but will activate the corresponding view for which it is
        theirs turn to become Primary.

        Step by step scenario:
        1. Use a one way isolating adversary to isolate the F-1 replicas after the current primary in such a way that they cannot send messages to the peers, but can receive messages from them.
        2. Stop the current primary.
        3. Send Client requests to trigger a View Change.
        4. Wait for the system to finish View Change. Note that multiple View increments will happen.
        5. Drop the network adversary and verify Fast Commit Path is recovered in the system by introducing client requests.

        We can perform this test in a loop multiple times.
        """

        # start replicas
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]

        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        loop_count = 0
        while (loop_count < 100):
            loop_count = loop_count + 1

            primary = await bft_network.get_current_primary()

            index_list = range(primary + 1, primary + bft_network.config.f)
            replicas_to_isolate = []
            for i in index_list:
                replicas_to_isolate.append(i % bft_network.config.n)

            other_replicas = bft_network.all_replicas(without=set(replicas_to_isolate))

            view = await bft_network.get_current_view()

            with net.ReplicaOneWayTwoSubsetsIsolatingAdversary(bft_network, other_replicas, replicas_to_isolate) as adversary:
                adversary.interfere()

                bft_network.stop_replica(primary)
                await skvbc.run_concurrent_ops(10)

                await bft_network.wait_for_replicas_to_reach_at_least_view(other_replicas, expected_view=view + bft_network.config.f, timeout=60)

            bft_network.start_replica(primary)

            await bft_network.wait_for_fast_path_to_be_prevalent(
                run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @skip_for_tls
    @with_trio
    @with_bft_network(start_replica_cmd)
    @with_constant_load
    async def test_recovering_view_after_restart_with_packet_loss(self, bft_network, skvbc, constant_load):
        """
        Implement a test with constant client load that is running under the Packet Dropping Adversary
        with 50% chance of dropping a packet from all replicas and trigger View Changes periodically.
        The test should run for 10 minutes and in the end we drop the adversarial packet dropping
        activity and verify that we will regain Fast Commit path in the system.
        Step by step scenario:
        1. Create a packet dropping adversary with 50% chance of dropping any packet.
        2. Introduce constant client load.
        3. Restart the current primary to trigger View Change.
        4. Wait for View Change to complete (because of the packet dropping activity this could take significantly more time than usual and a non-deterministic view increments can happen).
        5. Loop to step 3.
        """
        # uncomment for live tracking of log messages from the test
        # log = foo()
        [bft_network.start_replica(i) for i in bft_network.all_replicas()]

        loop_count_outer = 0

        while (loop_count_outer < 20):
            loop_count_outer = loop_count_outer + 1
            loop_count = 0
            primary = 0
            while (loop_count < 5):
                loop_count = loop_count + 1
                log.log_message(f"Loop run: {loop_count}")
                primary = await bft_network.get_current_primary()
                with net.PacketDroppingAdversary(bft_network, drop_rate_percentage=50) as adversary:
                    adversary.interfere()

                    bft_network.stop_replica(primary)
                    bft_network.start_replica(primary)

                    await trio.sleep(seconds=20)

                primary = (primary + 1) % bft_network.config.n
                await self._await_replicas_in_state_transfer(log, bft_network, skvbc, primary)

            await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @staticmethod
    async def _await_replicas_in_state_transfer(logger, bft_network, skvbc, primary):
        for r in bft_network.get_live_replicas():
            logger.log_message(f"Replica {r} is alive")

            fetching = await bft_network.is_fetching(r)
            if fetching:
                logger.log_message(f"Replica {r} is fetching, waiting for ST to finish ...")
                # assuming Primary has latest state
                with trio.fail_after(seconds=100):
                    key = ['replica', 'Gauges', 'lastStableSeqNum']
                    primary_last_stable = await bft_network.metrics.get(primary, *key)
                    fetching_last_stable = await bft_network.metrics.get(r, *key)
                    while primary_last_stable != fetching_last_stable:
                        primary_last_stable = await bft_network.metrics.get(primary, *key)
                        fetching_last_stable = await bft_network.metrics.get(r, *key)
                        logger.log_message(
                            f"primary_last_stable={primary_last_stable} fetching_last_stable={fetching_last_stable}")
                        await skvbc.run_concurrent_ops(50)
                        await trio.sleep(seconds=5)
