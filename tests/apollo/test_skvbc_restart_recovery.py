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
        # class foo:
        #     def log_message(self, var):
        #         print(f"{var}")
        #
        # log = foo()

        with trio.move_on_after(seconds=60):
            async with trio.open_nursery() as nursery:
                # Start the sending of client operations in the background.
                nursery.start_soon(skvbc.send_indefinite_ops)
                while True:
                    # Get the total amount of Fast Paths the replicas have committed.
                    total_nb_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    # Since we restart the selected replica multiple times in this loop, wait for it to initialise.
                    with trio.fail_after(seconds=5):
                        while replica_to_restart not in bft_network.get_live_replicas():
                            await trio.sleep(seconds=0.1)
                    if replica_to_restart in bft_network.get_live_replicas():
                        await trio.sleep(seconds=1)  # wait for some client operations to be processed.
                    log.log_message(f"Restarting replica {replica_to_restart}")
                    # Restart the selected replica.
                    bft_network.stop_replica(replica_to_restart, True)
                    bft_network.start_replica(replica_to_restart)
                    # Get the latest value for the fast paths processed in the system.
                    latest_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    # Wait for the system to start processing requests on the fast path
                    # once again since all replicas are up.
                    with trio.fail_after(seconds=15):
                        while latest_fast_paths - total_nb_fast_paths == 0:
                            await trio.sleep(seconds=0.1)
                            latest_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    log.log_message(f"fast paths since last: {latest_fast_paths - total_nb_fast_paths}")
                    log.log_message(f"latest_fast_paths: {latest_fast_paths}")
                    slow_paths = await bft_network.num_of_slow_path_requests(primary_replica)
                    log.log_message(f"slow_paths: {slow_paths}")

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

        # uncomment for live tracking of log messages from the test
        # class foo:
        #     def log_message(self, var):
        #         print(f"{var}")
        #
        # log = foo()

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
            for r in bft_network.get_live_replicas():
                fetching = await bft_network.is_fetching(r)
                if fetching:
                    log.log_message(f"Replica {r} is fetching, waiting for ST to finish ...")
                    # assuming Primary has latest state
                    with trio.fail_after(seconds=100):
                        key = ['replica', 'Gauges', 'lastStableSeqNum']
                        primary_last_stable = await bft_network.metrics.get(current_primary, *key)
                        fetching_last_stable = await bft_network.metrics.get(r, *key)
                        while primary_last_stable != fetching_last_stable:
                            primary_last_stable = await bft_network.metrics.get(current_primary, *key)
                            fetching_last_stable = await bft_network.metrics.get(r, *key)
                            log.log_message(f"primary_last_stable={primary_last_stable} fetching_last_stable={fetching_last_stable}")
                            await skvbc.run_concurrent_ops(50)
                            await trio.sleep(seconds=5)

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
