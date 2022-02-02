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


class SkvbcRestartRecoveryTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

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
                nursery.start_soon(skvbc.send_indefinite_ops)
                while True:
                    total_nb_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    with trio.fail_after(seconds=5):
                        while replica_to_restart not in bft_network.get_live_replicas():
                            await trio.sleep(seconds=0.1)
                    if replica_to_restart in bft_network.get_live_replicas():
                        await trio.sleep(seconds=1)  # wait for some client operations to be processed
                    log.log_message(f"Restarting replica {replica_to_restart}")
                    bft_network.stop_replica(replica_to_restart, True)
                    bft_network.start_replica(replica_to_restart)
                    latest_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    with trio.fail_after(seconds=15):
                        while latest_fast_paths - total_nb_fast_paths == 0:
                            await trio.sleep(seconds=0.1)
                            latest_fast_paths = await bft_network.num_of_fast_path_requests(primary_replica)
                    log.log_message(f"fast paths since last: {latest_fast_paths - total_nb_fast_paths}")
                    log.log_message(f"latest_fast_paths: {latest_fast_paths}")
                    slow_paths = await bft_network.num_of_slow_path_requests(primary_replica)
                    log.log_message(f"slow_paths: {slow_paths}")

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2, rotate_keys=False)
    async def test_restarting_replica_during_system_is_in_view_change(self, bft_network):
        """
        The goal of this test is trigger multiple View Changes and restart 1 non-next Priamry replica
        in order to test restart recovery during the replicas performing a View Change.
        """

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

        while view < 100:
            replica_to_restart = random.choice(
                bft_network.all_replicas(without={current_primary, next_primary}))
            log.log_message(f"Initiating View Change by restarting replica {current_primary} in view {view}")
            bft_network.stop_replica(current_primary, True)
            await skvbc.run_concurrent_ops(5)
            await trio.sleep(seconds=viewChangeTimeoutSec)
            log.log_message(f"Restarting replica during View Change {replica_to_restart}")
            bft_network.stop_replica(replica_to_restart, True)
            bft_network.start_replica(replica_to_restart)
            await trio.sleep(seconds=1)
            bft_network.start_replica(current_primary)

            old_view = view

            view = await bft_network.get_current_view()
            with trio.fail_after(seconds=40):
                while view == old_view:
                    log.log_message(f"waiting for vc view={view}")
                    await skvbc.run_concurrent_ops(1)
                    view = await bft_network.get_current_view()
                    await trio.sleep(seconds=1)

            advance = view - old_view
            current_primary += advance
            current_primary = current_primary % bft_network.config.n
            next_primary = (current_primary + 1) % bft_network.config.n

            log.log_message(f"view is {view}")

            log.log_message("Checking for replicas in state transfer:")
            for r in bft_network.get_live_replicas():
                fetching = await bft_network.is_fetching(r)
                if fetching:
                    log.log_message(f"Replica {r} is fetching, waiting for ST to finish ...")
                    # assuming Primary has latest state
                    await bft_network.wait_for_state_transfer_to_stop(current_primary, r)

            log.log_message("wait for fast path to be prevalent")
            await bft_network.wait_for_fast_path_to_be_prevalent(
                run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=1)
            log.log_message("fast path prevailed")

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)
