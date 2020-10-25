
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

from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX
from util import bft_network_partitioning as net
from util import eliot_logging as log

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
            "-v", view_change_timeout_milli,
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else "",
            "-t", os.environ.get('STORAGE_TYPE')]


def start_replica_cmd_with_vc_timeout(vc_timeout):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, view_change_timeout_milli=vc_timeout)
    return wrapper


class SkvbcChaoticStartupTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @unittest.skip("Unstable because of BC-5164")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_delayed_replicas_start_up(self, bft_network, skvbc, nursery):
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

        initial_view = 0
        try:
            # Delayed replica start-up...
            for r in replicas_starting_order:
                bft_network.start_replica(r)
                await trio.sleep(seconds=10)

            current_view = await bft_network.wait_for_view(
                replica_id=0,
                expected=lambda v: v > initial_view,
                err_msg="Make sure view change has occurred during the delayed replica start-up."
            )

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

            await trio.sleep(seconds=20)

            # Stop sending requests, and make sure the restarted replica
            # is up-and-running and participates in consensus
            nursery.cancel_scope.cancel()

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
    async def test_f_staggered_replicas_requesting_vc(self, bft_network, skvbc, nursery):
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
    async def test_f_minus_one_staggered_replicas_requesting_vc(self, bft_network, skvbc, nursery):
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

    from os import environ
    @unittest.skipIf(environ.get('BUILD_COMM_TCP_TLS', "").lower() == "true", "Unstable on CI (TCP/TLS only)")
    @with_trio
    @with_bft_network(start_replica_cmd_with_vc_timeout("20000"),
                      selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def stuck_view_change_bug_recreation(self, bft_network, skvbc, nursery):
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