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

import unittest
import trio
import os.path
import random

from util.test_base import ApolloTest
from util import bft
from util import skvbc as kvbc
from util.skvbc import SimpleKVBCProtocol
from util.skvbc_history_tracker import verify_linearizability
from math import inf

from util.bft import KEY_FILE_PREFIX, with_trio, with_bft_network, skip_for_tls
from util import eliot_logging as log


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    if os.environ.get('BLOCKCHAIN_VERSION', default="1").lower() == "4" :
        blockchain_version = "4"
    else :
        blockchain_version = "1"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-V",blockchain_version,
            "-v", viewChangeTimeoutMilli 
            ]

def start_replica_cmd_with_slowdown(builddir, replica_id):
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "--delay-state-transfer-messages-millisec", '8'
            ]

class SkvbcPersistenceTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability()
    async def test_view_change_transitions_safely_without_quorum(self, bft_network, tracker):
        """
        Start up only N-2 out of N replicas and send client commands. This should
        trigger a succesful view change attempt and trigger the assert in issue
        #194.

        This is a regression test for
        https://github.com/vmware/concord-bft/issues/194.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        [bft_network.start_replica(i) for i in range(1, bft_network.config.n - 1)]
        with trio.fail_after(60):  # seconds
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    skvbc.send_indefinite_ops)
                # See if replica 1 has become the new primary
                await bft_network.wait_for_view(
                    replica_id=1,
                    expected=lambda v: v == 1
                )
                # At this point a view change has successfully completed
                # with node 1 as the primary. The faulty assertion should
                # have crashed old nodes.
                #
                # In case the nodes didn't crash, stop node 1 to trigger
                # another view change. Starting node 0 should allow the view
                # change to succeed. If there is a timeout then the other
                # nodes have likely crashed due to the faulty assertion. The
                # crash will show in the logs when running the test
                # verbosely:
                #
                # 21: INFO 2019-08-30skvbc_replica:
                # /home/andrewstone/concord-bft.py/bftengine/src/bftengine/PersistentStorageImp.cpp:881:
                # void
                # bftEngine::impl::PersistentStorageImp::verifySetDescriptorOfLastExitFromView(const
                # bftEngine::impl::DescriptorOfLastExitFromView &):
                # Assertion `false' failed.
                bft_network.stop_replica(1)
                bft_network.start_replica(0)
                while True:
                    with trio.move_on_after(.5):  # seconds
                        key = ['replica', 'Gauges', 'lastAgreedView']
                        replica_id = 2
                        view = await bft_network.metrics.get(replica_id, *key)
                        if view == 2:
                            # success!
                            nursery.cancel_scope.cancel()

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability()
    async def test_read_written_data_after_restart_of_all_nodes(self, bft_network, tracker):
        """
        This test aims to validate the blockchain is persistent
        (i.e. the data is still available after restarting all nodes)
        1) Write and track a key-value entry to the blockchain
        2) Restart all replicas (stop all, followed by start all)
        3) Verify the same key-value can be read from the blockchain
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        # Pre-write a number of blocks to the chain to avoid the situation where
        # a replica fails to execute and needs to recover from the genesis block - this is an unhandled
        # edge case. This is possible when only a single write is used because
        # a reply guarantees 2f + 1 executions while the remaining f replicas race
        # to execute concurrently with the shutdown. By pre-writing to the chain we make sure
        # some number of blocks > 1 are created on every replica.
        # See BC-17607 for more info.
        
        # Important: block accumulation is assumed to be turned off!
        for _ in range(5):
            await skvbc.send_write_kv_set()
        
        client = bft_network.random_client()
        
        # Check that we've created more than 1 block
        last_block_id = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))
        self.assertGreater(last_block_id, 1) 

        (key, val) = await skvbc.send_write_kv_set()

        bft_network.stop_all_replicas()
        bft_network.start_all_replicas()

        kv_reply = await skvbc.send_read_kv_set(client, key)

        self.assertEqual({key: val}, kv_reply)

    @with_trio
    @with_bft_network(start_replica_cmd)
    @verify_linearizability()
    async def test_checkpoints_saved_and_transferred(self, bft_network, tracker):
        """
        Start a 3 nodes out of a 4 node cluster. Write a specific key, then
        enough data to the cluster to trigger a checkpoint and log garbage
        collection. Then stop all the nodes, restart the 3 nodes that should
        have checkpoints as well as the 4th empty node. The 4th empty node
        should catch up via state transfer.

        Then take down one of the original nodes, and try to read the specific
        key.

        After that ensure that a newly put value can be retrieved.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        stale_node = random.choice(bft_network.all_replicas(without={0}))

        client, known_key, known_val = \
            await skvbc.prime_for_state_transfer(stale_nodes={stale_node})

        # Start the replica without any data, and wait for state transfer to
        # complete.

        bft_network.start_replica(stale_node)
        
        await bft_network.wait_for_state_transfer_to_start()
        up_to_date_node = 0
        await bft_network.wait_for_state_transfer_to_stop(up_to_date_node,
                                                          stale_node)

        await bft_network.force_quorum_including_replica(stale_node)

        # Retrieve the value we put first to ensure state transfer worked
        # when the log went away
        kvpairs = await skvbc.send_read_kv_set(client, known_key)
        self.assertDictEqual(dict([(known_key, known_val)]), kvpairs)

        # Perform a put/get transaction pair to ensure we can read newly
        # written data after state transfer.

        await skvbc.read_your_writes()

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_slowdown)
    @verify_linearizability()
    async def test_st_when_fetcher_crashes(self, bft_network, tracker):
        """
        Start N-1 nodes out of a N node cluster (hence 1 stale node). Write a specific key,
        then enough data to the cluster to trigger a checkpoint. Then stop all the nodes,
        restart the N-1 nodes that should have checkpoints as well as the stale node.

        Kill and restart the stale node multiple times during fetching and
        make sure that it catches up.

        Then force a quorum including the stale node, and try to read the specific
        key.

        After that ensure that a newly put value can be retrieved.
        """
        stale_node = random.choice(bft_network.all_replicas(without={0}))
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        client, known_key, known_val = \
            await skvbc.prime_for_state_transfer(stale_nodes={stale_node}, checkpoints_num=10)
        # Start the empty replica, wait for it to start fetching, then stop
        # it.
        bft_network.start_replica(stale_node)
        await bft_network.wait_for_fetching_state(stale_node)
        bft_network.stop_replica(stale_node)

        # Loop repeatedly starting and killing the destination replica after
        # state transfer has started. On each restart, ensure the node is
        # still fetching or that it has received all the data.
        await self._fetch_or_finish_state_transfer_while_crashing(bft_network, 0, stale_node)

        # Restart the replica and wait for state transfer to stop
        bft_network.start_replica(stale_node)
        await bft_network.wait_for_state_transfer_to_stop(0, stale_node)

        await bft_network.force_quorum_including_replica(stale_node)

        # Retrieve the value we put first to ensure state transfer worked
        # when the log went away
        kvpairs = await skvbc.send_read_kv_set(client, known_key)
        self.assertDictEqual(dict([(known_key, known_val)]), kvpairs)

        # Perform a put/get transaction pair to ensure we can read newly
        # written data after state transfer.

        await skvbc.read_your_writes()

    async def _fetch_or_finish_state_transfer_while_crashing(self,
                                                             bft_network,
                                                             up_to_date_node,
                                                             stale_node,
                                                             nb_crashes=20):
       for _ in range(nb_crashes):
           log.log_message(message_type=f'Restarting replica {stale_node}')
           bft_network.start_replica(stale_node)
           try:
               await bft_network.wait_for_fetching_state(stale_node)
               # Sleep a bit to give some time for the fetch to make progress
               await trio.sleep(random.uniform(0, 1))

           except trio.TooSlowError:
               # We never made it to fetching state. Are we done?
               try:
                    await bft_network.wait_for_state_transfer_to_stop(
                       up_to_date_node, stale_node)
                    break
               except trio.TooSlowError:
                   self.fail("State transfer did not complete, " +
                             "but we are not fetching either!")
           finally:
               log.log_message(message_type=f'Stopping replica {stale_node}')
               bft_network.stop_replica(stale_node)

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_slowdown,
                      selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability()
    async def test_st_when_fetcher_and_sender_crash(self, bft_network, tracker):
        """
        Start N-1 nodes out of a N node cluster. Write a specific key, then
        enough data to the cluster to trigger a checkpoint.

        Restart the empty replica until it starts fetching from a non-primary
        node.

        Repeatedly stop and start the source, as well as the fetcher nodes.

        Await for state transfer to complete.

        Take down one of the backup replicas, to ensure the stale node is part
        of a result quorum.

        After that ensure that the key-value entry initially written
        can be retrieved.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        stale_node = random.choice(bft_network.all_replicas(without={0}))

        client, known_key, known_val = \
            await skvbc.prime_for_state_transfer(stale_nodes={stale_node},
                                                           checkpoints_num=random.randint(10, 13))

        # exclude the primary and the stale node
        non_primary_replicas = bft_network.all_replicas(without={0, stale_node})

        await self._run_state_transfer_while_crashing_non_primary(
            bft_network=bft_network,
            primary=0, stale=stale_node,
            non_primary_replicas=non_primary_replicas
        )

        await bft_network.force_quorum_including_replica(stale_node)

        # Retrieve the value we put first to ensure state transfer worked
        # when the log went away
        kvpairs = await skvbc.send_read_kv_set(client, known_key)
        self.assertDictEqual(dict([(known_key, known_val)]), kvpairs)

    async def _run_state_transfer_while_crashing_non_primary(
            self, bft_network,
            primary, stale,
            non_primary_replicas):

        source_replica_id = \
            await self._restart_stale_until_non_primary_chosen_as_source(
                bft_network, primary, stale, non_primary_replicas
            )

        if source_replica_id in non_primary_replicas:
            log.log_message(message_type=f'Stopping source replica {source_replica_id}')
            bft_network.stop_replica(source_replica_id)

            log.log_message(message_type=f'Re-starting stale replica {stale} to start state transfer')
            bft_network.start_replica(stale)

            await bft_network.wait_for_state_transfer_to_stop(
                up_to_date_node=primary,
                stale_node=stale
            )

            log.log_message(message_type=f'State transfer completed, despite initial source '
                  f'replica {source_replica_id} being down')

            bft_network.start_replica(source_replica_id)
        else:
            log.log_message(message_type="No source replica set in stale node, checking "
                  "if state transfer has already completed...")
            await bft_network.wait_for_state_transfer_to_stop(
                up_to_date_node=primary,
                stale_node=stale
            )
            log.log_message(message_type="State transfer completed before we had a chance "
                  "to stop the source replica.")

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability()
    async def test_st_while_crashing_primary_with_vc(self, bft_network, tracker):
        """
        Start N-1 nodes out of a N node cluster. Write a specific key, then
        enough data to the cluster to trigger a checkpoint.

        Stop the primary and trigger view change

        Await for state transfer to complete.

        Take down one of the backup replicas, to ensure the stale node is part
        of a result quorum.

        After that ensure that the key-value entry initially written
        can be retrieved.
        """
        await self._test_st_while_crashing_primary(
            bft_network=bft_network,
            trigger_view_change=True,
            crash_repeatedly=False,
            tracker=tracker
        )

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability()
    async def test_st_while_crashing_primary_no_vc(self, bft_network, tracker):
        """
        Start N-1 nodes out of a N node cluster. Write a specific key, then
        enough data to the cluster to trigger a checkpoint.

        Stop the primary (do NOT trigger view change)

        Start the stale node, in order to trigger state transfer.

        Await for state transfer to complete.

        Take down one of the backup replicas, to ensure the stale node is part
        of a result quorum.

        After that ensure that the key-value entry initially written
        can be retrieved.
        """
        await self._test_st_while_crashing_primary(
            bft_network,
            trigger_view_change=False,
            crash_repeatedly=False,
            tracker=tracker
        )

    async def _test_st_while_crashing_primary(
            self, bft_network, trigger_view_change, crash_repeatedly, tracker):
        # we need a BFT network with f >= 2, allowing us to have 2
        # crashed replicas at the same time (the primary and the stale node)

        n = bft_network.config.n

        stale_replica = n - 1
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)

        client, known_key, known_val = \
            await skvbc.prime_for_state_transfer(stale_nodes={stale_replica},
                                                           checkpoints_num=2)
        view = await bft_network.wait_for_view(
            replica_id=0,
            expected=lambda v: v == 0,
            err_msg="Make sure we are in the initial view."
        )

        log.log_message(message_type=f'Initial view number is {view}, as expected.')

        if crash_repeatedly:
            await self._run_state_transfer_while_crashing_primary_repeatedly(
                skvbc=skvbc,
                bft_network=bft_network,
                n=n,
                primary=0,
                stale=stale_replica
            )
        else:
            await self._run_state_transfer_while_crashing_primary_once(
                skvbc=skvbc,
                bft_network=bft_network,
                n=n,
                primary=0,
                stale=stale_replica,
                trigger_view_change=trigger_view_change
            )

        await bft_network.force_quorum_including_replica(stale_replica)

        kvpairs = await skvbc.send_read_kv_set(client, known_key)
        self.assertDictEqual(dict([(known_key, known_val)]), kvpairs)

    async def _run_state_transfer_while_crashing_primary_once(
            self, skvbc, bft_network, n, primary, stale, trigger_view_change=False):

        log.log_message(message_type=f'Stopping primary replica {primary} to trigger view change')
        bft_network.stop_replica(primary)

        log.log_message(message_type=f'Re-starting stale replica {stale} to start state transfer')
        bft_network.start_replica(stale)

        if trigger_view_change:
            await self._trigger_view_change(skvbc)

        up_to_date_replica = random.choice(
            bft_network.all_replicas(without={primary, stale}))

        await bft_network.wait_for_state_transfer_to_stop(
            up_to_date_node=up_to_date_replica,
            stale_node=stale,
            stop_on_stable_seq_num=True
        )

        if trigger_view_change:
            await bft_network.wait_for_view(
                replica_id=up_to_date_replica,
                expected=lambda v: v > 0,
                err_msg="Make sure view change has been triggered during state transfer."
            )
            await trio.sleep(seconds=5)  # so that replicas can rebuild the active window after the VC
        else:
            await bft_network.wait_for_view(
                replica_id=up_to_date_replica,
                expected=lambda v: v == 0,
                err_msg="Make sure view change has NOT been triggered during state transfer."
            )

        log.log_message(message_type=f'State transfer completed, despite the primary '
              f'replica crashing.')

        bft_network.start_replica(primary)

    async def _run_state_transfer_while_crashing_primary_repeatedly(
            self, skvbc, bft_network, n, primary, stale):

        current_primary = primary
        stable_replicas = bft_network.all_replicas(without={primary, stale})

        for _ in range(2):
            log.log_message(message_type=f'Stopping current primary replica {current_primary} '
                  f'to trigger view change')
            bft_network.stop_replica(current_primary)
            await self._trigger_view_change(skvbc)

            log.log_message(message_type=f'Repeatedly restarting stale replica {stale} '
                  f'to with view change running in the background.')

            await self._fetch_or_finish_state_transfer_while_crashing(
                bft_network=bft_network,
                up_to_date_node=random.choice(stable_replicas),
                stale_node=stale,
                nb_crashes=3)

            bft_network.start_replica(current_primary)

            current_primary = await bft_network.wait_for_view(
                replica_id=random.choice(stable_replicas),
                expected=lambda v: v > current_primary
            )

            stable_replicas = \
                bft_network.all_replicas(without={current_primary, stale})

        bft_network.start_replica(stale)

        await bft_network.wait_for_state_transfer_to_stop(
            up_to_date_node=current_primary,
            stale_node=stale
        )

        await bft_network.wait_for_view(
            replica_id=random.choice(stable_replicas),
            expected=lambda v: v > 0,
            err_msg="Make sure view change has been triggered during state transfer."
        )

        log.log_message(message_type=f'State transfer completed, despite the primary '
              f'replica crashing repeatedly in the process.')

    async def _trigger_view_change(self, skvbc):
        log.log_message(message_type="Sending random transactions to trigger view change...")
        with trio.move_on_after(1):  # seconds
            async with trio.open_nursery() as nursery:
                nursery.start_soon(skvbc.send_indefinite_write_requests)

    async def _restart_stale_until_non_primary_chosen_as_source(
            self, bft_network, primary, stale, non_primary_replicas):

        source_replica_id = inf

        log.log_message(message_type=f'Restarting stale replica until '
              f'it fetches from {non_primary_replicas}...')
        with trio.move_on_after(10):  # seconds
            while True:
                bft_network.start_replica(stale)
                source_replica_id = await bft_network.wait_for_fetching_state(
                    replica_id=stale
                )
                bft_network.stop_replica(stale)
                if source_replica_id in non_primary_replicas:
                    self.assertTrue(
                        expr=source_replica_id != primary,
                        msg="The source must NOT be the primary "
                        "(to avoid triggering a view change)"
                    )
                    log.log_message(message_type=f'Stale replica fetching from {source_replica_id}')
                    break
        
        self.assertTrue(source_replica_id != inf, msg="Stale replica is not fetching right now.")
        return source_replica_id
