# Concord
#
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.
import os.path
import unittest

import trio

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util import operator
from util.object_store import ObjectStore, start_replica_cmd_prefix, with_object_store
import sys
from util import eliot_logging as log
import concord_msgs as cmf_msgs

sys.path.append(os.path.abspath("../../util/pyclient"))

import bft_client

def start_replica_cmd_with_object_store(builddir, replica_id, config):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    ret = start_replica_cmd_prefix(builddir, replica_id, config)
    ret.extend(["-b", "2", "-q", "1", "-o", builddir + "/operator_pub.pem"])
    return ret

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
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-b", "2",
            "-q", "1",
            "-o", builddir + "/operator_pub.pem"]


class SkvbcReconfigurationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.object_store = ObjectStore()

    @classmethod
    def tearDownClass(cls):
        pass

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_key_exchange_command(self, bft_network):
        """
            No initial key rotation
            Sends key exchange command to replica 0
            New keys for replica 0 should get effective at checkpoint 2, i.e. seqnum 300
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
         
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        await op.key_exchange([0])
        for i in range(450):
            await skvbc.write_known_kv()

        sent_key_exchange_counter = await bft_network.metrics.get(0, *["KeyExchangeManager", "Counters", "sent_key_exchange"])
        assert sent_key_exchange_counter == 1
        self_key_exchange_counter = await bft_network.metrics.get(0, *["KeyExchangeManager", "Counters", "self_key_exchange"])
        assert self_key_exchange_counter == 1
        public_key_exchange_for_peer_counter = await bft_network.metrics.get(1, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
        assert public_key_exchange_for_peer_counter == 1
        
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_wedge_command(self, bft_network):
        """
             Sends a wedge command and checks that the system stops processing new requests.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a wedge command
             2. The client verifies that the system reached a super stable checkpoint.
             3. The client tries to initiate a new write bft command and fails
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.wedge()
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)
    
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_wedge_command_with_state_transfer(self, bft_network):
        """
            This test checks that even a replica that received the super stable checkpoint via the state transfer mechanism
            is able to stop at the super stable checkpoint.
            The test does the following:
            1. Start all replicas but 1
            2. A client sends a wedge command
            3. Validate that all started replicas reached to the next next checkpoint
            4. Start the late replica
            5. Validate that the late replica completed the state transfer
            6. Validate that all replicas stopped at the super stable checkpoint and that new commands are not being processed
        """
        initial_prim = 0
        late_replicas = bft_network.random_set_of_replicas(1, {initial_prim})
        on_time_replicas = bft_network.all_replicas(without=late_replicas)
        bft_network.start_replicas(on_time_replicas)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.wait_for_liveness()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        with log.start_action(action_type="send_wedge_cmd",
                              checkpoint_before=checkpoint_before,
                              late_replicas=list(late_replicas)):
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            await op.wedge()

        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, on_time_replicas)

        bft_network.start_replicas(late_replicas)

        await bft_network.wait_for_state_transfer_to_start()
        for r in late_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_wedge_command_with_f_failures(self, bft_network):
        """
            This test checks that even a replica that received the super stable checkpoint via the state transfer mechanism
            is able to stop at the super stable checkpoint.
            The test does the following:
            1. Start all replicas but 2
            2. A client sends a wedge command
            3. Validate that all started replicas have reached the wedge point
            4. Restart the live replicas and validate the system is able to make progress
            5. Start the late replica
            6. Validate that the late replicas completed the state transfer
            7. Join the late replicas to the quorum and make sure the system is able to make progress
        """
        initial_prim = 0
        late_replicas = bft_network.random_set_of_replicas(2, {initial_prim})
        on_time_replicas = bft_network.all_replicas(without=late_replicas)
        bft_network.start_replicas(on_time_replicas)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.wait_for_liveness()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        with log.start_action(action_type="send_wedge_cmd",
                              checkpoint_before=checkpoint_before,
                              late_replicas=list(late_replicas)):
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            await op.wedge()

        with trio.fail_after(seconds=60):
            done = False
            while done is False:
                await op.wedge_status(quorum=bft_client.MofNQuorum(on_time_replicas, len(on_time_replicas)), fullWedge=False)
                rsi_rep = client.get_rsi_replies()
                done = True
                for r in rsi_rep.values():
                    res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                    status = res[0].response.stopped
                    if status is False:
                        done = False
                        break


        # Make sure the system is able to make progress
        bft_network.stop_replicas(on_time_replicas)
        bft_network.start_replicas(on_time_replicas)
        for i in range(100):
            await skvbc.write_known_kv()

        # Start late replicas and wait for state transfer to stop
        bft_network.start_replicas(late_replicas)

        await bft_network.wait_for_state_transfer_to_start()
        for r in late_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=True)

        replicas_to_stop = bft_network.random_set_of_replicas(2, late_replicas | {initial_prim})

        # Make sure the system is able to make progress
        for i in range(100):
            await skvbc.write_known_kv()



    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_wedge_command_and_specific_replica_info(self, bft_network):
        """
             Sends a wedge command and check that the system stops from processing new requests.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a wedge command
             2. The client then sends a "Have you stopped" read only command such that each replica answers "I have stopped"
             3. The client validates with the metrics that all replicas have stopped
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.wedge()

        with trio.fail_after(seconds=90):
            done = False
            while done is False:
                await op.wedge_status()
                rsi_rep = client.get_rsi_replies()
                done = True
                for r in rsi_rep.values():
                    res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                    status = res[0].response.stopped
                    if status is False:
                        done = False
                        break

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_wedge_command_where_noops_should_be_sent_in_two_parts(self, bft_network):
        """
            Sends a wedge command on sequence number 300 and check that the system stops from processing new requests.
            this way, when the primary tries to sent noop commands, the working window is reach only to 450.
            Thus, it has to wait for a new stable checkpoint before sending the last 150 noops
            Note: In this test we assume that the batch duration is no
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)

        # bring the system to sequence number 299
        for i in range(299):
            await skvbc.write_known_kv()
            
        # verify that all nodes are in sequence number 299
        not_reached = True
        with trio.fail_after(seconds=30):
            while not_reached:
                not_reached = False
                for r in bft_network.all_replicas():
                    lastExecSeqNum = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
                    if lastExecSeqNum != 299:
                        not_reached = True
                        break

        # now, send a wedge command. The wedge command sequence number is 300. Hence, in this point the woeking window
        # is between 150 - 450. But, the wedge command will make the primary to send noops until 600.
        # we want to verify that the primary manages to send the noops as required.
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.wedge()

        # now, verify that the system has managed to stop
        with trio.fail_after(seconds=90):
            done = False
            while done is False:
                await op.wedge_status()
                rsi_rep = client.get_rsi_replies()
                done = True
                for r in rsi_rep.values():
                    res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                    status = res[0].response.stopped
                    if status is False:
                        done = False
                        break

        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, 2, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, 2)
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_get_latest_pruneable_block(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        # Create 100 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the minimal latest pruneable block among all replicas
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.latest_pruneable_block()

        rsi_rep = client.get_rsi_replies()
        min_prunebale_block = 1000
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            if lpab.response.block_id < min_prunebale_block:
                min_prunebale_block = lpab.response.block_id

        # Create another 100 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the new minimal latest pruneable block
        await op.latest_pruneable_block()

        rsi_rep = client.get_rsi_replies()
        min_prunebale_block_b = 1000
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            if lpab.response.block_id < min_prunebale_block_b:
                min_prunebale_block_b = lpab.response.block_id
        assert min_prunebale_block < min_prunebale_block_b

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_pruning_command(self, bft_network):
        with log.start_action(action_type="test_pruning_command"):
            bft_network.start_all_replicas()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            client = bft_network.random_client()

            # Create 100 blocks in total, including the genesis block we have 101 blocks
            k, v = await skvbc.write_known_kv()
            for i in range(99):
                v = skvbc.random_value()
                await client.write(skvbc.write_req([], [(k, v)], 0))

            # Get the minimal latest pruneable block among all replicas
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            await op.latest_pruneable_block()

            latest_pruneable_blocks = []
            rsi_rep = client.get_rsi_replies()
            for r in rsi_rep.values():
                lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
                latest_pruneable_blocks += [lpab.response]

            await op.prune(latest_pruneable_blocks)
            rsi_rep = client.get_rsi_replies()
            # we expect to have at least 2f + 1 replies
            for rep in rsi_rep:
                r = rsi_rep[rep]
                data = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
                pruned_block = int(data.additional_data.decode('utf-8'))
                assert pruned_block <= 90

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_pruning_status_command(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.prune_status()

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block == 0

        # Create 100 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the minimal latest pruneable block among all replicas
        await op.latest_pruneable_block()

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        await op.prune(latest_pruneable_blocks)

        # Verify the system is able to get new write requests (which means that pruning has done)
        with trio.fail_after(30):
            await skvbc.write_known_kv()

        await op.prune_status()

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block <= 90

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store, num_ro_replicas=1, selected_configs=lambda n, f, c: n == 7)
    async def test_pruning_with_ro_replica(self, bft_network):

        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)

        # Create more than 150 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(200):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Wait for the read only replica to catch with the state
        await self._wait_for_st(bft_network, ro_replica_id, 150)

        # Get the minimal latest pruneable block among all replicas
        await op.latest_pruneable_block()

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        await op.prune(latest_pruneable_blocks)

        # Verify the system is able to get new write requests (which means that pruning has done)
        with trio.fail_after(30):
            await skvbc.write_known_kv()

        await op.prune_status()

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block == 150


    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store, num_ro_replicas=1, selected_configs=lambda n, f, c: n == 7)
    async def test_pruning_with_ro_replica_failure(self, bft_network):

        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)

        # Create more than 150 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(200):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Wait for the read only replica to catch with the state
        await self._wait_for_st(bft_network, ro_replica_id, 150)

        # Get the minimal latest pruneable block among all replicas
        await op.latest_pruneable_block()

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        # Remove the read only latest pruneable block from the list
        for m in latest_pruneable_blocks:
            if m.replica >= bft_network.config.n:
                latest_pruneable_blocks.remove(m)

        assert len(latest_pruneable_blocks) == bft_network.config.n

        # Now, issue a prune request. we expect to receive an error as the read only latest prunebale block is missing
        rep = await op.prune(latest_pruneable_blocks)
        rep = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        assert rep.success is False

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_addRemove_command(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a addRemove command
             2. The client verifies reads the configuration back and verifies the configuration
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()
        client = bft_network.random_client()
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration'
        await op.add_remove(test_config)
        await op.add_remove_status()
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.reconfiguration == test_config

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_addRemoveStatusError(self, bft_network):
        """
             Sends a addRemoveStatus command without adding new configuration 
             and checks that replicas respond with valid error message.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a addRemoveStatus command
             2. The client verifies status returns a valid error message
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()
        client = bft_network.random_client()
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.add_remove_status()
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.error_msg == 'key_not_found'
            assert status.success is False


    async def validate_stop_on_super_stable_checkpoint(self, bft_network, skvbc):
          with log.start_action(action_type="validate_stop_on_super_stable_checkpoint") as action:
            with trio.fail_after(seconds=120):
                for replica_id in range(bft_network.config.n):
                    while True:
                        with trio.move_on_after(seconds=1):
                            try:
                                key = ['replica', 'Gauges', 'OnCallBackOfSuperStableCP']
                                value = await bft_network.metrics.get(replica_id, *key)
                                if value == 0:
                                    action.log(message_type=f"Replica {replica_id} has not reached super stable checkpoint yet")
                                    await trio.sleep(0.5)
                                    continue
                            except trio.TooSlowError:
                                action.log(message_type=
                                    f"Replica {replica_id} was not able to get super stable checkpoint metric within the timeout")
                                raise
                            else:
                                self.assertEqual(value, 1)
                                action.log(message_type=f"Replica {replica_id} has reached super stable checkpoint")
                                break
                with log.start_action(action_type='expect_kv_failure_due_to_wedge'):
                    with self.assertRaises(trio.TooSlowError):
                        await skvbc.write_known_kv()


    async def verify_replicas_are_in_wedged_checkpoint(self, bft_network, previous_checkpoint, replicas):
        with log.start_action(action_type="verify_replicas_are_in_wedged_checkpoint", previous_checkpoint=previous_checkpoint):
            for replica_id in replicas:
                with log.start_action(action_type="verify_replica", replica=replica_id):
                    with trio.fail_after(seconds=60):
                        while True:
                            with trio.move_on_after(seconds=1):
                                checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=replica_id)
                                if checkpoint_after == previous_checkpoint + 2:
                                    break
                                else:
                                    await trio.sleep(1)

    async def verify_last_executed_seq_num(self, bft_network, previous_checkpoint):
        expectedSeqNum = (previous_checkpoint  + 2) * 150
        for r in bft_network.all_replicas():
            lastExecSn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            self.assertEqual(expectedSeqNum, lastExecSn)


    async def validate_state_consistency(self, skvbc, key, val):
        return await skvbc.assert_kv_write_executed(key, val)

    async def _wait_for_st(self, bft_network, ro_replica_id, seqnum_threshold=150):
        # TODO replace the below function with the library function:
        # await tracker.skvbc.tracked_fill_and_wait_for_checkpoint(
        # initial_nodes=bft_network.all_replicas(),
        # num_of_checkpoints_to_add=1)
        with trio.fail_after(seconds=70):
            # the ro replica should be able to survive these failures
            while True:
                with trio.move_on_after(seconds=.5):
                    try:
                        key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                        lastExecutedSeqNum = await bft_network.metrics.get(ro_replica_id, *key)
                    except KeyError:
                        continue
                    else:
                        # success!
                        if lastExecutedSeqNum >= seqnum_threshold:
                            log.log_message(message_type="Replica" + str(ro_replica_id) + " : lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                            break

if __name__ == '__main__':
    unittest.main()
