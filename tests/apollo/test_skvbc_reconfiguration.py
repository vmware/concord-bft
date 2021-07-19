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
from shutil import copy2
import trio

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, TestConfig
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
    batch_size = "2" if os.environ.get('TIME_SERVICE_ENABLED') else "1"
    ret.extend(["-b", "2", "-q", batch_size, "-o", builddir + "/operator_pub.pem"])
    return ret

def start_replica_cmd_with_object_store_and_ke(builddir, replica_id, config):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    ret = start_replica_cmd_prefix(builddir, replica_id, config)
    batch_size = "2" if os.environ.get('TIME_SERVICE_ENABLED') else "1"
    ret.extend(["-b", "2", "-q", batch_size, "-e", str(True), "-o", builddir + "/operator_pub.pem"])
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
    batch_size = "2" if os.environ.get('TIME_SERVICE_ENABLED') else "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-b", "2",
            "-q", batch_size,
            "-o", builddir + "/operator_pub.pem"]


def start_replica_cmd_with_key_exchange(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    batch_size = "2" if os.environ.get('TIME_SERVICE_ENABLED') else "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-b", "2",
            "-q", batch_size,
            "-e", str(True),
            "-o", builddir + "/operator_pub.pem"]

class SkvbcReconfigurationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.object_store = ObjectStore()

    @classmethod
    def tearDownClass(cls):
        pass

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True)
    async def test_basic_cre(self, bft_network):
        """
            send a client key exchange message and see the whole flow working in cre
        """
        bft_network.start_all_replicas()
        bft_network.start_cre()

        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        all_client_ids=bft_network.all_client_ids()
        log.log_message(message_type=f"sending client key exchange command for clients {all_client_ids}")
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.client_key_exchange(all_client_ids)
        rep = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        assert rep.success is True
        log.log_message(message_type=f"block_id {rep.response.block_id}")
        await trio.sleep(20)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_key_exchange(self, bft_network):
        """
            No initial key rotation
            Operator sends key exchange command to replica 0
            New keys for replica 0 should get effective at checkpoint 2, i.e. seqnum 300
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
         
        await self.send_and_check_key_exchange(target_replica=0, bft_network=bft_network, client=client)

        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=3, 
                                                 verify_checkpoint_persistency=False)

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_key_exchange, 
                      selected_configs=lambda n, f, c: n == 7,
                      rotate_keys=True)
    async def test_key_exchange_with_restart(self, bft_network):
        """
            - With initial key rotation (keys get effective at checkpoint 2)
            - Reach checkpoint 2 since key cannot be generated twice within a 2 checkpoints window
            - Operator sends key exchange command to replica 1 + validate execution
              (New keys for replica 1 should get effective at checkpoint 4, i.e. seqnum 600)
            - Reach checkpoint 4
            - Stop replica 1
            - Client sends 50 requests
            - Start replica 1
            - Reach checkpoint 6 and validate replica 1 is back on track
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        await self.send_and_check_key_exchange(target_replica=1, bft_network=bft_network, client=client)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        bft_network.stop_replica(1)
        
        for i in range(50):
            await skvbc.write_known_kv()
        key, val = await skvbc.write_known_kv()
        await skvbc.assert_kv_write_executed(key, val)
        
        bft_network.start_replica(1)
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(without={1}), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_key_exchange, 
                      bft_configs=[{'n': 4, 'f': 1, 'c': 0, 'num_clients': 10}],
                      rotate_keys=True)
    async def test_key_exchange_with_file_backup(self, bft_network):
        """
            - With initial key rotation (keys get effective at checkpoint 2)
            - Reach checkpoint 2 since key cannot be generated twice within a 2 checkpoints window
            - Send key exchange command to replica 1 + validate execution
              (New keys for replica 1 should get effective at checkpoint 4, i.e. seqnum 600)
            - Backup the generated private key file at replica 1.
            - Reach checkpoint 4
            - Send key exchange command to replica 1 + validate execution again
            - Stop replica 1
            - Restore backed up file
            - Start replica 1
              (Replica's 1 published public key now doesn't match its private key. If there's no assertion, replica 1
              will send invalid sugnatires and will be malicious)
            - Reach checkpoint 6 and validate all replicas except replica 1 are back on track.
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        await self.send_and_check_key_exchange(target_replica=1, bft_network=bft_network, client=client)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        #backup gen-sec.1 file
        copy2(bft_network.testdir + "/gen-sec.1" , bft_network.testdir + "/gen-sec.1.bak")
        await self.send_and_check_key_exchange(target_replica=1, bft_network=bft_network, client=client)
        bft_network.stop_replica(1)
        # restore gen-sec.1 file
        copy2(bft_network.testdir + "/gen-sec.1.bak" , bft_network.testdir + "/gen-sec.1")
        bft_network.start_replica(1)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(without={1}), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False,
                                                 assert_state_transfer_not_started=False)
        
    async def send_and_check_key_exchange(self, target_replica, bft_network, client):
        with log.start_action(action_type="send_and_check_key_exchange",
                              target_replica=target_replica):
            sent_key_exchange_counter_before = 0
            self_key_exchange_counter_before = 0
            with trio.fail_after(seconds=15):
                try:
                    sent_key_exchange_counter_before = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "sent_key_exchange"])
                    self_key_exchange_counter_before = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                    # public_key_exchange_for_peer_counter_before = await bft_network.metrics.get(0, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                except:
                    log.log_message(message_type=f"Replica {target_replica} was unable to query KeyExchangeMetrics, assuming zero")
                    
            log.log_message(f"sending key exchange command to replica {target_replica}")
            op = operator.Operator(bft_network.config, client, bft_network.builddir)
            await op.key_exchange([target_replica])
            
            with trio.fail_after(seconds=15):
                while True:
                    with trio.move_on_after(seconds=2):
                        try:
                            sent_key_exchange_counter = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "sent_key_exchange"])
                            self_key_exchange_counter = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                            #public_key_exchange_for_peer_counter = await bft_network.metrics.get(0, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                            if  sent_key_exchange_counter == sent_key_exchange_counter_before or \
                                self_key_exchange_counter == self_key_exchange_counter_before:
                                continue
                        except (trio.TooSlowError, AssertionError) as e:
                            log.log_message(message_type=f"{e}: Replica {target_replica} was unable to query KeyExchangeMetrics")
                            raise KeyExchangeError
                        else:
                            assert sent_key_exchange_counter == sent_key_exchange_counter_before + 1
                            assert self_key_exchange_counter == self_key_exchange_counter_before + 1
                            #assert public_key_exchange_for_peer_counter ==  public_key_exchange_for_peer_counter_before + 1
                            break
     
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_client_key_exchange_command(self, bft_network):
        """
            Operator sends client key exchange command for all the clients
        """
        with log.start_action(action_type="test_client_key_exchange_command"):
            bft_network.start_all_replicas()
            client = bft_network.random_client()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            all_client_ids=bft_network.all_client_ids()
            log.log_message(message_type=f"sending client key exchange command for clients {all_client_ids}")
            op = operator.Operator(bft_network.config, client, bft_network.builddir)
            rep = await op.client_key_exchange(all_client_ids)
            rep = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
            assert rep.success is True        
            log.log_message(message_type=f"block_id {rep.response.block_id}")
            assert rep.response.block_id == 1
    
     
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
    async def test_unwedge_command(self, bft_network):
        """
             Sends a wedge command and checks that the system stops processing new requests.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a wedge command
             2. The client verifies that the system reached a super stable checkpoint.
             3. The client tries to initiate a new write bft command and fails
             4. The client sends an unwedge command
             5. The client sends a write request and then reads the written value
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(
            bft_network.config, client,  bft_network.builddir)
        await op.wedge()
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)
        await op.unwedge()

        protocol = kvbc.SimpleKVBCProtocol(bft_network)

        key = protocol.random_key()
        value = protocol.random_value()
        kv_pair = [(key, value)]
        await client.write(protocol.write_req([], kv_pair, 0))

        read_result = await client.read(protocol.read_req([key]))
        value_read = (protocol.parse_reply(read_result))[key]
        self.assertEqual(value, value_read, "A BFT Client failed to read a key-value pair from a "
                         "SimpleKVBC cluster matching the key-value pair it wrote "
                         "immediately prior to the read.")

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

            rep = await op.prune(latest_pruneable_blocks)
            data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
            pruned_block = int(data.additional_data.decode('utf-8'))
            log.log_message(message_type=f"pruned_block {pruned_block}")
            assert pruned_block <= 90   

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_pruning_command_with_failures(self, bft_network):
        with log.start_action(action_type="test_pruning_command_with_faliures"):
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

            # Now, crash one of the non-primary replicas
            crashed_replica = 3
            bft_network.stop_replica(crashed_replica)
            
            rep = await op.prune(latest_pruneable_blocks)
            data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
            pruned_block = int(data.additional_data.decode('utf-8'))
            log.log_message(message_type=f"pruned_block {pruned_block}")
            assert pruned_block <= 90   

            # creates 100 new blocks
            for i in range(100):
                v = skvbc.random_value()
                await client.write(skvbc.write_req([], [(k, v)], 0))

            # now, return the crashed replica and wait for it to done with state transfer
            bft_network.start_replica(crashed_replica)
            await self._wait_for_st(bft_network, crashed_replica, 150)

            # We expect the late replica to catch up with the state and to perform pruning
            with trio.fail_after(seconds=30):
                while True:
                    num_replies = 0
                    await op.prune_status()
                    rsi_rep = client.get_rsi_replies()
                    for r in rsi_rep.values():
                        status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
                        last_prune_blockid = status.response.last_pruned_block
                        if status.response.in_progress is False and last_prune_blockid <= 90 and last_prune_blockid > 0:
                            num_replies += 1
                    if num_replies == bft_network.config.n:
                        break


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
    async def test_restart_command(self, bft_network):
        """
             Send a restart command and verify that replicas have stopped and removed their metadata in two cases
             1. Where all replicas are alive
             2. When we have up to f failures
        """
        # 1. Test without bft
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(500):
            await skvbc.write_known_kv()
        client = bft_network.random_client()
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.restart("hello", bft=False, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        previous_last_exec_num = await bft_network.get_metric(0, bft_network, "Gauges", "lastExecutedSeqNum")
        bft_network.stop_all_replicas()
        bft_network.start_all_replicas()
        for r in bft_network.all_replicas():
            new_exec_sn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            assert(previous_last_exec_num > new_exec_sn)

        # 2. Test with bft
        crashed_replicas = {5, 6} # For simplicity, we crash the last two replicas
        live_replicas = bft_network.all_replicas(without=crashed_replicas)
        bft_network.stop_replicas(crashed_replicas)
        for i in range(500):
            await skvbc.write_known_kv()

        await op.restart("hello2", bft=True, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=False)
        previous_last_exec_num = await bft_network.get_metric(0, bft_network, "Gauges", "lastExecutedSeqNum")
        bft_network.stop_replicas(live_replicas)
        bft_network.start_replicas(live_replicas)
        for r in live_replicas:
            new_exec_sn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            assert(previous_last_exec_num > new_exec_sn)

        # make sure the system is alive
        for i in range(100):
            await skvbc.write_known_kv()

    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True)
    async def test_remove_nodes(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a remove command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Load  a new configuration to the bft network
             4. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()
        key, val = await skvbc.write_known_kv()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        await op.add_remove_with_wedge(test_config, bft=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self.verify_add_remove_status(bft_network, test_config, quorum_all=False)
        await self.verify_restart_ready_proof_msg(bft_network, bft=False)
        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=4,
                   f=1,
                   c=0,
                   num_clients=10,
                   key_file_prefix=KEY_FILE_PREFIX,
                   start_replica_cmd=start_replica_cmd_with_key_exchange,
                   stop_replica_cmd=None,
                   num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        await bft_network.check_initital_key_exchange(stop_replicas=False)

        for r in bft_network.all_replicas():
            last_stable_checkpoint = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
            self.assertEqual(last_stable_checkpoint, 0)
        await self.validate_state_consistency(skvbc, key, val)
        for i in range(100):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            assert( r < 4 )
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store_and_ke, num_ro_replicas=1, rotate_keys=True,
                      selected_configs=lambda n, f, c: n == 7)
    async def test_remove_nodes_with_ror(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a remove command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Wait for read only replica to done with state transfer
             3. Load  a new configuration to the bft network
             4. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
             5. Make sure the read only replica is able to catch up with the new state
         """
        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100): # Produce 149 new blocks
            await skvbc.write_known_kv()
        key, val = await skvbc.write_known_kv()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        await op.add_remove_with_wedge(test_config, bft=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self._wait_for_st(bft_network, ro_replica_id, 300)

        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=4,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_object_store_and_ke,
                          stop_replica_cmd=None,
                          num_ro_replicas=1)
        await bft_network.change_configuration(conf)
        ro_replica_id = bft_network.config.n
        await bft_network.check_initital_key_exchange(stop_replicas=False)
        bft_network.start_replica(ro_replica_id)

        for r in bft_network.all_replicas():
            last_stable_checkpoint = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
            self.assertEqual(last_stable_checkpoint, 0)
        await self.validate_state_consistency(skvbc, key, val)
        for i in range(150):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            assert( r < 4 )
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

        # Wait for the read only replica to catch with the state
        await self._wait_for_st(bft_network, ro_replica_id, 150)

    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True)
    async def test_remove_nodes_with_f_failures(self, bft_network):
        """
        In this test we show how a system operator can remove nodes (and thus reduce the cluster) from 7 nodes cluster
        to 4 nodes cluster even when f nodes are not responding
        For that the operator performs the following steps:
        1. Stop 2 nodes (f=2)
        2. Send a remove_node command - this command also wedges the system
        3. Verify that all live nodes have stopped
        4. Load  a new configuration to the bft network
        5. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        for i in range(100):
            await skvbc.write_known_kv()
        # choose two replicas to crash and crash them
        crashed_replicas = {5, 6} # For simplicity, we crash the last two replicas
        bft_network.stop_replicas(crashed_replicas)
        # All next request should be go through the slow path
        for i in range(100):
            await skvbc.write_known_kv()
        key, val = await skvbc.write_known_kv()
        live_replicas = bft_network.all_replicas(without=crashed_replicas)
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        await op.add_remove_with_wedge(test_config)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, live_replicas)
        expectedSeqNum = (checkpoint_before  + 2) * 150
        for r in live_replicas:
            lastExecSn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            self.assertEqual(expectedSeqNum, lastExecSn)
        await self.validate_stop_on_wedge_point(bft_network, skvbc)
        await self.verify_add_remove_status(bft_network, test_config, quorum_all=False)
        await self.verify_restart_ready_proof_msg(bft_network)
        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=4,
                f=1,
                c=0,
                num_clients=10,
                key_file_prefix=KEY_FILE_PREFIX,
                start_replica_cmd=start_replica_cmd_with_key_exchange,
                stop_replica_cmd=None,
                num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        await bft_network.check_initital_key_exchange(stop_replicas=False)

        for r in bft_network.all_replicas():
            last_stable_checkpoint = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
            self.assertEqual(last_stable_checkpoint, 0)
        await self.validate_state_consistency(skvbc, key, val)
        for i in range(100):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            assert (r < 4)
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True)
    async def test_remove_nodes_with_failures(self, bft_network):
        """
        In this test we show how a system operator can remove nodes (and thus reduce the cluster) from 7 nodes cluster
        to 4 nodes cluster even when f nodes are not responding
        For that the operator performs the following steps:
        1. Stop 2 nodes (f=2)
        2. Send a remove_node command - this command also wedges the system
        3. Verify that all live nodes have stopped
        4. Load  a new configuration to the bft network
        5. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
        """
        crashed_replica = 3
        live_replicas = bft_network.all_replicas(without={crashed_replica})

        bft_network.start_replicas(live_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()

        key, val = await skvbc.write_known_kv()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        await op.add_remove_with_wedge(test_config, False)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, live_replicas)
        expectedSeqNum = (checkpoint_before  + 2) * 150
        for r in live_replicas:
            lastExecSn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            self.assertEqual(expectedSeqNum, lastExecSn)

        # Verify that all live replicas have got to the wedge point
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=False)

        # Start replica 3 and wait for state transfer to finish
        bft_network.start_replica(crashed_replica)
        await self._wait_for_st(bft_network, crashed_replica, 300)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)


        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=4,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_key_exchange,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await bft_network.check_initital_key_exchange(stop_replicas=False)

        for r in bft_network.all_replicas():
            last_stable_checkpoint = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
            self.assertEqual(last_stable_checkpoint, 0)
        await self.validate_state_consistency(skvbc, key, val)
        for i in range(100):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            assert (r < 4)
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd, bft_configs=[{'n': 4, 'f': 1, 'c': 0, 'num_clients': 10}])
    async def test_add_nodes(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a add node command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Load a new configuration to the bft network
             4. Add node is done in phases, (n=4,f=1,c=0)->(n=6,f=1,c=0)->(n=7,f=2,c=0)
                Note: For new replicas to catch up with exiting replicas through ST, existing replicas must
                      move the checkpoint window, that means for n=7 configuration, there must be 5 non-faulty
                      replicas to move the checkpoint window, hence new replicas are added in two phases
             5. Rerun the cluster with only new configuration and make sure they succeed to perform transactions in fast path
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_6_f_1_c_0'
        await op.add_remove_with_wedge(test_config)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self.verify_add_remove_status(bft_network, test_config, quorum_all=False)
        await self.verify_restart_ready_proof_msg(bft_network)
        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=6,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        initial_prim = 0
        new_replicas = {4, 5}
        on_time_replicas = bft_network.all_replicas(without=new_replicas)
        bft_network.start_replicas(on_time_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(151):
            await skvbc.write_known_kv()
        bft_network.start_replicas(new_replicas)
        await bft_network.wait_for_state_transfer_to_start()
        for r in new_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        for i in range(200):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_7_f_2_c_0'
        await op.add_remove_with_wedge(test_config)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self.verify_add_remove_status(bft_network, test_config, quorum_all=False)
        await self.verify_restart_ready_proof_msg(bft_network)
        bft_network.stop_all_replicas()

        conf = TestConfig(n=7,
                          f=2,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        initial_prim = 0
        new_replicas = {6}
        on_time_replicas = bft_network.all_replicas(without=new_replicas)
        bft_network.start_replicas(on_time_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(151):
            await skvbc.write_known_kv()
        bft_network.start_replicas(new_replicas)
        await bft_network.wait_for_state_transfer_to_start()
        for r in new_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        for i in range(300):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
    
    @with_trio
    @with_bft_network(start_replica_cmd, bft_configs=[{'n': 4, 'f': 1, 'c': 0, 'num_clients': 10}])
    async def test_add_nodes_with_failures(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             We add nodes to 4 nodes cluster in phases to make it a 7 node cluster even when f nodes are not responding
             The test does the following:
             1. Stop one node and send a add node command which will also wedge the system on next next checkpoint
             2. Verify that all live nodes have stopped
             3. Load a new configuration to the bft network
             4. Add node is done in phases, (n=4,f=1,c=0)->(n=6,f=1,c=0)->(n=7,f=2,c=0)
                Note: For new replicas to catch up with exiting replicas through ST, existing replicas must
                      move the checkpoint window, that means for n=7 configuration, there must be 5 non-faulty
                      replicas to move the checkpoint window, hence new replicas are added in two phases
             5. Rerun the cluster with only new configuration and make sure they succeed to perform transactions in fast path
         """
        initial_prim = 0
        crashed_replica = bft_network.random_set_of_replicas(1, {initial_prim})
        live_replicas = bft_network.all_replicas(without=crashed_replica)
        bft_network.start_replicas(live_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_6_f_1_c_0'
        await op.add_remove_with_wedge(test_config)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, live_replicas)
        expectedSeqNum = (checkpoint_before  + 2) * 150
        for r in live_replicas:
            lastExecSn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            self.assertEqual(expectedSeqNum, lastExecSn)
        # Verify that all live replicas have got to the wedge point
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=False)
        # Start crashed replica and wait for state transfer to finish
        bft_network.start_replicas(crashed_replica)
        await bft_network.wait_for_state_transfer_to_start()
        for r in crashed_replica:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self.verify_restart_ready_proof_msg(bft_network)
        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=6,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        initial_prim = 0
        new_replicas = {4, 5}
        on_time_replicas = bft_network.all_replicas(without=new_replicas)
        bft_network.start_replicas(on_time_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(151):
            await skvbc.write_known_kv()
        bft_network.start_replicas(new_replicas)
        await bft_network.wait_for_state_transfer_to_start()
        for r in new_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        for i in range(200):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_7_f_2_c_0'
        await op.add_remove_with_wedge(test_config)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self.verify_add_remove_status(bft_network, test_config, quorum_all=False)
        await self.verify_restart_ready_proof_msg(bft_network)
        bft_network.stop_all_replicas()
        conf = TestConfig(n=7,
                          f=2,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        initial_prim = 0
        new_replica = 6
        late_replicas = bft_network.random_set_of_replicas(1, without={initial_prim, new_replica})
        late_replicas.add(new_replica)
        on_time_replicas = bft_network.all_replicas(without=late_replicas)
        bft_network.start_replicas(on_time_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(151):
            await skvbc.write_known_kv()
        bft_network.start_replicas(late_replicas)
        await bft_network.wait_for_state_transfer_to_start()
        for r in late_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        for i in range(300):
            await skvbc.write_known_kv()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
    

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

    async def validate_stop_on_wedge_point(self, bft_network, skvbc, fullWedge=False):
        with log.start_action(action_type="validate_stop_on_stable_checkpoint") as action:
            with trio.fail_after(seconds=90):
                client = bft_network.random_client()
                client.config._replace(req_timeout_milli=10000)
                op = operator.Operator(bft_network.config, client,  bft_network.builddir)
                done = False
                quorum = None if fullWedge is True else bft_client.MofNQuorum.LinearizableQuorum(bft_network.config, [r.id for r in bft_network.replicas])
                while done is False:
                    stopped_replicas = 0
                    await op.wedge_status(quorum=quorum, fullWedge=fullWedge)
                    rsi_rep = client.get_rsi_replies()
                    done = True
                    for r in rsi_rep.values():
                        res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                        status = res[0].response.stopped
                        if status:
                            stopped_replicas += 1
                    stop_condition = bft_network.config.n if fullWedge is True else (bft_network.config.n - bft_network.config.f)
                    if stopped_replicas < stop_condition:
                        done = False
                with log.start_action(action_type='expect_kv_failure_due_to_wedge'):
                    with self.assertRaises(trio.TooSlowError):
                        await skvbc.write_known_kv()
    
    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True)
    async def test_remove_nodes_wo_restart(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a remove command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Validate that replicas don't get restart ready or restart proof messages
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.write_known_kv()
        key, val = await skvbc.write_known_kv()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        await op.add_remove_with_wedge(test_config, bft=False, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await self.verify_add_remove_status(bft_network, test_config, quorum_all=False)
        for r in bft_network.all_replicas():
            restartReadyMsg = await bft_network.get_metric(r, bft_network, "Counters", "receivedRestartReadyMsg")
            restartProofMsg = await bft_network.get_metric(r, bft_network, "Counters", "receivedRestartProofMsg")
            self.assertEqual(restartReadyMsg, 0)
            self.assertEqual(restartProofMsg, 0)


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
    
    async def verify_restart_ready_proof_msg(self, bft_network, bft=True):
        restartProofCount = 0
        required = bft_network.config.n if bft is False else (bft_network.config.n - bft_network.config.f)
        for r in bft_network.all_replicas():
            restartProofMsg = await bft_network.get_metric(r, bft_network, "Counters", "receivedRestartProofMsg")
            if(restartProofMsg > 0):
                restartProofCount += 1
            if(restartProofCount >= required):
                break
        self.assertEqual(required, restartProofCount)
            
    
    async def verify_add_remove_status(self, bft_network, config_descriptor, quorum_all=True ):
        quorum = bft_client.MofNQuorum.All(bft_network.config, [r for r in range(bft_network.config.n)])
        if quorum_all == False:
            quorum = bft_client.MofNQuorum.LinearizableQuorum(bft_network.config, [r.id for r in bft_network.replicas])
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.add_remove_with_wedge_status(quorum)
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.config_descriptor == config_descriptor

        


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
