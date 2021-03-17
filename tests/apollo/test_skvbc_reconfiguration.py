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
import random
import unittest
from os import environ

import trio

from util import blinking_replica
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, TestConfig
import sys
from util import eliot_logging as log
from util import concord_msgs as cmf_msgs

sys.path.append(os.path.abspath("../../util/pyclient"))

import bft_client


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
            "-q", "1"]


class SkvbcReconfigurationTest(unittest.TestCase):

    def _construct_reconfiguraiton_wedge_coammand(self):
        wedge_cmd = cmf_msgs.WedgeCommand()
        wedge_cmd.stop_seq_num = 0
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = wedge_cmd
        reconf_msg.signature = bytes()
        reconf_msg.additional_data = bytes()
        return reconf_msg

    def _construct_reconfiguraiton_latest_prunebale_block_coammand(self):
        lpab_cmd = cmf_msgs.LatestPrunableBlockRequest()
        lpab_cmd.sender = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = lpab_cmd
        reconf_msg.signature = bytes()
        reconf_msg.additional_data = bytes()
        return reconf_msg

    def _construct_reconfiguraiton_wedge_status(self):
        wedge_status_cmd = cmf_msgs.WedgeStatusRequest()
        wedge_status_cmd.sender = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = wedge_status_cmd
        reconf_msg.signature = bytes()
        reconf_msg.additional_data = bytes()
        return reconf_msg

    def _construct_reconfiguraiton_prune_request(self, latest_pruneble_blocks):
        prune_cmd = cmf_msgs.PruneRequest()
        prune_cmd.sender = 1000
        prune_cmd.latest_prunable_block = latest_pruneble_blocks
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = prune_cmd
        reconf_msg.signature = bytes()
        reconf_msg.additional_data = bytes()
        return reconf_msg

    def _construct_reconfiguraiton_prune_status_request(self):
        prune_status_cmd = cmf_msgs.PruneStatusRequest()
        prune_status_cmd.sender = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = prune_status_cmd
        reconf_msg.signature = bytes()
        reconf_msg.additional_data = bytes()
        return reconf_msg

    from os import environ
    @unittest.skipIf(environ.get('BUILD_COMM_TCP_TLS', "").lower() == "true", "Unstable on CI (TCP/TLS only)")
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
        reconf_msg = self._construct_reconfiguraiton_wedge_coammand()
        await client.write(reconf_msg.serialize(), reconfiguration=True)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

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
        with log.start_action(action_type="send_wedge_cmd",
                              checkpoint_before=checkpoint_before,
                              late_replicas=list(late_replicas)):
            reconf_msg = self._construct_reconfiguraiton_wedge_coammand()
            await client.write(reconf_msg.serialize(), reconfiguration=True)

        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, on_time_replicas)

        bft_network.start_replicas(late_replicas)

        await bft_network.wait_for_state_transfer_to_start()
        for r in late_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=True)
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

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

        reconf_msg = self._construct_reconfiguraiton_wedge_coammand()
        await client.write(reconf_msg.serialize(), reconfiguration=True)

        with trio.fail_after(seconds=90):
            done = False
            while done is False:
                msg = self._construct_reconfiguraiton_wedge_status()
                await client.read(msg.serialize(), m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                    bft_network.config.n)]), reconfiguration=True)
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
        await client.write(skvbc.write_req([], [], block_id=0, wedge_command=True))

        # now, verify that the system has managed to stop
        with trio.fail_after(seconds=90):
            done = False
            while done is False:
                msg = skvbc.get_have_you_stopped_req(n_of_n=1)
                rep = await client.read(msg, m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                    bft_network.config.n)]))
                rsi_rep = client.get_rsi_replies()
                done = True
                for r in rsi_rep.values():
                    if skvbc.parse_rsi_reply(rep, r) == 0:
                        done = False
                        break

        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, 2, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, 2)
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    @unittest.skip("manual testcase - not part of CI")
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_get_latest_prunebable_block(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        # Create 100 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the minimal latest pruneable block among all replicas
        reconf_msg = self._construct_reconfiguraiton_latest_prunebale_block_coammand()
        await client.read(reconf_msg.serialize(),
                                m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                                    bft_network.config.n)]), reconfiguration=True)

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
        reconf_msg = self._construct_reconfiguraiton_latest_prunebale_block_coammand()
        await client.read(reconf_msg.serialize(),
                                m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                                    bft_network.config.n)]), reconfiguration=True)

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

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        # Create 100 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.write_known_kv()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the minimal latest pruneable block among all replicas
        reconf_msg = self._construct_reconfiguraiton_latest_prunebale_block_coammand()
        await client.read(reconf_msg.serialize(), m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                    bft_network.config.n)]), reconfiguration=True)

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        reconf_msg = self._construct_reconfiguraiton_prune_request(latest_pruneable_blocks)
        await client.write(reconf_msg.serialize(), reconfiguration=True)
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            data = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert '90' in data.additional_data.decode('utf-8')

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_pruning_status_command(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        reconf_msg = self._construct_reconfiguraiton_prune_status_request()
        await client.read(reconf_msg.serialize(),
                                m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                                    bft_network.config.n)]), reconfiguration=True)

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
        reconf_msg = self._construct_reconfiguraiton_latest_prunebale_block_coammand()
        await client.read(reconf_msg.serialize(),
                                m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                                    bft_network.config.n)]), reconfiguration=True)

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        reconf_msg = self._construct_reconfiguraiton_prune_request(latest_pruneable_blocks)
        await client.write(reconf_msg.serialize(), reconfiguration=True)

        # Verify the system is able to get new write requests (which means that pruning has done)
        with trio.fail_after(30):
            await skvbc.write_known_kv()

        reconf_msg = self._construct_reconfiguraiton_prune_status_request()
        await client.read(reconf_msg.serialize(),
                          m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                              bft_network.config.n)]), reconfiguration=True)

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block == 90



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

if __name__ == '__main__':
    unittest.main()
