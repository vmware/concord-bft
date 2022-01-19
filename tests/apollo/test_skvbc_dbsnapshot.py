# Concord
#
# Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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
import time
import difflib
import subprocess
import shutil
from util import bft
from util import skvbc as kvbc
from util.skvbc import SimpleKVBCProtocol
from util.skvbc_history_tracker import verify_linearizability
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, DB_FILE_PREFIX, DB_SNAPSHOT_PREFIX
from util import bft_metrics, eliot_logging as log
from util.object_store import ObjectStore, start_replica_cmd_prefix, with_object_store
from util import operator
import concord_msgs as cmf_msgs
import sys
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
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        batch_size = "2"
        time_service_enabled = "1"
    else :
        batch_size = "1"
        time_service_enabled = "0"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "3",
            "-j", "150",
            "-o", builddir + "/operator_pub.pem"]

def start_replica_cmd_with_operator(builddir, replica_id):
    """
    Return a command with operator that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        batch_size = "2"
        time_service_enabled = "1"
    else :
        batch_size = "1"
        time_service_enabled = "0"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "3",
            "-j", "600",
            "-o", builddir + "/operator_pub.pem"]

def start_replica_cmd_db_snapshot_disabled(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        batch_size = "2"
        time_service_enabled = "1"
    else :
        batch_size = "1"
        time_service_enabled = "0"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "0",
            "-o", builddir + "/operator_pub.pem"]

class SkvbcDbSnapshotTest(unittest.TestCase):
    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_db_checkpoint_creation(self, bft_network, tracker):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(150): 
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 150)
        await self.wait_for_created_snapshots_metric(bft_network, 1)
        for replica_id in range(len(bft_network.all_replicas())):
            last_blockId =  await bft_network.get_metric(replica_id, bft_network, 
                "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
            self.assertEqual(last_blockId, 150)
            self.verify_snapshot_is_available(bft_network, replica_id, last_blockId)
    
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_restore_from_snapshot(self, bft_network, tracker):
        initial_prim = 0
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(150): 
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 150)
        await self.wait_for_created_snapshots_metric(bft_network, 1)
        snapshot_id = 0
        for replica_id in range(len(bft_network.all_replicas())):
            snapshot_id = await bft_network.get_metric(replica_id, bft_network, 
                "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
            self.assertEqual(snapshot_id, 150)
            self.verify_snapshot_is_available(bft_network, replica_id, snapshot_id)
        fast_paths = {}
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            fast_paths[r] = nb_fast_path
        crashed_replica = list(bft_network.random_set_of_replicas(1, {initial_prim}))
        bft_network.stop_replicas(crashed_replica)
        for i in range(450): 
            await skvbc.send_write_kv_set()
        for r in crashed_replica:
            self.restore_form_older_snapshot(bft_network, r, snapshot_id)
        bft_network.start_replicas(crashed_replica)
        await bft_network.wait_for_state_transfer_to_start()
        for r in crashed_replica:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                                r,
                                                               stop_on_stable_seq_num=False)
        for i in range(600):
            await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
                nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
                self.assertGreater(nb_fast_path, fast_paths[r])

    @with_trio
    @with_bft_network(start_replica_cmd_db_snapshot_disabled, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_db_checkpoint_disabled(self, bft_network, tracker):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(300): 
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 300)
        await self.wait_for_created_snapshots_metric(bft_network, 0)
    
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_db_checkpoint_cleanup(self, bft_network, tracker):
        '''
        In this test, we verify that oldest db checkpoint is removed once,
        we reach the maxNumber of allowed db checkpoints
        '''
        initial_prim = 0
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=initial_prim)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )
        checkpoint_after_1 = await bft_network.wait_for_checkpoint(replica_id=initial_prim)
        self.assertGreaterEqual(checkpoint_before + 1, checkpoint_after_1)
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), checkpoint_after_1*150)
        await self.wait_for_created_snapshots_metric(bft_network, 1)
        old_snapshot_id = 0
        for r in bft_network.all_replicas():
            old_snapshot_id = await bft_network.get_metric(r, bft_network, 
                "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
            self.verify_snapshot_is_available(bft_network, r, old_snapshot_id)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=3,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )
        checkpoint_after_2 = await bft_network.wait_for_checkpoint(replica_id=initial_prim)
        self.assertGreaterEqual(checkpoint_after_1 + 3, checkpoint_after_2)
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), checkpoint_after_2*150)
        await self.wait_for_created_snapshots_metric(bft_network, 4)
        for replica_id in range(len(bft_network.all_replicas())):
            self.verify_snapshot_is_available(bft_network, replica_id, old_snapshot_id, isPresent=False)

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_create_dbcheckpoint_cmd(self, bft_network, tracker):
        """
            sends a createdbCheckpoint command and test for created dbcheckpoints.
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(200):
            await skvbc.send_write_kv_set()
        # db checkpoint is created on stable bft-checkpoint
        # we use empty request to fill the checkpoint window
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.create_dbcheckpoint_cmd()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)

        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 300)
        await self.wait_for_created_snapshots_metric(bft_network, 1)
        rep = await op.get_dbcheckpoint_info_request()
        rsi_rep = client.get_rsi_replies()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in rsi_rep.values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 1)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num == 300 for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in range(len(bft_network.all_replicas())):
            last_blockId =  await bft_network.get_metric(replica_id, bft_network, 
                "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
            self.verify_snapshot_is_available(bft_network, replica_id, last_blockId)
    
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_get_dbcheckpoint_info_request_cmd(self, bft_network, tracker):
        """
            sends a getdbCheckpointInfoRequest command and test for created dbcheckpoints.
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(300): 
            await skvbc.send_write_kv_set()
        # There will be 2 dbcheckpoints created on stable seq num 150 and 300.
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 300)
        await self.wait_for_created_snapshots_metric(bft_network, 2)
        
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.get_dbcheckpoint_info_request()
        rsi_rep = client.get_rsi_replies()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in rsi_rep.values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 2)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num == 150 for dbcheckpoint_info in dbcheckpoint_info_list))
            self.assertTrue(any(dbcheckpoint_info.seq_num == 300 for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in range(len(bft_network.all_replicas())):
            last_blockId =  await bft_network.get_metric(replica_id, bft_network, 
                "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
            self.verify_snapshot_is_available(bft_network, replica_id, last_blockId)

    @with_trio
    @with_bft_network(start_replica_cmd_db_snapshot_disabled, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_when_snapshot_disabled(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(300): 
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 300)
        await self.wait_for_created_snapshots_metric(bft_network, 0)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.state_snapshot_req()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertFalse(resp.success)
        self.assertEqual(resp.response.error_msg,
            "StateSnapshotRequest(participant ID = apollo_test_participant_id): failed, the DB checkpoint feature is disabled")

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_existing_checkpoint(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(600):
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 600)
        
        # Expect that a snapshot/checkpoint with an ID of 600 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        await self.wait_for_created_snapshots_metric(bft_network, 1)
        for replica_id in range(len(bft_network.all_replicas())):
            last_block_id = await bft_network.get_metric(replica_id, bft_network, 
                "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
            self.assertEqual(last_block_id, 600)
            await self.wait_for_snapshot(bft_network, replica_id, last_block_id)

        # Send a StateSnapshotRequest and make sure we get the already existing checkpoint/snapshot ID of 600
        # and an event group ID of 0.
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.state_snapshot_req()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(resp.success)
        self.assertIsNotNone(resp.response.data)
        self.assertEqual(resp.response.data.snapshot_id, 600)
        self.assertEqual(resp.response.data.event_group_id, 0)

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_non_existent_checkpoint(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(100):
            await skvbc.send_write_kv_set()

        # Make sure no snapshots exist.
        for replica_id in range(len(bft_network.all_replicas())):
            self.assertFalse(self.snapshot_exists(bft_network, replica_id))

        # Send a StateSnapshotRequest and make sure a new checkpoint/snapshot ID of 100 is created.
        # Expect an event group ID of 0.
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.state_snapshot_req()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(resp.success)
        self.assertIsNotNone(resp.response.data)
        self.assertEqual(resp.response.data.snapshot_id, 100)
        self.assertEqual(resp.response.data.event_group_id, 0)

        # Expect that a snapshot/checkpoint with an ID of 100 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        last_block_id = 100
        for replica_id in range(len(bft_network.all_replicas())):
            await self.wait_for_snapshot(bft_network, replica_id, last_block_id)

    def verify_snapshot_is_available(self, bft_network, replica_id, snapshot_id, isPresent=True):
        with log.start_action(action_type="verify snapshot db files"):
            snapshot_db_dir = os.path.join(bft_network.testdir, DB_SNAPSHOT_PREFIX + str(replica_id) + "/" + str(snapshot_id))
            if isPresent == True:
                self.assertTrue(self.snapshot_exists(bft_network, replica_id, snapshot_id))
            else:
                self.assertFalse(os.path.exists(snapshot_db_dir))

    def snapshot_exists(self, bft_network, replica_id, snapshot_id=None):
      with log.start_action(action_type="snapshot_exists()"):
          snapshot_db_dir = os.path.join(bft_network.testdir, DB_SNAPSHOT_PREFIX + str(replica_id))
          if snapshot_id is not None:
              snapshot_db_dir = os.path.join(snapshot_db_dir, str(snapshot_id))
          if not os.path.exists(snapshot_db_dir):
            return False

          # Make sure that checkpoint folder is not empty.
          size = 0
          for element in os.scandir(snapshot_db_dir):
              size += os.path.getsize(element)
          return (size > 0)

    async def wait_for_snapshot(self, bft_network, replica_id, snapshot_id=None):
        with trio.fail_after(seconds=30):
            while True:
                if self.snapshot_exists(bft_network, replica_id, snapshot_id) == True:
                    break
                await trio.sleep(0.5)

    def restore_form_older_snapshot(self, bft_network, replica, snapshot_id):
        with log.start_action(action_type="restore with older snapshot"):
            snapshot_db_dir = os.path.join(bft_network.testdir, DB_SNAPSHOT_PREFIX + str(replica) + "/" + str(snapshot_id))
            dest_db_dir = os.path.join(bft_network.testdir, DB_FILE_PREFIX + str(replica))
            if os.path.exists(dest_db_dir) :
                    shutil.rmtree(dest_db_dir)
            ret = shutil.copytree(snapshot_db_dir, dest_db_dir) 
            log.log_message(message_type=f"copy db files from {snapshot_db_dir} to {dest_db_dir}, result is {ret}")
         

    def transfer_dbcheckpoint_files(self, bft_network, source_replica, snapshot_id, dest_replicas):
        with log.start_action(action_type="transfer snapshot db files"):
            snapshot_db_dir = os.path.join(bft_network.testdir, DB_SNAPSHOT_PREFIX + str(source_replica) + "/" + str(snapshot_id))
            for r in dest_replicas:
                dest_db_dir = os.path.join(bft_network.testdir, DB_FILE_PREFIX + str(r))
                if os.path.exists(dest_db_dir) :
                    shutil.rmtree(dest_db_dir)
                ret = shutil.copytree(snapshot_db_dir, dest_db_dir) 
                log.log_message(message_type=f"copy db files from {snapshot_db_dir} to {dest_db_dir}, result is {ret}")

    async def wait_for_stable_checkpoint(self, bft_network, replicas, stable_seqnum):
        with trio.fail_after(seconds=30):
            all_in_checkpoint = False
            while all_in_checkpoint is False:
                all_in_checkpoint = True
                for r in replicas:
                    lastStable = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
                    if lastStable != stable_seqnum:
                        all_in_checkpoint = False
                        break
                await trio.sleep(0.5)

    async def wait_for_created_snapshots_metric(self, bft_network, expected_num_of_created_snapshots):
      with trio.fail_after(seconds=30):
          while True:
              found_mismatch = False
              for replica_id in range(len(bft_network.all_replicas())):
                  num_of_created_snapshots = await bft_network.get_metric(replica_id, bft_network,
                      "Counters", "numOfDbCheckpointsCreated", component="rocksdbCheckpoint") 
                  if num_of_created_snapshots != expected_num_of_created_snapshots:
                      found_mismatch = True
                      break
              if found_mismatch:
                  await trio.sleep(0.5)
              else:
                  break
