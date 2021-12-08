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
            "-h", "3"]

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
            "-h", "0"]

class SkvbcDbSnapshotTest(unittest.TestCase):
    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_db_checkpoint_creation(self, bft_network, tracker):

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(150): 
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 150)
        num_of_db_snapshots =  await bft_network.get_metric(0, bft_network, "Counters", "numOfDbCheckpointsCreated", component="rocksdbCheckpoint")
        assert num_of_db_snapshots == 1
        last_blockId =  await bft_network.get_metric(0, bft_network, "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
        self.verify_snapshot_is_available(bft_network, 0, last_blockId)
    
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
        num_of_db_snapshots =  await bft_network.get_metric(0, bft_network, "Counters", "numOfDbCheckpointsCreated", component="rocksdbCheckpoint")
        assert num_of_db_snapshots == 1
        snapshot_id = await bft_network.get_metric(0, bft_network, "Gauges", "lastDbCheckpointBlockId", component="rocksdbCheckpoint")
        self.verify_snapshot_is_available(bft_network, 0, snapshot_id)
        fast_paths = {}
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            fast_paths[r] = nb_fast_path
        crashed_replica = list(bft_network.random_set_of_replicas(1, {initial_prim}))
        live_replicas =  bft_network.all_replicas(without=set(crashed_replica))
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
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(300): 
            await skvbc.send_write_kv_set()
        await self.wait_for_stable_checkpoint(bft_network, bft_network.all_replicas(), 300)
        num_of_db_snapshots =  await bft_network.get_metric(0, bft_network, "Counters", "numOfDbCheckpointsCreated", component="rocksdbCheckpoint")
        assert num_of_db_snapshots == 0


    def verify_snapshot_is_available(self, bft_network, replicaId, shapshotId):
        with log.start_action(action_type="verify snapshot db files"):
            snapshot_db_dir = os.path.join(bft_network.testdir, DB_SNAPSHOT_PREFIX + str(replicaId) + "/" + str(shapshotId))
            assert os.path.exists(snapshot_db_dir)
            size=0
            for ele in os.scandir(snapshot_db_dir):
                size+=os.path.getsize(ele)
            assert (size > 0) #make sure that checkpoint folder is not empty

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