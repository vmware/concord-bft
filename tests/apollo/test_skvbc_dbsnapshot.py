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
import tempfile
from util.test_base import ApolloTest
from util import bft
from util import skvbc as kvbc
from util.skvbc import SimpleKVBCProtocol
from util.skvbc_history_tracker import verify_linearizability
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, DB_FILE_PREFIX, DB_SNAPSHOT_PREFIX, ConsensusPathType
from util import bft_metrics, eliot_logging as log
from util.object_store import ObjectStore, start_replica_cmd_prefix
from util import operator
import concord_msgs as cmf_msgs
import sys

sys.path.append(os.path.abspath("../../util/pyclient"))
import bft_client

TEMP_DB_SNAPSHOT_PREFIX = "TEMP_DB_SNAPSHOT_PREFIX"

DB_CHECKPOINT_WIN_SIZE = 150
DB_CHECKPOINT_HIGH_WIN_SIZE = 4 * DB_CHECKPOINT_WIN_SIZE


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC",
                        "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true":
        time_service_enabled = "1"
    else:
        time_service_enabled = "0"
    batch_size = "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC",
                               "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "3",
            "-j", str(DB_CHECKPOINT_WIN_SIZE),
            "-o", builddir + "/operator_pub.pem"]


def start_replica_cmd_with_high_db_window_size(builddir, replica_id):
    """
    Return a command with operator that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC",
                        "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true":
        time_service_enabled = "1"
    else:
        time_service_enabled = "0"

    batch_size = "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC",
                               "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "3",
            "-j", str(DB_CHECKPOINT_HIGH_WIN_SIZE),
            "-o", builddir + "/operator_pub.pem"]


def start_replica_cmd_db_snapshot_disabled(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC",
                        "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true":
        time_service_enabled = "1"
    else:
        time_service_enabled = "0"

    batch_size = "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC",
                               "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "0",
            "-o", builddir + "/operator_pub.pem"]


def start_replica_cmd_with_operator_and_public_keys(builddir, replica_id):
    """
    Return a command with operator that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC",
                        "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true":
        time_service_enabled = "1"
    else:
        time_service_enabled = "0"

    batch_size = "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC",
                               "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-h", "3",
            "-j", str(DB_CHECKPOINT_HIGH_WIN_SIZE),
            "-o", builddir + "/operator_pub.pem",
            "--add-all-keys-as-public"]


class SkvbcDbSnapshotTest(ApolloTest):
    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_db_checkpoint_creation(self, bft_network, tracker):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in bft_network.all_replicas():
            last_blockId = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(last_blockId, DB_CHECKPOINT_WIN_SIZE)
            bft_network.verify_db_snapshot_is_available(replica_id, last_blockId)
        await self.verify_db_size_on_disk(bft_network)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_restore_from_snapshot(self, bft_network, tracker):
        initial_prim = 0
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        snapshot_id = 0
        for replica_id in bft_network.all_replicas():
            snapshot_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(snapshot_id, DB_CHECKPOINT_WIN_SIZE)
            bft_network.verify_db_snapshot_is_available(replica_id, snapshot_id)
        fast_paths = {}
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.num_of_fast_path_requests(r)
            fast_paths[r] = nb_fast_path
        crashed_replica = list(bft_network.random_set_of_replicas(1, {initial_prim}))
        bft_network.stop_replicas(crashed_replica)
        await skvbc.send_n_kvs_sequentially(3 * DB_CHECKPOINT_WIN_SIZE)

        for r in crashed_replica:
            bft_network.restore_form_older_db_snapshot(snapshot_id, src_replica=r, dest_replicas=[r], prefix=TEMP_DB_SNAPSHOT_PREFIX)
        bft_network.start_replicas(crashed_replica)
        await bft_network.wait_for_state_transfer_to_start()
        for r in crashed_replica:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        await skvbc.send_n_kvs_sequentially(4 * DB_CHECKPOINT_WIN_SIZE)
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.num_of_fast_path_requests(r)
            self.assertGreater(nb_fast_path, fast_paths[r])

    @with_trio
    @with_bft_network(start_replica_cmd_db_snapshot_disabled, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_db_checkpoint_disabled(self, bft_network, tracker):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum = 2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 0)

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
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), checkpoint_after_1 * 150)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        old_snapshot_id = 0
        for r in bft_network.all_replicas():
            old_snapshot_id = await bft_network.last_db_checkpoint_block_id(r)
            bft_network.verify_db_snapshot_is_available(r, old_snapshot_id)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=3,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )
        checkpoint_after_2 = await bft_network.wait_for_checkpoint(replica_id=initial_prim)
        self.assertGreaterEqual(checkpoint_after_1 + 3, checkpoint_after_2)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum = checkpoint_after_2 * 150)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 4)
        for replica_id in bft_network.all_replicas():
           bft_network.verify_db_snapshot_is_available(replica_id, old_snapshot_id, isPresent=False)

    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_create_dbcheckpoint_cmd(self, bft_network, tracker):
        """
            sends a createdbCheckpoint command and test for created dbcheckpoints.
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(200)

        # db checkpoint is created on stable bft-checkpoint
        # we use empty request to fill the checkpoint window
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.create_dbcheckpoint_cmd()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)

        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), 300)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        rep = await op.get_dbcheckpoint_info_request()
        rsi_rep = client.get_rsi_replies()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in rsi_rep.values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 1)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                300 for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in bft_network.all_replicas():
            last_blockId = await bft_network.last_db_checkpoint_block_id(replica_id)
            bft_network.verify_db_snapshot_is_available(replica_id, last_blockId)

    @unittest.skip("Disable until fixed. Unstable test because of BC-17338")
    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_create_dbcheckpoint_with_parallel_client_requests(self, bft_network, tracker):
        """
            sends a createdbCheckpoint command and test for created dbcheckpoints.
            We send empty requests to fill the checkpoint window. In this test, after 
            sending operator command, we continue to send write kv requests and test 
            for db checkpoint creation
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(200)

        # db checkpoint is created on stable bft-checkpoint
        # we use empty request to fill the checkpoint window
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.create_dbcheckpoint_cmd()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        # execute some write kv requests after create command
        # these requests are executed in parallel with empty
        # requests to fill the checkpoint window
        await skvbc.send_n_kvs_sequentially(150)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=300)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        rep = await op.get_dbcheckpoint_info_request()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 1)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                300 for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in bft_network.all_replicas():
            last_blockId = await bft_network.last_db_checkpoint_block_id(replica_id)
            bft_network.verify_db_snapshot_is_available(replica_id, last_blockId)

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
        await skvbc.send_n_kvs_sequentially(2 * DB_CHECKPOINT_WIN_SIZE)

        # There will be 2 dbcheckpoints created on stable seq num 150 and 300.
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum = 2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 2)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.get_dbcheckpoint_info_request(bft=False)
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 2)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                2 * DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in bft_network.all_replicas():
            last_blockId = await bft_network.last_db_checkpoint_block_id(replica_id)
            bft_network.verify_db_snapshot_is_available(replica_id, last_blockId)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_get_dbcheckpoint_info_request_cmd_with_bft(self, bft_network, tracker):
        """
            sends a getdbCheckpointInfoRequest command with bft to test for n-f dbCheckpoint responses from replicas.
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(2 * DB_CHECKPOINT_WIN_SIZE)

        # There will be 2 dbcheckpoints created on stable seq num 150 and 300.
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum = 2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 2)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.get_dbcheckpoint_info_request(bft=False)
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 2)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                2 * DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in bft_network.all_replicas():
            last_blockId = await bft_network.last_db_checkpoint_block_id(replica_id)
            bft_network.verify_db_snapshot_is_available(replica_id, last_blockId)

        # Now, crash one of the non-primary replicas
        crashed_replica = 1
        bft_network.stop_replica(crashed_replica)
        # Make sure the system is able to make progress
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_WIN_SIZE)

        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(without={crashed_replica}), stable_seqnum=450)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(without={crashed_replica}),
                                                     3)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.get_dbcheckpoint_info_request(bft=True)
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        assert (data.success == True)
        for r in client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertEqual(len(res[0].response.db_checkpoint_info), 3)
            assert (any(dbcheckpoint_info.seq_num ==
                        DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
            assert (any(dbcheckpoint_info.seq_num ==
                        2 * DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
            assert (any(dbcheckpoint_info.seq_num ==
                        3 * DB_CHECKPOINT_WIN_SIZE for dbcheckpoint_info in dbcheckpoint_info_list))
        for replica_id in bft_network.all_replicas(without={crashed_replica}):
            last_blockId = await bft_network.last_db_checkpoint_block_id(replica_id)
            bft_network.verify_db_snapshot_is_available(replica_id, last_blockId)

    @with_trio
    @with_bft_network(start_replica_cmd_db_snapshot_disabled, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_when_snapshot_disabled(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum = 2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 0)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.state_snapshot_req()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertFalse(resp.success)
        self.assertEqual(resp.response.error_msg,
                         "StateSnapshotRequest(participant ID = apollo_test_participant_id): failed, the DB checkpoint feature is disabled")

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator_and_public_keys, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_existing_checkpoint_with_public_keys(self, bft_network, tracker):
        await self.state_snapshot_req_existing_checkpoint(bft_network, tracker, DB_CHECKPOINT_HIGH_WIN_SIZE)

    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_existing_checkpoint_without_public_keys(self, bft_network, tracker):
        await self.state_snapshot_req_existing_checkpoint(bft_network, tracker, 0)

    async def state_snapshot_req_existing_checkpoint(self, bft_network, tracker, expected_key_value_count_estimate):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(DB_CHECKPOINT_HIGH_WIN_SIZE):
            key = skvbc.unique_random_key()
            value = skvbc.random_value()
            await skvbc.send_kv_set(client, set(), [(key, value)], 0)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_HIGH_WIN_SIZE)

        # Expect that a snapshot/checkpoint with an ID of 600 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in bft_network.all_replicas():
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(last_block_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)

        # Send a StateSnapshotRequest and make sure we get the already existing checkpoint/snapshot ID of 600.
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.state_snapshot_req()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(resp.success)
        self.assertIsNotNone(resp.response.data)
        self.assertEqual(resp.response.data.snapshot_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
        # TODO: add test for BlockchainHeightType.EventGroupId here (including support for it in TesterReplica).
        self.assertEqual(resp.response.data.blockchain_height, DB_CHECKPOINT_HIGH_WIN_SIZE)
        self.assertEqual(resp.response.data.blockchain_height_type, cmf_msgs.BlockchainHeightType.BlockId)
        self.assertEqual(resp.response.data.key_value_count_estimate, expected_key_value_count_estimate)

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator_and_public_keys, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_non_existent_checkpoint_with_public_keys(self, bft_network, tracker):
        await self.state_snapshot_req_non_existent_checkpoint(bft_network, tracker, 100)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_state_snapshot_req_non_existent_checkpoint_without_public_keys(self, bft_network, tracker):
        await self.state_snapshot_req_non_existent_checkpoint(bft_network, tracker, 0)

    async def state_snapshot_req_non_existent_checkpoint(self, bft_network, tracker, expected_key_value_count_estimate):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(100):
            key = skvbc.unique_random_key()
            value = skvbc.random_value()
            await skvbc.send_kv_set(client, set(), [(key, value)], 0)

        # Make sure no snapshots exist.
        for replica_id in bft_network.all_replicas():
            self.assertFalse(bft_network.db_snapshot_exists(replica_id))

        # Send a StateSnapshotRequest and make sure a new checkpoint/snapshot ID of 100 is created.
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.state_snapshot_req()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(resp.success)
        self.assertIsNotNone(resp.response.data)
        self.assertEqual(resp.response.data.snapshot_id, 100)
        self.assertEqual(resp.response.data.blockchain_height, 100)
        # TODO: add test for BlockchainHeightType.EventGroupId here (including support for it in TesterReplica).
        self.assertEqual(resp.response.data.blockchain_height_type, cmf_msgs.BlockchainHeightType.BlockId)
        self.assertEqual(resp.response.data.key_value_count_estimate, expected_key_value_count_estimate)

        # Expect that a snapshot/checkpoint with an ID of 100 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        last_block_id = 100
        for replica_id in bft_network.all_replicas():
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)

    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    async def test_db_checkpoint_creation_with_wedge(self, bft_network):
        """
            We create a db-snapshot when we wedge the replicas.
            Steps performed in this test:
            1. Configure replicas to create db-snapshot with bft-sequence number window sz = 600
            2. Send a wedge command to wedge replicas at seq number 300
            3. Verify replicas are wedged
            4. Verify db-checkpoints are created on stable sequence number
            5. Unwedge replicas
            6. Write kv requests.
            7. Verify that 2nd db-checkpoint is created at seqNum 600 as a result of configured policy
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(
            bft_network.config, client, bft_network.builddir)
        await op.wedge()
        await bft_network.wait_for_stable_checkpoint( bft_network.all_replicas(), stable_seqnum = (checkpoint_before + 2) * 150)
        await self.validate_stop_on_wedge_point(bft_network, skvbc=skvbc, fullWedge=True)
        # verify that snapshot is created on wedge point
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in bft_network.all_replicas():
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)
        # verify from operatore get db snapshot status command
        rep = await op.get_dbcheckpoint_info_request(bft=False)
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 1)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                300 for dbcheckpoint_info in dbcheckpoint_info_list))
        # unwedge replicas
        await op.unwedge()
        protocol = kvbc.SimpleKVBCProtocol(bft_network)
        key = protocol.random_key()
        value = protocol.random_value()
        kv_pair = [(key, value)]
        await client.write(protocol.write_req([], kv_pair, 0))
        read_result = await client.read(protocol.read_req([key]))
        value_read = (protocol.parse_reply(read_result))[key]
        # verify that un-wedge was successfull
        self.assertEqual(value, value_read, "A BFT Client failed to read a key-value pair from a "
                                            "SimpleKVBC cluster matching the key-value pair it wrote "
                                            "immediately prior to the read.")
        for r in bft_network.all_replicas():
            epoch = await bft_network.get_metric(r, bft_network, "Gauges", "epoch_number", component="epoch_manager")
            self.assertEqual(epoch, 1)
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_HIGH_WIN_SIZE)

        await bft_network.wait_for_stable_checkpoint( bft_network.all_replicas(), stable_seqnum=900)
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 2)
        for replica_id in bft_network.all_replicas():
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)
        # verify from operatore get db snapshot status command
        rep = await op.get_dbcheckpoint_info_request(bft=False)
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        self.assertTrue(data.success)
        for r in client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            self.assertEqual(len(res[0].response.db_checkpoint_info), 2)
            dbcheckpoint_info_list = res[0].response.db_checkpoint_info
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                300 for dbcheckpoint_info in dbcheckpoint_info_list))
            self.assertTrue(any(dbcheckpoint_info.seq_num ==
                                900 for dbcheckpoint_info in dbcheckpoint_info_list))

    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    async def test_scale_and_restart_blockchain_with_db_snapshot(self, bft_network):
        """
             Sends a scale replica command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a scale replica command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Replicas create db snapshot at wedge point
             4. Use db snapshot to restart blockchain
             This test is equivalent to starting a new blockchain with db snapshot
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.send_n_kvs_sequentially(100)
        key, val = await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        test_config = 'new_configuration'
        await op.add_remove_with_wedge(test_config, bft=False, restart=False)

        # verify that snapshot is created on wedge point
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in bft_network.all_replicas():
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)
        bft_network.stop_all_replicas()
        # restore all replicas using snapshot created in replica id=0
        bft_network.restore_form_older_db_snapshot(last_block_id, src_replica=0,
                                         dest_replicas=bft_network.all_replicas(), prefix=TEMP_DB_SNAPSHOT_PREFIX)
        # replicas will clean metadata and start a new blockchain
        bft_network.start_all_replicas()
        await skvbc.send_n_kvs_sequentially(100)

        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.num_of_fast_path_requests(r)
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_signed_public_state_hash_req_existing_checkpoint(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_HIGH_WIN_SIZE)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_HIGH_WIN_SIZE)

        # Expect that a snapshot/checkpoint with an ID of 600 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in bft_network.all_replicas():
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(last_block_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        ser_resp = await op.signed_public_state_hash_req(DB_CHECKPOINT_HIGH_WIN_SIZE)
        ser_rsis = op.get_rsi_replies()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_resp)[0]
        self.assertTrue(resp.success)
        replica_ids = set()
        signatures = set()
        for ser_rsi in ser_rsis.values():
            rsi_resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_rsi)[0]
            self.assertEqual(rsi_resp.response.status, cmf_msgs.SnapshotResponseStatus.Success)
            self.assertEqual(rsi_resp.response.data.snapshot_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            self.assertEqual(rsi_resp.response.data.block_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            # Expect the SHA3-256 hash of the empty string.
            empty_string_sha3_256 = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
            self.assertEqual(bytearray(rsi_resp.response.data.hash), empty_string_sha3_256)
            replica_ids.add(rsi_resp.response.data.replica_id)
            signatures.add(rsi_resp.response.signature)
        # Make sure the replica IDs and the signatures are unique.
        self.assertEqual(len(replica_ids), len(bft_network.all_replicas()))
        self.assertEqual(len(signatures), len(bft_network.all_replicas()))

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_signed_public_state_hash_req_non_existent_checkpoint(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        ser_resp = await op.signed_public_state_hash_req(42)
        ser_rsis = op.get_rsi_replies()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_resp)[0]
        self.assertTrue(resp.success)
        for ser_rsi in ser_rsis.values():
            rsi_resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_rsi)[0]
            self.assertEqual(rsi_resp.response.status, cmf_msgs.SnapshotResponseStatus.SnapshotNonExistent)

    @with_trio
    @with_bft_network(start_replica_cmd_with_high_db_window_size, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_signed_state_snapshot_read_as_of_req_without_public_keys(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        kvs = []
        for i in range(DB_CHECKPOINT_HIGH_WIN_SIZE):
            key = skvbc.unique_random_key()
            value = skvbc.random_value()
            kvs.append((key.decode(), value.decode()))
            await skvbc.send_kv_set(client, set(), [(key, value)], 0)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_HIGH_WIN_SIZE)

        # Expect that a snapshot/checkpoint with an ID of 600 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in range(len(bft_network.all_replicas())):
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(last_block_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        # Try to read two of the keys that we wrote. We shouldn't be able to get them, though, because they are not public.
        ser_resp = await op.state_snapshot_read_as_of_req(DB_CHECKPOINT_HIGH_WIN_SIZE, [kvs[0][0], kvs[1][0]])
        ser_rsis = op.get_rsi_replies()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_resp)[0]
        self.assertTrue(resp.success)
        for ser_rsi in ser_rsis.values():
            rsi_resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_rsi)[0]
            self.assertEqual(rsi_resp.response.status, cmf_msgs.SnapshotResponseStatus.Success)
            self.assertEqual(len(rsi_resp.response.values), 2)
            self.assertEqual(rsi_resp.response.values[0], None)
            self.assertEqual(rsi_resp.response.values[1], None)

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator_and_public_keys, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_signed_state_snapshot_read_as_of_req_with_public_keys(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        kvs = []
        for i in range(DB_CHECKPOINT_HIGH_WIN_SIZE):
            key = skvbc.unique_random_key()
            value = skvbc.random_value()
            kvs.append((key.decode(), value.decode()))
            await skvbc.send_kv_set(client, set(), [(key, value)], 0)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_HIGH_WIN_SIZE)

        # Expect that a snapshot/checkpoint with an ID of 600 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in range(len(bft_network.all_replicas())):
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(last_block_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        # Read two of the keys that we wrote.
        ser_resp = await op.state_snapshot_read_as_of_req(DB_CHECKPOINT_HIGH_WIN_SIZE, [kvs[0][0], kvs[1][0]])
        ser_rsis = op.get_rsi_replies()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_resp)[0]
        self.assertTrue(resp.success)
        for ser_rsi in ser_rsis.values():
            rsi_resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_rsi)[0]
            self.assertEqual(rsi_resp.response.status, cmf_msgs.SnapshotResponseStatus.Success)
            self.assertEqual(len(rsi_resp.response.values), 2)
            self.assertEqual(rsi_resp.response.values[0], kvs[0][1])
            self.assertEqual(rsi_resp.response.values[1], kvs[1][1])

    @with_trio
    @with_bft_network(start_replica_cmd_with_operator_and_public_keys, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_signed_state_snapshot_read_as_of_req_invalid_key_with_public_keys(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        for i in range(DB_CHECKPOINT_HIGH_WIN_SIZE):
            key = skvbc.unique_random_key()
            value = skvbc.random_value()
            await skvbc.send_kv_set(client, set(), [(key, value)], 0)
        await bft_network.wait_for_stable_checkpoint(bft_network.all_replicas(), stable_seqnum=DB_CHECKPOINT_HIGH_WIN_SIZE)

        # Expect that a snapshot/checkpoint with an ID of 600 is available. For that, we assume that the snapshot/checkpoint ID
        # is the last block ID at which the snapshot/checkpoint is created.
        await bft_network.wait_for_created_db_snapshots_metric(bft_network.all_replicas(), 1)
        for replica_id in range(len(bft_network.all_replicas())):
            last_block_id = await bft_network.last_db_checkpoint_block_id(replica_id)
            self.assertEqual(last_block_id, DB_CHECKPOINT_HIGH_WIN_SIZE)
            await bft_network.wait_for_db_snapshot(replica_id, last_block_id)

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        # Read two keys that we haven't written.
        ser_resp = await op.state_snapshot_read_as_of_req(DB_CHECKPOINT_HIGH_WIN_SIZE,
                                                          [skvbc.unique_random_key().decode(),
                                                           skvbc.unique_random_key().decode()])
        ser_rsis = op.get_rsi_replies()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_resp)[0]
        self.assertTrue(resp.success)
        for ser_rsi in ser_rsis.values():
            rsi_resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_rsi)[0]
            self.assertEqual(rsi_resp.response.status, cmf_msgs.SnapshotResponseStatus.Success)
            self.assertEqual(len(rsi_resp.response.values), 2)
            self.assertEqual(rsi_resp.response.values[0], None)
            self.assertEqual(rsi_resp.response.values[1], None)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_signed_state_snapshot_read_as_of_req_non_existent_checkpoint(self, bft_network, tracker):
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        ser_resp = await op.state_snapshot_read_as_of_req(42, [])
        ser_rsis = op.get_rsi_replies()
        resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_resp)[0]
        self.assertTrue(resp.success)
        for ser_rsi in ser_rsis.values():
            rsi_resp = cmf_msgs.ReconfigurationResponse.deserialize(ser_rsi)[0]
            self.assertEqual(rsi_resp.response.status, cmf_msgs.SnapshotResponseStatus.SnapshotNonExistent)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @verify_linearizability()
    async def test_restore_from_snapshot_of_other(self, bft_network, tracker):
        initial_prim = 0
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_WIN_SIZE)  # get to a checkpoint

        crashed_replica = list(bft_network.random_set_of_replicas(1, {initial_prim}))[0]
        bft_network.stop_replica(crashed_replica)
        await skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_WIN_SIZE)  # run till the next checkpoint

        src_replica = list(bft_network.random_set_of_replicas(1, without={crashed_replica}))[0]
        await bft_network.wait_for_stable_checkpoint({src_replica}, stable_seqnum = 2 * DB_CHECKPOINT_WIN_SIZE)
        await bft_network.wait_for_created_db_snapshots_metric({src_replica}, 2)
        snapshot_id = await bft_network.last_db_checkpoint_block_id(src_replica)
        bft_network.verify_db_snapshot_is_available(src_replica, snapshot_id)
        bft_network.restore_form_older_db_snapshot(snapshot_id, src_replica=src_replica,
                                         dest_replicas=[crashed_replica], prefix=TEMP_DB_SNAPSHOT_PREFIX)
        bft_network.reset_metadata(crashed_replica)
        await skvbc.send_n_kvs_sequentially(3 * DB_CHECKPOINT_WIN_SIZE)

        bft_network.start_replica(crashed_replica)
        await bft_network.wait_for_state_transfer_to_start()
        await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                          crashed_replica,
                                                          stop_on_stable_seq_num=False)
        # make sure that the restored replica participates in consensus
        await bft_network.wait_for_consensus_path(path_type=ConsensusPathType.OPTIMISTIC_FAST,
                                                  run_ops=lambda: skvbc.send_n_kvs_sequentially(DB_CHECKPOINT_WIN_SIZE),
                                                  threshold=5)

    async def validate_stop_on_wedge_point(self, bft_network, skvbc, fullWedge=False):
        with log.start_action(action_type="validate_stop_on_stable_checkpoint") as action:
            with trio.fail_after(seconds=90):
                client = bft_network.random_client()
                client.config._replace(req_timeout_milli=10000)
                op = operator.Operator(bft_network.config, client, bft_network.builddir)
                done = False
                quorum = None if fullWedge is True else bft_client.MofNQuorum.LinearizableQuorum(bft_network.config,
                                                                                                 [r.id for r in
                                                                                                  bft_network.replicas])
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
                    stop_condition = bft_network.config.n if fullWedge is True else (
                            bft_network.config.n - bft_network.config.f)
                    if stopped_replicas < stop_condition:
                        done = False

    async def verify_db_size_on_disk(self, bft_network):
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        resp = await op.get_db_size()
        rep = cmf_msgs.ReconfigurationResponse.deserialize(resp)[0]
        assert rep.success is True
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            replica_id = res[0].response.replica_id
            assert len(res[0].response.mapCheckpointIdDbSize) > 0
            for checkPtId,db_size in res[0].response.mapCheckpointIdDbSize:
                db_dir = os.path.join(
                    bft_network.testdir, DB_FILE_PREFIX + str(replica_id))
                if checkPtId != 0 :
                    db_dir = os.path.join(
                        bft_network.testdir, DB_SNAPSHOT_PREFIX + str(replica_id) + "/" + str(checkPtId))
                size = 0
                for element in os.scandir(db_dir):
                    if element.is_file():
                        size += os.path.getsize(element)
                assert size == db_size      


