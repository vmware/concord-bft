# Concord
#
# Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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
import os
import subprocess

from minio import Minio
from util.test_base import ApolloTest
from util import skvbc as kvbc
from util import eliot_logging as log
from util.object_store import ObjectStore, start_replica_cmd_prefix
from util.bft import KEY_FILE_PREFIX, with_trio, with_bft_network

def start_replica_cmd(builddir, replica_id, config):
    """
    There are two test s3 config files. The one used here hasn't got s3-prefix parameter
    set its value will be an empty string.
    The effect of this is that on each RO replica start we have got empty S3 storage.
    This is the default behaviour for most of the tests.
    """
    return start_replica_cmd_prefix(builddir, replica_id, config)


class SkvbcS3IntegrityCheckTest(ApolloTest):
    """
    Integrity Check has got two modes of operation:
        - validate all
        - validate key
    """

    @classmethod
    def setUpClass(cls):
        cls.object_store = ObjectStore()

    @classmethod
    def tearDownClass(cls):
        pass

    @with_trio
    @unittest.skip("unstable scenario")
    @with_bft_network(start_replica_cmd=start_replica_cmd_prefix, num_ro_replicas=1, selected_configs=lambda n, f, c: n == 7)
    async def test_integrity_check_validate_all(self, bft_network):
        """
        Start all replicas including ROR
        Generate a checkpoint
        start integrity
        check that integrity was verified
        This test is executed only in S3 mode.
        """
        if not os.environ.get("CONCORD_BFT_MINIO_BINARY_PATH"):
            return

        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n

        bft_network.start_replica(ro_replica_id)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )

        await self._wait_for_st(bft_network, ro_replica_id)

        keys_config = f"{KEY_FILE_PREFIX}{ro_replica_id}"
        s3_config = "test_s3_config_prefix.txt"
        self._start_integrity_check(bft_network, keys_config, s3_config)

    @with_trio
    @unittest.skip("unstable scenario")
    @with_bft_network(start_replica_cmd=start_replica_cmd_prefix, num_ro_replicas=1, selected_configs=lambda n, f, c: n == 7)
    async def test_integrity_check_validate_all_with_missing_block(self, bft_network):
        """
        Start all replicas including ROR
        Generate a checkpoint
        start integrity
        delete block
        verify that integrity check failed
        This test is executed only in S3 mode.
        """
        if not os.environ.get("CONCORD_BFT_MINIO_BINARY_PATH"):
            return

        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n

        bft_network.start_replica(ro_replica_id)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            num_of_checkpoints_to_add=1,
            verify_checkpoint_persistency=False,
            assert_state_transfer_not_started=False
        )

        await self._wait_for_st(bft_network, ro_replica_id)
        self._delete_block()
        keys_config = f"{KEY_FILE_PREFIX}{ro_replica_id}"
        s3_config = "test_s3_config_prefix.txt"
        try:
            self._start_integrity_check(bft_network, keys_config, s3_config)
            assert False
        except AssertionError as e:
            pass

    def _start_integrity_check(self, bft_network, keys_file, s3_config_file, key_to_validate=None):
        """
        Start integrity check
        """
        with log.start_action(action_type="start_integrity_check"):
            stdout_file = None
            stderr_file = None

            if os.environ.get('KEEP_APOLLO_LOGS', "").lower() in ["true", "on"]:
                test_name = os.environ.get('TEST_NAME')

                if not test_name:
                    now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
                    test_name = f"{now}_{bft_network.current_test}"

                test_dir = f"{bft_network.builddir}/tests/apollo/logs/{test_name}/{bft_network.current_test}/"
                test_log = f"{test_dir}stdout_integrity_check.log"
                log.log_message(message_type=f"test log is: {test_log}")
                os.makedirs(test_dir, exist_ok=True)

                stdout_file = open(test_log, 'w+')
                stderr_file = open(test_log, 'w+')

                stdout_file.write("############################################\n")
                stdout_file.flush()
                stderr_file.write("############################################\n")
                stderr_file.flush()

                s3_config_path = os.path.join(bft_network.builddir, s3_config_file)
                integrity_check_fds = (stdout_file, stderr_file)
                integrity_check_exe = os.path.join(bft_network.builddir, "kvbc", "tools", "object_store_utility", "object_store_utility")
                integrity_check_cmd = [integrity_check_exe,
                                       "--keys-file", keys_file,
                                       "--s3-config-file", s3_config_path]
                integrity_check_cmd.append("validate")
                if key_to_validate is not None:
                    integrity_check_cmd.append("--key")
                    integrity_check_cmd.append(key_to_validate)
                else:
                    integrity_check_cmd.append("--all")
                log.log_message(message_type="starting the subprocess")
                integrity_check_pid = subprocess.Popen(
                    integrity_check_cmd,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    close_fds=True)
                try:
                    exit_code = integrity_check_pid.wait()
                    assert exit_code == 0
                except Exception as e:
                    assert False
                finally:
                    for fd in integrity_check_fds:
                        fd.close()

    def _delete_block(self):
        client = Minio("127.0.0.1:9000", access_key="concordbft", secret_key="concordbft", secure=False)
        buckets = client.list_buckets()
        bucketName = buckets[0].name
        try:
            client.remove_object(bucketName, 'concord/blocks/1')
        except ResponseError as err:
            assert False

    async def _wait_for_st(self, bft_network, ro_replica_id, seqnum_threshold=150):
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
