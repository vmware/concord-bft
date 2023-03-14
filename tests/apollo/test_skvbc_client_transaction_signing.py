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

import os.path
import random
import unittest
from os import environ

import trio

from util.test_base import ApolloTest
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX

SKVBC_INIT_GRACE_TIME = 5

# Comment for all positive tests:
# The exact number of verification is larger, due to unknown primary + double verification on pre-prepare on unknown-primary
# and the fact that we choose a random client which might have an unknown primary.
# since the "steps" between updates are of 1000 in the source code - we can be sure for now on the exact metric value

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
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-e", str(True)
            ]

class SkvbcTestClientTxnSigning(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    async def setup_skvbc(self, bft_network):
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        return kvbc.SimpleKVBCProtocol(bft_network)

    def writeset(self, skvbc, max_size, keys=None):
        writeset_keys = skvbc.random_keys(
            random.randint(0, max_size)) if keys is None else keys
        writeset_values = skvbc.random_values(len(writeset_keys))
        return list(zip(writeset_keys, writeset_values))

    async def corrupted_write(self, bft_network, skvbc, corrupt_params, client=None, pre_add_valid_write=True):
        assert(len(corrupt_params) > 0 and corrupt_params != None)
        client = bft_network.random_client() if client == None else client
        if pre_add_valid_write:
            read_set = set()
            write_set = self.writeset(skvbc, 2)
            await client.write(skvbc.write_req(read_set, write_set, 0))
        read_set = set()
        write_set = self.writeset(skvbc, 2)
        try:
            await client.write(skvbc.write_req(read_set, write_set, 0), corrupt_params=corrupt_params, no_retries=True)
        except trio.TooSlowError as e:
            pass

    async def corrupted_read(self, bft_network, skvbc, corrupt_params, client=None):
        assert(len(corrupt_params) > 0 and corrupt_params != None)
        client = bft_network.random_client() if client == None else client
        try:
            await client.read(skvbc.get_last_block_req(), corrupt_params=corrupt_params)
        except trio.TooSlowError as e:
            pass

    async def read_n_times(self, bft_network, skvbc, num_reads, client=None):
        for i in range(num_reads):
            client = bft_network.random_client() if client == None else client
            await client.read(skvbc.get_last_block_req())

    async def write_n_times(self, bft_network, skvbc, num_writes, client=None, pre_exec=False):
        for i in range(num_writes):
            client = bft_network.random_client() if client == None else client
            read_set = set()
            write_set = self.writeset(skvbc, 2)
            await client.write(skvbc.write_req(read_set, write_set, 0), pre_process=pre_exec)

    async def send_batch_write_with_pre_execution(self, skvbc, bft_network, num_writes, batch_size, client=None, corrupt_params=None):
        assert num_writes % batch_size == 0, \
            f"num_writes={num_writes} must divide in batch_size={batch_size} without a reminder"
        num_batches = num_writes // batch_size
        client = bft_network.random_client() if client == None else client
        for i in range(num_batches):
            msg_batch = []
            batch_seq_nums = []
            for j in range(batch_size):
                readset = set()
                writeset = self.writeset(skvbc, 2)
                msg_batch.append(skvbc.write_req(readset, writeset, 0, False))
                seq_num = client.req_seq_num.next()
                batch_seq_nums.append(seq_num)
            try:
                no_retries = (corrupt_params != None)
                replies = await client.write_batch(msg_batch, batch_seq_nums, corrupt_params=corrupt_params, no_retries=no_retries)
            except trio.TooSlowError:
                if not corrupt_params:
                    raise trio.TooSlowError
                continue
            assert((replies == None) != (corrupt_params == None))
            if replies:
                for seq_num, reply_msg in replies.items():
                    self.assertTrue(skvbc.parse_reply(
                        reply_msg.get_common_data()).success)

    async def get_metrics(self, bft_network):
        metrics = [{} for _ in range(bft_network.num_total_replicas())]
        for i in bft_network.all_replicas():
            metrics[i]["num_signatures_failed_verification"] = int(await bft_network.get_metric(
                i, bft_network, 'Counters', "external_client_request_signature_verification_failed", "signature_manager"))
            metrics[i]["num_signatures_failed_on_unrecognized_participant_id"] = int(await bft_network.get_metric(
                i, bft_network, 'Counters', "signature_verification_failed_on_unrecognized_participant_id", "signature_manager"))
            metrics[i]["num_signatures_verified"] = int(await bft_network.get_metric(
                i, bft_network, 'Counters', "external_client_request_signatures_verified", "signature_manager"))
            metrics[i]["peer_replicas_signature_verification_failed"] = int(await bft_network.get_metric(
                i, bft_network, 'Counters', "peer_replicas_signature_verification_failed", "signature_manager"))
            metrics[i]["peer_replicas_signatures_verified"] = int(await bft_network.get_metric(
                i, bft_network, 'Counters', "peer_replicas_signatures_verified", "signature_manager"))
        return metrics

    async def assert_metrics(self,
                             bft_network,
                             expected_num_signatures_verified=0,
                             is_expected_signatures_failed_verification=False,
                             is_expected_signatures_failed_on_unrecognized_participant_id=False):

        metrics = await self.get_metrics(bft_network)
        current_primary = await bft_network.get_current_primary()
        for i in bft_network.all_replicas():
            if expected_num_signatures_verified != None:
                if i == current_primary:
                    # Due to unknown primary initially and retransmissions, number of actual verified signatures might be higher than expected
                    assert expected_num_signatures_verified <= metrics[i]["num_signatures_verified"], \
                        f"expected_num_signatures_verified={expected_num_signatures_verified}; actual={metrics[i]['num_signatures_verified']}"
                else:
                    assert expected_num_signatures_verified == metrics[i]["num_signatures_verified"], \
                        f"expected_num_signatures_verified={expected_num_signatures_verified}; actual={metrics[i]['num_signatures_verified']}"

            if is_expected_signatures_failed_verification != None:
                if is_expected_signatures_failed_verification:
                    assert metrics[i]['num_signatures_failed_verification'] > 0, \
                        f"num_signatures_failed_verification={metrics[i]['num_signatures_failed_verification']}"
                else:
                    assert metrics[i]['num_signatures_failed_verification'] == 0, \
                        f"num_signatures_failed_verification={metrics[i]['num_signatures_failed_verification']}"

            if is_expected_signatures_failed_on_unrecognized_participant_id != None:
                if is_expected_signatures_failed_on_unrecognized_participant_id:
                    assert metrics[i]["num_signatures_failed_on_unrecognized_participant_id"] > 0
                else:
                    assert metrics[i]["num_signatures_failed_on_unrecognized_participant_id"] == 0
        return metrics

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_positive_read(self, bft_network):
        """
        In this test, we send 1000 read requests and test happy read path.
        """
        NUM_OF_SEQ_READS = 1000  # This is the minimum amount to update the aggregator
        skvbc = await self.setup_skvbc(bft_network)

        await self.read_n_times(bft_network, skvbc, NUM_OF_SEQ_READS)
        await self.assert_metrics(bft_network, expected_num_signatures_verified=NUM_OF_SEQ_READS)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_positive_write_pre_exec_disabled(self, bft_network):
        """
        In this test, we send 1000 write requests with pre-execution disabled
        and test happy write path.
        """
        NUM_OF_SEQ_WRITES = 1000  # This is the minimum amount to update the aggregator
        skvbc = await self.setup_skvbc(bft_network)

        await self.write_n_times(bft_network, skvbc, NUM_OF_SEQ_WRITES)
        await self.assert_metrics(bft_network, expected_num_signatures_verified=NUM_OF_SEQ_WRITES)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_positive_write_pre_exec_enabled(self, bft_network):
        """
        In this test, we send 1000 write requests with pre-execution enabled
        and test happy write path.
        """
        NUM_OF_SEQ_WRITES = 1000  # This is the minimum amount to update the aggregator
        skvbc = await self.setup_skvbc(bft_network)

        await self.write_n_times(bft_network, skvbc, NUM_OF_SEQ_WRITES, pre_exec=True)
        await self.assert_metrics(bft_network, expected_num_signatures_verified=NUM_OF_SEQ_WRITES)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_positive_write_batching_enabled(self, bft_network):
        """
        In this test, we send 1000 write requests with batching enabled, in batch size of 10,
        and test happy write path.
        """
        NUM_OF_SEQ_WRITES = 1000  # This is the minimum amount to update the aggregator
        skvbc = await self.setup_skvbc(bft_network)

        await self.send_batch_write_with_pre_execution(skvbc, bft_network, NUM_OF_SEQ_WRITES, 10)
        await self.assert_metrics(bft_network, expected_num_signatures_verified=NUM_OF_SEQ_WRITES)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_negative_corrupt_single_request_in_batch(self, bft_network):
        """
        In this test, we send corrupt signature and message, and wrong signature and message lengths,
        in a single batch, and verify metrics accordingly.
        """
        REQUESTS_IN_BATCH = 10
        skvbc = await self.setup_skvbc(bft_network)
        corrupt_dict = {"corrupt_signature": "", "corrupt_msg": "",
                        "wrong_signature_length": "", "wrong_msg_length": ""}
        client = bft_network.random_client()

        num_ver_failed = 0
        for k, v in corrupt_dict.items():
            metrics1 = await self.get_metrics(bft_network)
            await self.send_batch_write_with_pre_execution(
                skvbc, bft_network, REQUESTS_IN_BATCH, REQUESTS_IN_BATCH, client=client, corrupt_params={k: v})
            metrics2 = await self.assert_metrics(
                bft_network, expected_num_signatures_verified=None, is_expected_signatures_failed_verification=True)
            for i in bft_network.all_replicas():
                if k == "corrupt_signature" or k == "corrupt_msg" or k == "wrong_msg_length":
                    assert(metrics1[i]["num_signatures_failed_verification"] + 1 ==
                           metrics2[i]["num_signatures_failed_verification"])
                else:
                    assert(metrics1[i]["num_signatures_failed_verification"] ==
                           metrics2[i]["num_signatures_failed_verification"])
                assert(metrics1[i]["num_signatures_failed_on_unrecognized_participant_id"] ==
                       metrics2[i]["num_signatures_failed_on_unrecognized_participant_id"])
                assert(metrics1[i]["num_signatures_verified"] <=
                       metrics2[i]["num_signatures_verified"])

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_negative_corrupt_signature_and_msg(self, bft_network):
        """
        In this test, we send corrupt signature and message,
        and wrong signature and message lengths, and verify metrics accordingly. 
        """
        skvbc = await self.setup_skvbc(bft_network)
        corrupt_dict = {"corrupt_signature": "", "corrupt_msg": "",
                        "wrong_signature_length": "", "wrong_msg_length": ""}
        client = bft_network.random_client()

        for corrupt_pair in corrupt_dict:
            await self.corrupted_write(bft_network, skvbc, corrupt_pair, client, pre_add_valid_write=False)
            metrics1 = await self.assert_metrics(
                bft_network, expected_num_signatures_verified=None, is_expected_signatures_failed_verification=True)

            await self.write_n_times(bft_network, skvbc, 1, client)

            await self.corrupted_write(bft_network, skvbc, corrupt_pair, client)
            metrics2 = await self.assert_metrics(bft_network,
                                                 expected_num_signatures_verified=None,
                                                 is_expected_signatures_failed_verification=True)

            for i in bft_network.all_replicas():
                assert(metrics1[i]["num_signatures_failed_verification"] <=
                       metrics2[i]["num_signatures_failed_verification"])
                assert(metrics1[i]["num_signatures_failed_on_unrecognized_participant_id"] ==
                       metrics2[i]["num_signatures_failed_on_unrecognized_participant_id"])
                assert(metrics1[i]["num_signatures_verified"] <=
                       metrics2[i]["num_signatures_verified"])

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_negative_wrong_client_id(self, bft_network):
        """
        In this test, we send wrong client ids as replica id and an unkown id,
        and verify metrics accordingly.
        """
        skvbc = await self.setup_skvbc(bft_network)
        client = bft_network.random_client()
        corrupt_dict = {"wrong_client_id_as_replica_id": 0, "wrong_client_id_as_unknown_id": 10000}

        for k, v in corrupt_dict.items():
            await self.write_n_times(bft_network, skvbc, 1, client)

            await self.corrupted_write(bft_network, skvbc, {k:v}, client)
            metrics = await self.assert_metrics(bft_network, expected_num_signatures_verified=None)

            # In both cases earlier checks should filter both and all metrics should not increment
            for i in bft_network.all_replicas():
                assert(metrics[i]["num_signatures_failed_verification"] == 0)
                assert(metrics[i]["num_signatures_failed_on_unrecognized_participant_id"] == 0)
                assert(metrics[i]["num_signatures_verified"] == 0)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_negative_wrong_client_id_as_other_participant_client_id(self, bft_network):
        """
        In this test, we send wrong client ids as client id from another participant,
        and verify metrics accordingly.
        """
        skvbc = await self.setup_skvbc(bft_network)
        # lets get 2 clients with different keys (different participants)
        while (True):
            clients = bft_network.random_clients(2)
            if len(clients) != 2:
                continue
            client, client2 = clients
            if client.signing_key != client2.signing_key:
                break
        assert client.client_id != client2.client_id

        corrupt_dict = {
            "wrong_client_id_as_other_participant_client_id": client2.client_id}

        await self.write_n_times(bft_network, skvbc, 1, client)

        await self.corrupted_write(bft_network, skvbc, corrupt_dict, client)
        metrics = await self.get_metrics(bft_network)

        current_primary = await bft_network.get_current_primary()
        for i in bft_network.all_replicas():
            if i == current_primary:
                assert(metrics[i]["num_signatures_failed_verification"] == 1)
                assert(metrics[i]["num_signatures_verified"] > 0)
            else:
                assert(metrics[i]["num_signatures_failed_verification"] == 0)
                assert(metrics[i]["num_signatures_verified"] == 0)
