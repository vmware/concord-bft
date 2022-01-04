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

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, with_constant_load
from util.skvbc_history_tracker import verify_linearizability
import util.eliot_logging as log

SKVBC_INIT_GRACE_TIME = 2
BATCH_SIZE = 5
NUM_OF_SEQ_WRITES = 10

def start_replica_cmd(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    The replica is started with a short view change timeout.
    Note each arguments is an element in a list.
    The primary replica is started with a Byzantine Strategy, so it
    will exhibit Byzantine behaviours
    """

    status_timer_milli = "500"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    cmd = [path,
           "-k", KEY_FILE_PREFIX,
           "-i", str(replica_id),
           "-s", status_timer_milli,
           "-v", view_change_timeout_milli,
           "-x"
           ]
    if replica_id == 0 :
        cmd.extend(["-g", "MangledPreProcessResultMsgStrategy"])

    return cmd

def start_replica_cmd_asymmetric_communication(builddir, replica_id, view_change_timeout_milli="10000"):
    """
    Return a command that starts a skvbc replica when passed to subprocess.
    The replica is started with a short view change timeout. Note each argument is an element in a list.
    The primary replica is started with a Byzantine Strategy, so it will exhibit Byzantine behaviours.
    Along with this it will also send different message to different backup replicas. This primary
    will try to mess up the consensus and confuse all the other replicas.
    """

    status_timer_milli = "500"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    cmd = [path,
           "-k", KEY_FILE_PREFIX,
           "-i", str(replica_id),
           "-s", status_timer_milli,
           "-v", view_change_timeout_milli,
           "-x"
           ]
    if replica_id == 0 :
        cmd.extend(["-d", "-g", "MangledPreProcessResultMsgStrategy"])

    return cmd


class SkvbcPrimaryByzantinePreExecutionTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    async def check_viewchange_noexcept(self, bft_network, initial_primary, viewchange_timeout_secs=5):
        """
        Returns : The New Primary
        Description :  When clients start sending requests to a replica system which contain
        a Byzantine Primary, the primary tries to behave as a good primary replica but sends
        wrong messages. The other replica should detect this by analysing the received messages.
        They immediately should request for view change by sending ReplicaAsksToLeaveViewMsg.
        After that if a quorum of replica feels that new primary should be elected, a view change
        will be triggered in the system.
        If clients do not send any message, no one will know if any Byzantine replica is present
        in the system.
        This function should be called after clients start sending message.
        This function will wait for a view change. After the view change it will check the new primary
        and then initialize the pre-execution counts for the new primary and returns the new primary.
        """
        try:
            with trio.move_on_after(seconds=4*viewchange_timeout_secs):
                await bft_network.wait_for_view(
                    replica_id=random.choice(
                        bft_network.all_replicas(without={initial_primary})),
                    expected=lambda v: v != initial_primary,
                    err_msg="Make sure view change has occurred."
                )
                new_primary = await bft_network.get_current_primary()
                await bft_network.init_preexec_count(new_primary)
                await trio.sleep(seconds=viewchange_timeout_secs)
                self.assertNotEqual(initial_primary, new_primary)
                return new_primary
        except trio.TooSlowError:
            return await self.check_viewchange_noexcept(bft_network, initial_primary, viewchange_timeout_secs)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_byzantine_behavior(self, bft_network, tracker):
        """
        Use a random client to launch one batch pre-process request at a time.
        As there is a Byzantine Primary in the replica cluster, view change will
        be triggered.
        Check the view change and then get the new primary and test the counts.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()
        client = bft_network.random_client()

        await skvbc.send_write_kv_set_batch(client, 10, BATCH_SIZE)

        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await skvbc.send_write_kv_set_batch(client, 10, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(new_primary, NUM_OF_SEQ_WRITES * BATCH_SIZE)

    @with_trio
    @with_bft_network(start_replica_cmd_asymmetric_communication, selected_configs=lambda n, f, c: f >= 2)
    @verify_linearizability(pre_exec_enabled=True, no_conflicts=True)
    async def test_byzantine_behavior_with_asymmetric_comm(self, bft_network, tracker):
        """
        Use a random client to launch one batch pre-process request at a time.
        As there is a Byzantine Primary in the replica cluster, which sends different messages
        to different replica, view change will be triggered to elect a new primary.
        Check the view change and then get the new primary and test the counts.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network, tracker)
        bft_network.start_all_replicas()
        await trio.sleep(SKVBC_INIT_GRACE_TIME)
        await bft_network.init_preexec_count()

        client = bft_network.random_client()
        await skvbc.send_write_kv_set_batch(client, 10, BATCH_SIZE)
        new_primary = await self.check_viewchange_noexcept(bft_network, 0, 10)

        for i in range(NUM_OF_SEQ_WRITES):
            client = bft_network.random_client()
            await skvbc.send_write_kv_set_batch(client, 10, BATCH_SIZE)

        await bft_network.assert_successful_pre_executions_count(new_primary, NUM_OF_SEQ_WRITES * BATCH_SIZE)

if __name__ == '__main__':
    unittest.main()
