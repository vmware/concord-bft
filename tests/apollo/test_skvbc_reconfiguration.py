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
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
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
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower() in set(["true", "on"]) else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcReconfigurationTest(unittest.TestCase):
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

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        await client.write(skvbc.write_req([], [], block_id=0, wedge_command=True))

        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))

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

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        client = bft_network.random_client()
        await client.write(skvbc.write_req([], [], block_id=0, wedge_command=True))

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

        await client.write(skvbc.write_req([], [], block_id=0, wedge_command=True))

        with trio.fail_after(seconds=60):
            done = False
            while done is False:
                msg = skvbc.get_have_you_stopped_req()
                rep = await client.read(msg, m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                    bft_network.config.n)]))
                rsi_rep = client.get_rsi_replies()
                done = True
                for r in rsi_rep.values():
                    if skvbc.parse_rsi_reply(rep, r) == 0:
                        done = False
                        break

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    async def test_semi_manual_upgrade(self, bft_network):
        """
             Sends a wedge command and check that the system stops from processing new requests.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a wedge command
             2. The client then sends a "Have you stopped" read only command such that each replica answers "I have stopped"
             3. Apollo stops all replicas
             4. Apollo starts all replicas
             5. A client verifies that new write requests are being processed
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        key, val = await skvbc.write_known_kv()
        await client.write(skvbc.write_req([], [], block_id=0, wedge_command=True))

        with trio.fail_after(seconds=60):
            done = False
            while done is False:
                await trio.sleep(seconds=1)
                msg = skvbc.get_have_you_stopped_req()
                rep = await client.read(msg, m_of_n_quorum=bft_client.MofNQuorum.All(client.config, [r for r in range(
                    bft_network.config.n)]))
                rsi_rep = client.get_rsi_replies()
                done = True
                for r in rsi_rep.values():
                    if skvbc.parse_rsi_reply(rep, r) == 0:
                        done = False
                        break

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

        bft_network.stop_all_replicas()

        # Here the system operator runs a manual upgrade
        # input("update the software and press any kay to continue")

        bft_network.start_all_replicas()

        await skvbc.assert_kv_write_executed(key, val)
        await skvbc.write_known_kv()

    async def validate_stop_on_super_stable_checkpoint(self, bft_network, skvbc):
        with trio.fail_after(seconds=120):
            for replica_id in range(bft_network.config.n):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            key = ['replica', 'Gauges', 'OnCallBackOfSuperStableCP']
                            value = await bft_network.metrics.get(replica_id, *key)
                            if value == 0:
                                continue
                        except trio.TooSlowError:
                            print(
                                f"Replica {replica_id} was not able to get to super stable checkpoint within the timeout")
                            self.assertTrue(False)
                        else:
                            self.assertEqual(value, 1)
                            break
            try:
                await skvbc.write_known_kv()
            except trio.TooSlowError:
                return
            else:
                self.assertTrue(False)

    async def verify_replicas_are_in_wedged_checkpoint(self, bft_network, previous_checkpoint, replicas):
        for replica_id in replicas:
            with trio.fail_after(seconds=30):
                while True:
                    with trio.move_on_after(seconds=1):
                        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=replica_id)
                        if checkpoint_after == previous_checkpoint + 2:
                            break


if __name__ == '__main__':
    unittest.main()
