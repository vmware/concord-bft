# Concord
#
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
            else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcControlCommandsTest(unittest.TestCase):
    """
        Sends a wedge command and check that the system stops from processing new requests.
        Note that in this test we assume no failures and synchronized network.
        The test does the following:
        1. A client sends a wedge command
        2. The client verify that the system reached to a super stable checkpoint
        3. The client tries to initiate a new write bft command and fails
    """

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 4 and c == 0)
    async def test_wedge_command(self, bft_network):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        last_bid = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))
        await client.write(skvbc.write_req([], [], block_id=last_bid, wedge_command=True))
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

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
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 4 and c == 0)
    async def test_wedge_command_with_state_transfer(self, bft_network):
        initial_prim = 0
        late_replica = bft_network.random_set_of_replicas(1, {initial_prim})
        on_time_replicas = bft_network.all_replicas(without=late_replica)
        bft_network.start_replicas(on_time_replicas)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        client = bft_network.random_client()
        last_bid = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))
        await client.write(skvbc.write_req([], [], block_id=last_bid, wedge_command=True))

        for replica_id in on_time_replicas:
            with trio.fail_after(seconds=30):
                while True:
                    with trio.move_on_after(seconds=1):
                        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=replica_id)
                        if checkpoint_after == checkpoint_before + 2:
                            break

        bft_network.start_replicas(late_replica)

        await bft_network.wait_for_state_transfer_to_start()
        for r in late_replica:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    async def validate_stop_on_super_stable_checkpoint(self, bft_network, skvbc):
        for replica_id in range(bft_network.config.n):
            with trio.fail_after(seconds=60):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            key = ['replica', 'Gauges', 'OnCallBackOfSuperStableCP']
                            value = await bft_network.metrics.get(replica_id, *key)
                            if value == 0:
                                continue
                        except KeyError:
                            # metrics not yet available, continue looping
                            print(f"KeyError! OnCallBackOfSuperStableCP not yet available.")
                        else:
                            self.assertEqual(value, 1)
                            break
        try:
            await skvbc.write_known_kv()
        except trio.TooSlowError:
            return
        else:
            self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
