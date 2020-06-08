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
from util.bft import with_trio, with_bft_network, with_constant_load, KEY_FILE_PREFIX


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "20000"
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


class SkvbcTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_checkpoint_creation(self, bft_network):
        """
        Test the creation of checkpoints (independently of state transfer or view change)

        Start all replicas, then send a sufficient number of client requests to trigger the
        checkpoint protocol. Then make sure a checkpoint is created and agreed upon by all replicas.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes=bft_network.all_replicas(),
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )
        checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=0)

        self.assertEqual(checkpoint_after, 1 + checkpoint_before)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_state_transfer(self, bft_network):
        """
        Test that state transfer starts and completes.

        Stop one node, add a bunch of data to the rest of the cluster, restart
        the node and verify state transfer works as expected. In a 4 node
        cluster with f=1 we should be able to stop a different node after state
        transfer completes and still operate correctly.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        stale_node = random.choice(
            bft_network.all_replicas(without={0}))

        await skvbc.prime_for_state_transfer(
            stale_nodes={stale_node},
            persistency_enabled=False
        )
        bft_network.start_replica(stale_node)
        await bft_network.wait_for_state_transfer_to_start()
        await bft_network.wait_for_state_transfer_to_stop(0, stale_node)
        await skvbc.assert_successful_put_get(self)
        random_replica = random.choice(
            bft_network.all_replicas(without={0, stale_node}))
        bft_network.stop_replica(random_replica)
        await skvbc.assert_successful_put_get(self)

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_get_block_data_with_blinking_replica(self, bft_network):
        """
        Test that the cluster continues working when one blinking replica
        By a blinking replic we mean a replica that goes up and down for random
        period of time
        """
        with blinking_replica.BlinkingReplica() as blinking:
            br = random.choice(
                bft_network.all_replicas(without={0}))
            bft_network.start_replicas(replicas=bft_network.all_replicas(without={br}))
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)

            blinking.start_blinking(bft_network.start_replica_cmd(br))

            for _ in range(300):
                # Perform an unconditional KV put.
                # Ensure keys aren't identical
                await skvbc.read_your_writes(self)


    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_get_block_data(self, bft_network):
        """
        Ensure that we can put a block and use the GetBlockData API request to
        retrieve its KV pairs.
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        last_block = skvbc.parse_reply(await client.read(skvbc.get_last_block_req()))

        # Perform an unconditional KV put.
        # Ensure keys aren't identical
        kv = [(skvbc.keys[0], skvbc.random_value()),
              (skvbc.keys[1], skvbc.random_value())]

        reply = await client.write(skvbc.write_req([], kv, 0))
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        last_block = reply.last_block_id

        # Get the kvpairs in the last written block
        data = await client.read(skvbc.get_block_data_req(last_block))
        kv2 = skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))

        # Write another block with the same keys but (probabilistically)
        # different data
        kv3 = [(skvbc.keys[0], skvbc.random_value()),
               (skvbc.keys[1], skvbc.random_value())]
        reply = await client.write(skvbc.write_req([], kv3, 0))
        reply = skvbc.parse_reply(reply)
        self.assertTrue(reply.success)
        self.assertEqual(last_block + 1, reply.last_block_id)

        # Get the kvpairs in the previously written block
        data = await client.read(skvbc.get_block_data_req(last_block))
        kv2 = skvbc.parse_reply(data)
        self.assertDictEqual(kv2, dict(kv))

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_conflicting_write(self, bft_network):
        """
        The goal is to validate that a conflicting write request does not
        modify the blockchain state. Verifying this can be done as follows:
        1) write a key K several times with different values
        (to ensure several versions in the blockchain)
        2) create a (conflicting) conditional write on K'
        based on an old version of K
        3) execute the conflicting write
        4) verify K' is not written to the blockchain
        """
        bft_network.start_all_replicas()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        key = skvbc.random_key()

        write_1 = skvbc.write_req(
            readset=[],
            writeset=[(key, skvbc.random_value())],
            block_id=0)

        write_2 = skvbc.write_req(
            readset=[],
            writeset=[(key, skvbc.random_value())],
            block_id=0)

        client = bft_network.random_client()

        await client.write(write_1)
        last_write_reply = \
            skvbc.parse_reply(await client.write(write_2))

        last_block_id = last_write_reply.last_block_id

        key_prime = skvbc.random_key()

        # this write is conflicting because the writeset (key_prime) is
        # based on an outdated version of the readset (key)
        conflicting_write = skvbc.write_req(
            readset=[key],
            writeset=[(key_prime, skvbc.random_value())],
            block_id=last_block_id - 1)

        write_result = \
            skvbc.parse_reply(await client.write(conflicting_write))
        successful_write = write_result.success

        self.assertTrue(not successful_write)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7)
    @with_constant_load
    async def test_delayed_replicas_start_up(self, bft_network, skvbc, nursery):
        """
        The goal is to make sure that if replicas are started in a random
        order, with delays in-between, and with constant load (request sent every 1s),
        the BFT network eventually stabilizes its view and correctly processes requests
        from this point onwards.
        1) Start sending constant requests, even before the BFT network is started
        2) Create a random replica start order
        3) Use the order defined above to start all replicas with delays in-between
        4) Make sure at least one view change was agreed (because not enough replicas for quorum initially)
        5) Make sure the agreed view is stable after all replicas are started
        6) Make sure the system correctly processes requests in the new view
        """

        replicas_starting_order = bft_network.all_replicas()
        random.shuffle(replicas_starting_order)

        initial_view = 0
        try:
            # Delayed replica start-up...
            for r in replicas_starting_order:
                bft_network.start_replica(r)
                await trio.sleep(seconds=10)

            current_view = await bft_network.wait_for_view(
                replica_id=0,
                expected=lambda v: v > initial_view,
                err_msg="Make sure view change has occurred during the delayed replica start-up."
            )

            client = bft_network.random_client()
            current_block = skvbc.parse_reply(
                await client.read(skvbc.get_last_block_req()))

            with trio.move_on_after(seconds=60):
                while True:
                    # Make sure the current view is stable
                    await bft_network.wait_for_view(
                        replica_id=0,
                        expected=lambda v: v == current_view,
                        err_msg="Make sure view is stable after all replicas are started."
                    )
                    await trio.sleep(seconds=5)

                    # Make sure the system is processing requests
                    last_block = skvbc.parse_reply(
                        await client.read(skvbc.get_last_block_req()))
                    self.assertTrue(last_block > current_block)
                    current_block = last_block

            current_primary = current_view % bft_network.config.n
            non_primaries = bft_network.all_replicas(without={current_primary})
            restarted_replica = random.choice(non_primaries)
            print(f"Restart replica #{restarted_replica} to make sure its view is persistent...")

            bft_network.stop_replica(restarted_replica)
            await trio.sleep(seconds=5)
            bft_network.start_replica(restarted_replica)

            await trio.sleep(seconds=20)

            # Stop sending requests, and make sure the restarted replica
            # is up-and-running and participates in consensus
            nursery.cancel_scope.cancel()

            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
            last_executed_seq_num = await bft_network.metrics.get(current_primary, *key)

            with trio.fail_after(seconds=30):
                while True:
                    last_executed_seq_num_restarted = await bft_network.metrics.get(restarted_replica, *key)
                    if last_executed_seq_num_restarted >= last_executed_seq_num:
                        break

        except Exception as e:
            print(f"Delayed replicas start-up failed for start-up order: {replicas_starting_order}")
            raise e
        else:
            print(f"Delayed replicas start-up succeeded for start-up order: {replicas_starting_order}. "
                  f"The BFT network eventually stabilized in view #{current_view}.")

if __name__ == '__main__':
    unittest.main()
