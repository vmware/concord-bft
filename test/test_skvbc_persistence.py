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

import unittest
import trio
import os.path

import bft_tester
import skvbc
import bft_client
import bft_metrics_client

KEY_FILE_PREFIX = "replica_keys_"

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "3000"

    path = os.path.join(builddir, "bftengine",
            "tests", "simpleKVBCTests", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-p"
            ]

class SkvbcPersistenceTest(unittest.TestCase):
    def setUp(self):
        self.protocol = skvbc.SimpleKVBCProtocol()

    def test_view_change_transitions_safely_without_quorum(self):
        """
        Start up only 5 out of 7 replicas and send client commands. This should
        trigger a succesful view change attempt and trigger the assert in issue
        #194.

        This is a regression test for
        https://github.com/vmware/concord-bft/issues/194.
        """
        trio.run(self._test_view_change_transitions_safely_without_quorum)

    async def _test_view_change_transitions_safely_without_quorum(self):
        config = bft_tester.TestConfig(n=7,
                f=2,
                c=0,
                num_clients=1,
                key_file_prefix=KEY_FILE_PREFIX,
                start_replica_cmd=start_replica_cmd)

        with bft_tester.BftTester(config) as tester:
            await tester.init()
            [tester.start_replica(i) for i in range(1,6)]
            with trio.fail_after(60): # seconds
                async with trio.open_nursery() as nursery:
                    msg = self.protocol.write_req([],
                            [(tester.random_key(), tester.random_value())],
                            0)
                    nursery.start_soon(tester.send_indefinite_write_requests,
                            tester.random_client(), msg)
                    # See if replica 1 has become the new primary
                    # Check every .5 seconds
                    while True:
                        with trio.move_on_after(.5): # seconds
                            key = ['replica', 'Gauges', 'lastAgreedView']
                            replica_id = 1
                            view = await tester.metrics.get(replica_id, *key)
                            if view == 1:
                                break
                    # At this point a view change has successfully completed
                    # with node 1 as the primary. The faulty assertion should
                    # have crashed old nodes.
                    #
                    # In case the nodes didn't crash, stop node 1 to trigger
                    # another view change. Starting node 0 should allow the view
                    # change to succeed. If there is a timeout then the other
                    # nodes have likely crashed due to the faulty assertion. The
                    # crash will show in the logs when running the test
                    # verbosely:
                    #
                    # 21: INFO 2019-08-30skvbc_replica:
                    # /home/andrewstone/concord-bft/bftengine/src/bftengine/PersistentStorageImp.cpp:881:
                    # void
                    # bftEngine::impl::PersistentStorageImp::verifySetDescriptorOfLastExitFromView(const
                    # bftEngine::impl::DescriptorOfLastExitFromView &):
                    # Assertion `false' failed.
                    tester.stop_replica(1)
                    tester.start_replica(0)
                    while True:
                        with trio.move_on_after(.5): # seconds
                            key = ['replica', 'Gauges', 'lastAgreedView']
                            replica_id = 2
                            view = await tester.metrics.get(replica_id, *key)
                            if view == 2:
                                # success!
                                nursery.cancel_scope.cancel()

    def test_read_written_data_after_restart_of_all_nodes(self):
        """
        This test aims to validate the blockchain is persistent
        (i.e. the data is still available after restarting all nodes)
        1) Write a key-value entry to the blockchain
        2) Restart all replicas (stop all, followed by start all)
        3) Verify the same key-value can be read from the blockchain
        """
        trio.run(self._test_read_written_data_after_restart_of_all_nodes)

    async def _test_read_written_data_after_restart_of_all_nodes(self):
        config = bft_tester.TestConfig(n=4,
                                       f=1,
                                       c=0,
                                       num_clients=1,
                                       key_file_prefix=KEY_FILE_PREFIX,
                                       start_replica_cmd=start_replica_cmd)

        with bft_tester.BftTester(config) as tester:
            await tester.init()
            tester.start_all_replicas()

            key = tester.random_key()
            value = tester.random_value()

            kv = (key, value)
            write_kv_msg = self.protocol.write_req([], [kv], 0)

            client = tester.random_client()
            await client.write(write_kv_msg)

            tester.stop_all_replicas()
            tester.start_all_replicas()

            read_key_msg = self.protocol.read_req([key])
            reply = await client.read(read_key_msg)

            kv_reply = self.protocol.parse_reply(reply)

            self.assertEqual({key: value}, kv_reply)

    def test_checkpoints_saved_and_transferred(self):
        """
        Start a 3 nodes out of a 4 node cluster. Write a specific key, then
        enough data to the cluster to trigger a checkpoint and log garbage
        collection. Then stop all the nodes, restart the 3 nodes that should
        have checkpoints as well as the 4th empty node. The 4th empty node
        should catch up via state transfer.

        Then take down one of the original nodes, and try to read the specific
        key.

        After that ensure that a newly put value can be retrieved.
        """
        trio.run(self._test_checkpoints_saved_and_transferred)

    async def _test_checkpoints_saved_and_transferred(self):
        config = bft_tester.TestConfig(n=4,
                                       f=1,
                                       c=0,
                                       num_clients=1,
                                       key_file_prefix=KEY_FILE_PREFIX,
                                       start_replica_cmd=start_replica_cmd)
        with bft_tester.BftTester(config) as tester:
            await tester.init()
            initial_nodes = [0, 1, 2]
            [tester.start_replica(i) for i in initial_nodes]

            p = self.protocol
            client = tester.random_client()

            # Write a KV pair with a known value
            known_key = tester.max_key()
            known_val = tester.random_value()
            kv = [(known_key, known_val)]
            reply = p.parse_reply(await client.write(p.write_req([], kv, 0)))
            self.assertTrue(reply.success)

            # Write enough data to checkpoint and create a need for state transfer
            for i in range (301):
                key = tester.random_key()
                val = tester.random_value()
                msg = self.protocol.write_req([], [(key, val)], 0)
                reply = p.parse_reply(await client.write(msg))
                self.assertTrue(reply.success)

            await tester.assert_state_transfer_not_started_all_up_nodes(self)

            # Wait for initial replicas to take 2 checkpoints (exhausting
            # the full window)
            checkpoint_num = 2;
            await tester.wait_for_replicas_to_checkpoint(initial_nodes,
                    checkpoint_num)
            # Stop the initial replicas to ensure the checkpoints get persisted
            [tester.stop_replica(i) for i in initial_nodes]

            # Bring up the first 3 replicas and ensure that they have the
            # checkpoint data.
            [tester.start_replica(i) for i in initial_nodes]
            await tester.wait_for_replicas_to_checkpoint(initial_nodes,
                    checkpoint_num)

            # Start the replica without any data, and wait for state transfer to
            # complete.
            tester.start_replica(3)
            await tester.wait_for_state_transfer_to_start()
            up_to_date_node = 0
            stale_node = 3
            await tester.wait_for_state_transfer_to_stop(up_to_date_node,
                                                         stale_node)

            # Stop another replica, so that a quorum of replies is forced to
            # include data at the previously stale node
            tester.stop_replica(2)

            # Retrieve the value we put first to ensure state transfer worked
            # when the log went away
            read_req = self.protocol.read_req([known_key])
            kvpairs = self.protocol.parse_reply(await client.read(read_req))
            self.assertDictEqual(dict(kv), kvpairs)

            # Perform a put/get transaction pair to ensure we can read newly
            # written data after state transfer.
            await tester.assert_successful_put_get(self, self.protocol)
