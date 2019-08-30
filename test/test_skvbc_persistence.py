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

