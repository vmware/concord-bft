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
import random

from util import bft
from util import skvbc as kvbc
from util.skvbc import SimpleKVBCProtocol
from util.skvbc_history_tracker import verify_linearizability
from math import inf

from util.bft import KEY_FILE_PREFIX, with_trio, with_bft_network


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-p"
            ]

class SkvbcReadOnlyReplicaTest(unittest.TestCase):

####################################################################################################################### 
    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd,
                      explicit_config=[{'n': 4, 'f': 1, 'c': 0, 'num_clients': 1, 'num_ro_replicas': 1}])
    @verify_linearizability
    async def test_ro_replica_start_with_delay(self, bft_network, tracker):
        """
        Start up N of N regular replicas.
        Send client commands until checkpoint 1 is reached.
        Start read-only replica.
        Wait for State Transfer in ReadOnlyReplica to complete.
        """
        [bft_network.start_replica(i) for i in range(0, bft_network.config.n)]
        
        with trio.fail_after(60):  # seconds
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.send_indefinite_tracked_ops)
                while True:
                    with trio.move_on_after(.5):  # seconds
                        key = ['replica', 'Gauges', 'lastStableSeqNum']
                        replica_id = 0
                        lastStableSeqNum = await bft_network.metrics.get(replica_id, *key)
                        
                        if lastStableSeqNum >= 150:
                            #enough requests
                            print("Consensus: lastStableSeqNum:" + str(lastStableSeqNum))  
                            nursery.cancel_scope.cancel()
        
        # start the read-only replica
        bft_network.start_replica(4)
        with trio.fail_after(60):  # seconds
                while True:
                    with trio.move_on_after(.5):  # seconds
                        key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                        replica_id = 4
                        lastExecutedSeqNum = await bft_network.metrics.get(replica_id, *key)
                        # success!
                        if lastExecutedSeqNum >= 150:
                            print("Replica 4: lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                            break
####################################################################################################################### 
    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd,
                      explicit_config=[{'n': 4, 'f': 1, 'c': 0, 'num_clients': 1, 'num_ro_replicas': 1}])
    @verify_linearizability
    async def test_ro_replica_start_simultaneously (self, bft_network, tracker):
        """
        Start up N of N regular replicas.
        Start read-only replica.
        Send client commands.
        Wait for State Transfer in ReadOnlyReplica to complete.
        """
        [bft_network.start_replica(i) for i in range(0, bft_network.config.n)]
        # start the read-only replica
        bft_network.start_replica(4)
             
        with trio.fail_after(60):  # seconds
            async with trio.open_nursery() as nursery:
                #nursery.start_soon(self._wait_for_state_transfer_to_complete, bft_network, 150)
                nursery.start_soon(tracker.send_indefinite_tracked_ops)
                while True:
                    with trio.move_on_after(.5):  # seconds
                        key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                        replica_id = 4
                        lastExecutedSeqNum = await bft_network.metrics.get(replica_id, *key)
                        # success!
                        if lastExecutedSeqNum >= 150:
                            print("Replica 4: lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                            nursery.cancel_scope.cancel()          
                