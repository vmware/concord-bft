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
import time

import trio

from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util.skvbc_history_tracker import verify_linearizability
from util import eliot_logging as log
from util import skvbc as kvbc

FILE_PATH_PREFIX = "/tmp/fake_clock_"
FILE_PATH_SUFFIX = ".config"
CLOCK_NO_DRIFT = '0'
CLOCK_DRIFT = '5000' #milliseconds

def start_replica_cmd(builddir, replica_id, time_service_enabled='1'):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.
    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "3000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path, 
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-e", str(True),
            "-v", viewChangeTimeoutMilli,
            "-f", time_service_enabled 
            ]

def start_replica_cmd_without_time_service(disable_time_service):
    def wrapper(*args, **kwargs):
        return start_replica_cmd(*args, **kwargs, time_service_enabled=disable_time_service)
    return wrapper

class SkvbcTimeServiceTest(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    def setUp(self):
        # Whenever a replica goes down, all messages initially go via the slow path.
        # However, when an "evaluation period" elapses (set at 64 sequence numbers),
        # the system should return to the fast path.
        self.evaluation_period_seq_num = 64

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: c == 0 and n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_wrong_time_in_primary(self, bft_network, tracker):
        """
        1. Launch a cluster
        2. Make sure that the cluster works in the fast path mode
        3. Change local time in the primary's container so that it would be behind or beyond non-primary's local time
        4. Expected result: View Change due to a large amount of dropped PrePrepare messages
        5. Change time in the primary's container so that it is the same as in other replicas
        6. Expected result: The cluster should switch to the fast path
        """
        
        n = bft_network.config.n
        initial_primary = 0
        expected_next_primary = 1

        for replica_id in range(n):
            path = FILE_PATH_PREFIX + str(replica_id) + FILE_PATH_SUFFIX
            await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)
        
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

        path = FILE_PATH_PREFIX + str(initial_primary) + FILE_PATH_SUFFIX
        await self.manipulate_time_file_write(path, CLOCK_DRIFT) 
        
        await skvbc.run_concurrent_ops(400)

        # wait for replicas to go to higher view (View 1 in this case)
        await bft_network.wait_for_replicas_to_reach_view(bft_network.all_replicas(), 1)

        await bft_network.wait_for_view(
            replica_id=random.choice(
            bft_network.all_replicas()),
            expected=lambda v: v == expected_next_primary,
            err_msg="Make sure view change has happened"
        )
        
        await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20) 

    @with_trio
    @with_bft_network(start_replica_cmd,
                      selected_configs=lambda n, f, c: c == 0 and n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_wrong_time_in_non_primary(self, bft_network, tracker):
        """
        1. Launch a cluster
        2. Make sure that the cluster works in the fast path mode
        3. Change local time in one of the non-primary's container so that it would be behind or beyond other replica's local time
        4. Expected result: The cluster should switch to the slow path
        5. Change time in the non-primary's container so that it is the same as in other replicas
        6. Expected result: The cluster should switch to the fast path
        """
        
        n = bft_network.config.n
        initial_primary = 0
        
        for replica_id in range(n):
            path = FILE_PATH_PREFIX + str(replica_id) + FILE_PATH_SUFFIX
            await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)
        
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

        non_primary_replica = random.choice(
            bft_network.all_replicas(without={initial_primary})) 
        
        path = FILE_PATH_PREFIX + str(non_primary_replica) + FILE_PATH_SUFFIX 
        await self.manipulate_time_file_write(path, CLOCK_DRIFT)
        
        await bft_network.wait_for_slow_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)
        
        await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20) 

    @with_trio
    @with_bft_network(start_replica_cmd_without_time_service('0'),
                      selected_configs=lambda n, f, c: c == 0 and n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_wrong_time_in_primary_without_ts(self, bft_network, tracker):
        """
        1. Launch a cluster
        2. Make sure that the cluster works in the fast path mode
        3. Change local time in the primary's container so that it would be behind or beyond non-primary's local time
        4. Expected result: Cluster should continue to work in fast path as time service is disabled
        5. Change time in the primary's container so that it is the same as in other replicas
        6. Expected result: No effect, Cluster should continue to work in fast path
        """
        n = bft_network.config.n
        initial_primary = 0

        for replica_id in range(n):
            path = FILE_PATH_PREFIX + str(replica_id) + FILE_PATH_SUFFIX
            await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)
        
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)

        path = FILE_PATH_PREFIX + str(initial_primary) + FILE_PATH_SUFFIX

        initial_view = await bft_network.get_current_view()
        await self.manipulate_time_file_write(path, CLOCK_DRIFT) 
        
        await skvbc.run_concurrent_ops(400)

        current_view = await bft_network.get_current_view()
        self.assertEqual(initial_view, current_view, "Make sure view change does not happen")
        
        await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20) 

    @with_trio
    @with_bft_network(start_replica_cmd_without_time_service('0'),
                      selected_configs=lambda n, f, c: c == 0 and n >= 6, rotate_keys=True)
    @verify_linearizability()
    async def test_wrong_time_in_non_primary_without_ts(self, bft_network, tracker): 
        """
        1. Launch a cluster
        2. Make sure that the cluster works in the fast path mode
        3. Change local time in one of the non-primary's container so that it would be behind or beyond other replica's local time
        4. Expected result: Cluster should continue to work in fast path as time service is disabled
        5. Change time in the non-primary's container so that it is the same as in other replicas
        6. Expected result: No effect, Cluster should continue to work in fast path
        """
        n = bft_network.config.n
        initial_primary = 0
        
        for replica_id in range(n):
            path = FILE_PATH_PREFIX + str(replica_id) + FILE_PATH_SUFFIX
            await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)
        
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network,tracker)
        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)
        initial_view = await bft_network.get_current_view()

        non_primary_replica = random.choice(
            bft_network.all_replicas(without={initial_primary})) 
        
        path = FILE_PATH_PREFIX + str(non_primary_replica) + FILE_PATH_SUFFIX 
        await self.manipulate_time_file_write(path, CLOCK_DRIFT)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20)  

        await self.manipulate_time_file_write(path, CLOCK_NO_DRIFT)

        await bft_network.wait_for_fast_path_to_be_prevalent(
            run_ops=lambda: skvbc.run_concurrent_ops(num_ops=20, write_weight=1), threshold=20) 

    @classmethod
    async def manipulate_time_file_write(self, path, data):
        with open(path, 'w') as fp:
            fp.write(str(data))
            fp.close()
