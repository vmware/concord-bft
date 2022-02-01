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
import unittest

from util.test_base import ApolloTest
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX

expected_number_of_executes = 3

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    status_timer_milli = "500"
    view_change_timeout_milli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", status_timer_milli,
            "-v", view_change_timeout_milli,
            "-e", str(True),
            "-r", str(expected_number_of_executes)
            ]

class SkvbcTestCron(ApolloTest):
    __test__ = False  # so that PyTest ignores this test scenario

    expected_component_id = "42"
    # Since the cron entry is called every second, wait for the number of expected executes + some grace period
    # to allow for proper custer startup.
    wait_for_timeout_sec = expected_number_of_executes + 30
    wait_for_interval_sec = 1

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_cron_action_called_correct_number_of_times(self, bft_network):
        """
        Test that Concord Cron actions are called the correct number of times.

        Instruct TesterReplica to add a cron entry that is executed a specific
        number of times. Verify that the entry is not executed more times, even
        though ticks generation is proceeding.
        """
        bft_network.start_all_replicas()

        async def wait_for_executes():
          nums = []
          component_ids = []
          for r in bft_network.all_replicas():
            num = await bft_network.retrieve_metric(r, "cron_test", "Gauges", "cron_entry_number_of_executes")
            nums.append(num)

            component_id = await bft_network.retrieve_metric(r, "cron_test", "Statuses", "cron_ticks_component_id")
            component_ids.append(component_id)

          for component_id in component_ids:
            if component_id:
              self.assertEqual(component_id, self.expected_component_id)

          for num in nums:
            if num != expected_number_of_executes:
              return None
          return True

        # Wait for the expected number of executes.
        await bft_network.wait_for(wait_for_executes, self.wait_for_timeout_sec, self.wait_for_interval_sec)

        # Get the last executed seq number of all replicas.
        seq_nums = {}
        for r in bft_network.all_replicas():
          seq_num = await bft_network.wait_for_last_executed_seq_num(r)
          seq_nums[r] = seq_num

        # Wait until all replicas have executed at least one more tick and verify that
        # the entry is not executed anymore.
        for r in bft_network.all_replicas():
          await bft_network.wait_for_last_executed_seq_num(r, seq_nums[r] + 1)
        await bft_network.wait_for(wait_for_executes, self.wait_for_timeout_sec, self.wait_for_interval_sec)

if __name__ == '__main__':
    unittest.main()
