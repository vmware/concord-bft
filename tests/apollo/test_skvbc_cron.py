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

import trio

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

class SkvbcTestCron(unittest.TestCase):
    __test__ = False  # so that PyTest ignores this test scenario

    period_after_executes_seconds = 7
    expected_component_id = "42"

    @with_trio
    @with_bft_network(start_replica_cmd, rotate_keys=True)
    async def test_cron_action_called_correct_number_of_times(self, bft_network):
        """
        Test that Concord Cron actions are called the correct number of times.

        Instruct TesterReplica to add a cron entry that is executed a specific
        number of times. Verify that number after an additional period of time,
        ensuring that the cron table doesn't execute the entry any more, even
        though ticks generation is proceeding.
        """
        bft_network.start_all_replicas()

        # Since the cron entry is called every second, wait for the number of expected executes + a period after them.
        await trio.sleep(seconds=expected_number_of_executes + self.period_after_executes_seconds)

        for r in bft_network.all_replicas():
          # Make sure the number of executes is exactly as expected and not more, even after an additional period has elapsed.
          num_executes = await bft_network.get_metric(r, bft_network, "Gauges", "cron_entry_number_of_executes", "cron_test")
          self.assertEqual(num_executes, expected_number_of_executes)

          # Make sure the component ID in ticks is correct.
          component_id = await bft_network.get_metric(r, bft_network, "Statuses", "cron_ticks_component_id", "cron_test")
          self.assertEqual(component_id, self.expected_component_id)

if __name__ == '__main__':
    unittest.main()
