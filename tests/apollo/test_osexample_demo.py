# Concord
#
# Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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
import trio
import subprocess
from os import environ

from util.test_base import ApolloTest
from util import blinking_replica
from util import skvbc as kvbc
from util import eliot_logging as log
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX

"""
Here this test is generally used to test example demo in CI pipeline.
"""
def start_replica_cmd(builddir, replica_id, config):
    return []


class OSExampleDemoTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_osexample_demo(self, bft_network):
        self._run_example_demo(bft_network)


    def _run_example_demo(self, bft_network):
        """
        Run osexample demo script
        """
        with log.start_action(action_type="run osexample demo script"):
            stdout_file = None
            stderr_file = None

            if os.environ.get('KEEP_APOLLO_LOGS', "").lower() in ["true", "on"]:
                test_name = os.environ.get('TEST_NAME')

                if not test_name:
                    now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
                    test_name = f"{now}_{bft_network.current_test}"

                test_dir = f"{bft_network.builddir}/tests/apollo/logs/{test_name}/test_osexample_demo/"
                test_log = f"{test_dir}stdout_osexample_demo.log"
                log.log_message(message_type=f"test log is: {test_log}")
                os.makedirs(test_dir, exist_ok=True)

                stdout_file = open(test_log, 'w+')
                stderr_file = open(test_log, 'w+')

                stdout_file.write("############################################\n")
                stdout_file.flush()
                stderr_file.write("############################################\n")
                stderr_file.flush()

                demo_script_fds = (stdout_file, stderr_file)
                demo_script = os.path.join(bft_network.builddir, "examples", "scripts", "test_osexample.py")
                total_num_replicas = 4
                num_running_clients = 1
                params = [
                        "-bft",
                        "n={0},cl={1}".format(str(total_num_replicas), str(num_running_clients))
                ]

                demo_script_cmd = [demo_script]
                demo_script_cmd.extend(params)
                log.log_message(message_type="starting the script")
                
                demo_script_process = subprocess.Popen(
                    demo_script_cmd,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    close_fds=True)
                try:
                    exit_code = demo_script_process.wait()
                    print("exit_code: ", exit_code)
                    assert exit_code == 0
                except Exception as e:
                    assert False
                finally:
                    for fd in demo_script_fds:
                        fd.close()
