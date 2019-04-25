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

# This code requires python 3.5 or later

import unittest
import subprocess
import os.path
import trio

from config import Replica
from metrics_client import MetricsClient

class MetricsClientTest(unittest.TestCase):
    """
    Test that we can send and get a response from the MetricsServer wich
    contains empty metrics.
    """

    def setUp(self):
        self.server_path = os.path.abspath("../../build/util/test/metric_server")
        self.server = subprocess.Popen([self.server_path], close_fds=True)
        self.replicas = [Replica(0, "127.0.0.1", 6161)]

    def tearDown(self):
        self.server.kill()
        self.server.wait()

    def testGet(self):
        trio.run(self._testGet)

    async def _testGet(self):
        with MetricsClient(self.replicas) as client:
            metrics = await client.get(self.replicas[0].id)
            self.assertEqual([], metrics['Components'])

if __name__ == '__main__':
    unittest.main()
