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
import socket
import subprocess
import json
import os.path
import time

class MetricsSeverTest(unittest.TestCase):
    """
    Test that a metric server with empty metrics responds correclty to UDP
    requests.
    """

    def setUp(self):
        self.server_path = os.path.abspath("../../build/util/test/metric_server")
        self.server = subprocess.Popen([self.server_path], close_fds=True)
        self.server_addr = ("127.0.0.1", 6161)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(.05)

    def tearDown(self):
        self.server.kill()
        self.server.wait()
        self.sock.close()

    def sendAndReceive(self, request):
       """
       Retry until we recv a message. This waits for the server to come up.
       """
       count = 0
       while count < 100: # 5 seconds
           try:
               count += 1
               self.sock.sendto(request, self.server_addr)
               reply, _ = self.sock.recvfrom(1024)
               return reply
           except:
               pass

    def testSuccess(self):
       """ Send a valid request and wait for a correct reply """
       request = bytearray()
       request.append(0) # requests only consist of single byte 0
       reply = self.sendAndReceive(request)
       self.assertEqual(1, reply[0])
       metrics = json.loads(reply[1:])
       self.assertEqual([], metrics['Components'])

    def testFailure(self):
       """ Send an invalid request and wait for an error reply """
       request = b'hello'
       reply = self.sendAndReceive(request)
       self.assertEqual(2, reply[0])
