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
import struct
import tempfile
import shutil
import os
import os.path
import subprocess
import trio

import bft_client
import bft_config
from functools import wraps


def verify_system_is_ready(async_fn):
    """ Decorator for running a coroutine (async_fn) with trio. """
    @wraps(async_fn)
    async def sys_ready_wrapper(*args, **kwargs):
        config = bft_config.Config(4, 1, 0, 4096, 10000, 500, "")
        class_object = args[0]
        with bft_client.UdpClient(config, class_object.replicas, None) as udp_client:
            await udp_client.sendSync(class_object.writeRequest(1989), False)
        return await async_fn(*args, **kwargs)
    return sys_ready_wrapper

# This requires python 3.5 for subprocess.run
class SimpleTest(unittest.TestCase):
    """
    Test a UDP client against simpleTest servers

    Use n=4, f=1, c=0
    """
    @classmethod
    def startServers(cls):
        """Start all 4 simpleTestServers"""
        cls.procs = [subprocess.Popen([cls.serverbin, str(i)], close_fds=True)
                      for i in range(0, 4)]
    @classmethod
    def stopServers(cls):
        """Stop all processes in self.procs"""
        for p in cls.procs:
            p.terminate()
            p.wait()

    @classmethod
    def setUpClass(cls):
        cls.origdir = os.getcwd()
        cls.testdir = tempfile.mkdtemp()
        cls.builddir = os.path.abspath("../../build")
        cls.toolsdir = os.path.join(cls.builddir, "tools")
        cls.serverbin = os.path.join(cls.builddir,"tests/simpleTest/server")
        os.chdir(cls.testdir)
        cls.generateKeys()
        cls.config = bft_config.Config(4, 1, 0, 4096, 1000, 50, "")
        cls.replicas = [bft_config.Replica(id=i,
                                           ip="127.0.0.1",
                                           port=bft_config.bft_msg_port_from_node_id(i),
                                           metrics_port=bft_config.metrics_port_from_node_id(i))
                        for i in range(0,4)]

        print("Running tests in {}".format(cls.testdir))
        cls.startServers()



    @classmethod
    def tearDownClass(cls):
        cls.stopServers()
        shutil.rmtree(cls.testdir)
        os.chdir(cls.origdir)

    @classmethod
    def generateKeys(cls):
        """Create keys expected by SimpleTest server for 4 nodes"""
        keygen = os.path.join(cls.toolsdir, "GenerateConcordKeys")
        args = [keygen, "-n", "4", "-f", "1", "-o", "private_replica_"]
        subprocess.run(args, check=True)

    def readRequest(self):
        """Serialize a read request"""
        return struct.pack("<Q", 100)

    def writeRequest(self, val):
        """Serialize a write request"""
        return struct.pack("<QQ", 200, val)

    def read_val(self, val):
        """Return a deserialized read value"""
        return struct.unpack("<Q", val)[0]

    def testTimeout(self):
        """Client requests will timeout since no servers are running"""
        read = self.readRequest()
        write = self.writeRequest(1)
        trio.run(self._testTimeout, read, True)
        trio.run(self._testTimeout, write, False)

    async def _testTimeout(self, msg, read_only):
       config = self.config._replace(req_timeout_milli=0)
       with bft_client.UdpClient(config, self.replicas, None) as udp_client:
           with self.assertRaises(trio.TooSlowError):
               await udp_client.sendSync(msg, read_only)

    def testReadWrittenValue(self):
        """Write a value and then read it"""
        trio.run(self._testReadWrittenValue)

    @verify_system_is_ready
    async def _testReadWrittenValue(self):
       val = 999
       with bft_client.UdpClient(self.config, self.replicas, None) as udp_client:
           await udp_client.sendSync(self.writeRequest(val), False)
           read = await udp_client.sendSync(self.readRequest(), True)
           self.assertEqual(val, self.read_val(read))

    def testRetry(self):
        """
        Start servers after client has already made an attempt to send and
        ensure request succeeds.
        """
        trio.run(self._testRetry)

    async def _testRetry(self):
        """Start servers after a delay in parallel with a write request"""
        await self.writeWithRetryAssert()

    async def writeWithRetryAssert(self):
        """Issue a write and ensure that a retry occurs"""
        config = self.config._replace(retry_timeout_milli=0)
        val = 2
        with bft_client.UdpClient(config, self.replicas, None) as udp_client:
           self.assertEqual(udp_client.retries, 0)
           try:
            await udp_client.sendSync(self.writeRequest(val), False)
           except(trio.TooSlowError):
               pass
           self.assertTrue(udp_client.retries > 0)

    def testPrimaryWrite(self):
        """Test that we learn the primary and using it succeeds."""
        trio.run(self._testPrimaryWrite)

    @verify_system_is_ready
    async def _testPrimaryWrite(self):
       # Try to guarantee we don't retry accidentally
       config = self.config._replace(retry_timeout_milli=500)
       with bft_client.UdpClient(config, self.replicas, None) as udp_client:
           self.assertEqual(None, udp_client.primary)
           await udp_client.sendSync(self.writeRequest(3), False)
           # We know the servers are up once the write completes
           self.assertNotEqual(None, udp_client.primary)
           sent = udp_client.msgs_sent
           read = await udp_client.sendSync(self.readRequest(), True)
           sent += 4
           self.assertEqual(sent, udp_client.msgs_sent)
           self.assertEqual(3, self.read_val(read))
           self.assertNotEqual(None, udp_client.primary)
           await udp_client.sendSync(self.writeRequest(4), False)
           sent += 1 # Only send to the primary
           self.assertEqual(sent, udp_client.msgs_sent)
           read = await udp_client.sendSync(self.readRequest(), True)
           sent += 4
           self.assertEqual(sent, udp_client.msgs_sent)
           self.assertEqual(4, self.read_val(read))
           self.assertNotEqual(None, udp_client.primary)

    @verify_system_is_ready
    async def _testMofNQuorum(self):
        config = self.config._replace(retry_timeout_milli=500)
        with bft_client.UdpClient(config, self.replicas, None) as udp_client:
            await udp_client.sendSync(self.writeRequest(5), False)
            single_read_q = bft_client.MofNQuorum([0, 1, 2, 3], 1)
            read = await udp_client.sendSync(self.readRequest(), True, m_of_n_quorum=single_read_q)
            self.assertEqual(5, self.read_val(read))

    def testMonNQuorum(self):
        trio.run(self._testMofNQuorum)


if __name__ == '__main__':
    unittest.main()
