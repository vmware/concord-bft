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
import trio
import json
import struct

from bft_config import Replica

REQUEST_TYPE = 0
REPLY_TYPE = 1
ERROR_TYPE = 2

HEADER_FMT = "<BQ"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

MAX_MSG_SIZE = 64*1024; # 64k

class MetricsClient:
    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        self.sock.close()

    def __init__(self, replica):
        self.seq_num = 0
        self.replica = replica
        self.sock = trio.socket.socket(trio.socket.AF_INET,
                                       trio.socket.SOCK_DGRAM)
    def _req(self):
        """Return a get request to the metrics server"""
        self.seq_num += 1
        req = struct.pack(HEADER_FMT, REQUEST_TYPE, self.seq_num)
        return req

    async def get(self):
        """
        Send a get metrics request, retrieve the JSON response, decode it and
        return a map of metrics.

        There is no explicit timeout here. Users should call `with
        trio.fail_after as necessary`.
        """
        destination = (self.replica.ip, self.replica.metrics_port)
        await self.sock.sendto(self._req(), destination)
        while True:
            reply, _ = await self.sock.recvfrom(MAX_MSG_SIZE)
            assert 1 == reply[0]
            _, seq_num = struct.unpack(HEADER_FMT, reply[0:HEADER_SIZE])
            if seq_num == self.seq_num:
                return json.loads(reply[HEADER_SIZE:])
