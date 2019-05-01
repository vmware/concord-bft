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

from bft_config import Replica

MAX_MSG_SIZE = 64*1024; # 64k

def req():
    """Return a get request to the metrics server"""
    req = bytearray()
    req.append(0)
    return req

class MetricsClient:
    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        self.sock.close()

    def __init__(self, replicas):
        self.replicas = {}
        for r in replicas:
            self.replicas[r.id] = (r.ip, r.port)
        self.sock = trio.socket.socket(trio.socket.AF_INET,
                                       trio.socket.SOCK_DGRAM)

    async def get(self, replica):
        """
        Send a get metrics request, retrieve the JSON response, decode it and
        return a map of metrics.

        There is no explicit timeout here. Users should call `with
        trio.fail_after as necessary`.
        """
        await self.sock.sendto(req(), self.replicas[replica])
        reply, _ = await self.sock.recvfrom(MAX_MSG_SIZE)
        assert 1 == reply[0]
        return json.loads(reply[1:])
