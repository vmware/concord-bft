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
import struct
import trio
import time

import bft_msgs
from bft_config import Config, Replica

# All test communication expects ports to start from 3710
BASE_PORT = 3710

class ReqSeqNum:
    def __init__(self):
        self.time_since_epoch_milli = int(time.time()*1000)
        self.count = 0
        self.max_count = 0x3FFFFF
        self.max_count_len = 22

    def next(self):
        """
        Calculate the next req_seq_num.
        Return the calculated value as an int sized for 64 bits
        """
        milli = int(time.time()*1000)
        if milli > self.time_since_epoch_milli:
            self.time_since_epoch_milli = milli
            self.count = 0
        else:
            if self.count == self.max_count:
                self.time_since_epoch_milli += 1
                self.count = 0
            else:
                self.count += 1
        return self.val()

    def val(self):
        """ Return an int sized for 64 bits """
        assert(self.count <= self.max_count)
        r = self.time_since_epoch_milli << self.max_count_len
        r = r | self.count
        return r


class UdpClient:
    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        self.sock.close()

    def __init__(self, config, replicas):
        self.config = config
        self.replicas = replicas
        self.sock = trio.socket.socket(trio.socket.AF_INET,
                                       trio.socket.SOCK_DGRAM)
        self.req_seq_num = ReqSeqNum()
        self.client_id = config.id
        self.replies = dict()
        self.primary = None
        self.reply = None
        self.retries = 0
        self.msgs_sent = 0
        self.reply_quorum = 2*config.f + config.c + 1
        self.sock_bound = False

    async def write(self, msg, seq_num=None):
        """ A wrapper around sendSync for requests that mutate state """
        return await self.sendSync(msg, False, seq_num)

    async def read(self, msg, seq_num=None):
        """ A wrapper around sendSync for requests that do not mutate state """
        return await self.sendSync(msg, True, seq_num)

    async def sendSync(self, msg, read_only, seq_num=None):
        """
        Send a client request and wait for a quorum (2F+C+1) of replies.

        Return a single reply message if a quorum of replies matches.
        Otherwise, raise a trio.TooSlowError indicating the request timed out.

        Retry Strategy:
            If the request is a write and the primary is known then send only to
            the primary on the first attempt. Otherwise, if the request is read
            only or the primary is unknown, then send to all replicas on the
            first attempt.

            After `config.retry_timeout_milli` without receiving a quorum of
            identical replies, then clear the replies and send to all replicas.
            Continue this strategy every `retry_timeout_milli` until
            `config.req_timeout_milli` elapses. If `config.req_timeout_milli`
            elapses then a trio.TooSlowError is raised.

         Note that this method also binds the socket to an appropriate port if
         not already bound.
        """
        if not self.sock_bound:
            await self.bind()

        if seq_num is None:
            seq_num = self.req_seq_num.next()

        data = bft_msgs.pack_request(
                    self.client_id, seq_num, read_only, msg)

        # Raise a trio.TooSlowError exception if a quorum of replies
        with trio.fail_after(self.config.req_timeout_milli/1000):
            self.reset_on_new_request()
            self.retries = 0
            return await self.send_loop(data, read_only)

    def reset_on_retry(self):
        """Reset any state that must be reset during retries"""
        self.replies = dict()
        self.primary = None
        self.retries += 1

    def reset_on_new_request(self):
        """Reset any state that must be reset during new requests"""
        self.replies = dict()
        self.reply = None
        self.retries = 0

    async def bind(self):
        # Each port is a function of its client_id
        port = BASE_PORT + 2*self.client_id
        await self.sock.bind(("127.0.0.1", port))
        self.sock_bound = True

    async def send_loop(self, data, read_only):
        """
        Send and wait for a quorum of replies. Keep retrying if a quorum
        isn't received. Eventually the max request timeout from the
        outer scope will fire cancelling all sub-scopes and their coroutines
        including this one.
        """
        while self.reply is None:
            with trio.move_on_after(self.config.retry_timeout_milli/1000):
                async with trio.open_nursery() as nursery:
                    if read_only or self.primary is None:
                        await self.send_all(data)
                    else:
                        await self.send_to_primary(data)
                    nursery.start_soon(self.recv, nursery.cancel_scope)
            if self.reply is None:
                self.reset_on_retry()
        return self.reply

    async def send_to_primary(self, request):
        """Send a serialized request to the primary"""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.sendto, request, (self.primary.ip,
                                                      self.primary.port))

    async def send_all(self, request):
        """Send a serialized request to all replicas"""
        async with trio.open_nursery() as nursery:
            for replica in self.replicas:
                nursery.start_soon(self.sendto, request, (replica.ip,
                                                          replica.port))
    async def sendto(self, request, ip_port):
        """Send a request over a udp socket"""
        await self.sock.sendto(request, ip_port)
        self.msgs_sent += 1

    async def recv(self, cancel_scope):
        """
        Receive reply messages until a quorum is achieved or the enclosing
        cancel_scope times out.
        """
        while True:
            data, sender = await self.sock.recvfrom(self.config.max_msg_size)
            header, reply = bft_msgs.unpack_reply(data)
            if self.valid_reply(header):
                self.replies[sender] = (header, reply)
            if self.has_quorum():
                # This cancel will propagate upward and gracefully terminate all
                # coroutines. self.reply will get set in self.has_quorum()
                cancel_scope.cancel()

    def valid_reply(self, header):
        """Return true if the sequence number is correct"""
        return self.req_seq_num.val() == header.req_seq_num

    def has_quorum(self):
        """
        Return true if the client has seen 2F+C+1 matching replies

        Side Effects:
            Set self.reply to the reply with the quorum
            Set self.primary to the primary in the quorum of reply headers
        """

        if len(self.replies) < self.reply_quorum:
            return False

        # a reply mapped to a count of each specific reply
        # We must have a quorum of identical replies
        reply_counts = dict()
        for reply in self.replies.values():
            count = reply_counts.get(reply, 0)
            reply_counts[reply] = count + 1

        for reply, counts in reply_counts.items():
            if counts >= self.reply_quorum:
                self.reply = reply[1]
                self.primary = self.replicas[reply[0].primary_id]
                return True
        return False
