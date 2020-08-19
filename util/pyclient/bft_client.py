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
import replica_specific_info as rsi
from bft_config import Config, Replica

# All test communication expects ports to start from 3710
BASE_PORT = 3710


class ReqSeqNum:
    def __init__(self):
        self.time_since_epoch_milli = int(time.time() * 1000)
        self.count = 0
        self.max_count = 0x3FFFFF
        self.max_count_len = 22

    def next(self):
        """
        Calculate the next req_seq_num.
        Return the calculated value as an int sized for 64 bits
        """
        milli = int(time.time() * 1000)
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
        assert (self.count <= self.max_count)
        r = self.time_since_epoch_milli << self.max_count_len
        r = r | self.count
        return r


class MofNQuorum:
    def __init__(self, replicas, required):
        self.replicas = replicas
        self.required = required
    @classmethod
    def LinearizableQuorum(cls, config, replicas):
        f = config.f
        c = config.c
        return MofNQuorum(replicas, 2 * f + c + 1)

    @classmethod
    def ByzantineSafeQuorum(cls, config, replicas):
        f = config.f
        return MofNQuorum(replicas, f + 1)

    @classmethod
    def All(cls, config, replicas):
        return MofNQuorum(replicas, len(replicas))


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
        self.primary = None
        self.reply = None
        self.retries = 0
        self.msgs_sent = 0
        self.port = BASE_PORT + 2 * self.client_id
        self.sock_bound = False
        self.replies_manager = rsi.RepliesManager()
        self.rsi_replies = dict()

    async def write(self, msg, seq_num=None, cid=None, pre_process=False, m_of_n_quorum=None):
        """ A wrapper around sendSync for requests that mutate state """
        return await self.sendSync(msg, False, seq_num, cid, pre_process, m_of_n_quorum)

    async def read(self, msg, seq_num=None, cid=None, m_of_n_quorum=None):
        """ A wrapper around sendSync for requests that do not mutate state """
        return await self.sendSync(msg, True, seq_num, cid, m_of_n_quorum=m_of_n_quorum)

    async def sendSync(self, msg, read_only, seq_num=None, cid=None, pre_process=False, m_of_n_quorum=None):
        """
        Send a client request and wait for a m_of_n_quorum (if None, it will set to 2F+C+1 quorum) of replies.

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

        if cid is None:
            cid = str(seq_num)
        data = bft_msgs.pack_request(
            self.client_id, seq_num, read_only, self.config.req_timeout_milli, cid, msg, pre_process)

        if m_of_n_quorum is None:
            m_of_n_quorum = MofNQuorum.LinearizableQuorum(self.config, [r.id for r in self.replicas])

        # Raise a trio.TooSlowError exception if a quorum of replies
        try:
            with trio.fail_after(self.config.req_timeout_milli / 1000):
                self.reset_on_new_request()
                self.retries = 0
                return await self.send_loop(data, read_only, m_of_n_quorum)
        except trio.TooSlowError:
            print("TooSlowError thrown from client_id", self.client_id, "for seq_num", seq_num)
            raise trio.TooSlowError
        finally:
            pass

    def reset_on_retry(self):
        """Reset any state that must be reset during retries"""
        self.primary = None
        self.retries += 1
        self.rsi_replies = dict()
        self.replies_manager.clear_replies()

    def reset_on_new_request(self):
        """Reset any state that must be reset during new requests"""
        self.reply = None
        self.retries = 0
        self.rsi_replies = dict()
        self.replies_manager.clear_replies()

    async def bind(self):
        # Each port is a function of its client_id
        await self.sock.bind(("127.0.0.1", self.port))
        self.sock_bound = True

    async def send_loop(self, data, read_only, m_of_n_quorum):
        """
        Send and wait for a quorum of replies. Keep retrying if a quorum
        isn't received. Eventually the max request timeout from the
        outer scope will fire cancelling all sub-scopes and their coroutines
        including this one.
        """
        dest_replicas = [r for r in self.replicas if r.id in m_of_n_quorum.replicas]
        while self.reply is None:
            with trio.move_on_after(self.config.retry_timeout_milli / 1000):
                async with trio.open_nursery() as nursery:
                    if read_only or self.primary is None:
                        await self.send_to_replicas(data, dest_replicas)
                    else:
                        await self.send_to_primary(data)
                    nursery.start_soon(self.recv, m_of_n_quorum.required, dest_replicas, nursery.cancel_scope)
            if self.reply is None:
                self.reset_on_retry()
        return self.reply

    async def send_to_primary(self, request):
        """Send a serialized request to the primary"""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.sendto, request, (self.primary.ip,
                                                      self.primary.port))

    async def send_to_replicas(self, request, replicas):
        """Send a serialized request to all replicas"""
        async with trio.open_nursery() as nursery:
            for replica in replicas:
                nursery.start_soon(self.sendto, request, (replica.ip,
                                                          replica.port))

    async def sendto(self, request, ip_port):
        """Send a request over a udp socket"""
        await self.sock.sendto(request, ip_port)
        self.msgs_sent += 1

    async def recv(self, required_replies, dest_replicas, cancel_scope):
        """
        Receive reply messages until a quorum is achieved or the enclosing
        cancel_scope times out.
        """
        replicas_ids = [(r.ip, r.port) for r in dest_replicas]
        while True:
            data, sender = await self.sock.recvfrom(self.config.max_msg_size)
            rsi_msg = rsi.MsgWithReplicaSpecificInfo(data, sender)
            header, reply = rsi_msg.get_common_reply()
            if self.valid_reply(header, rsi_msg.get_sender_id(), replicas_ids):
                quorum_size = self.replies_manager.add_reply(rsi_msg)
                if quorum_size == required_replies:
                    self.reply = reply
                    self.rsi_replies = self.replies_manager.get_rsi_replies(rsi_msg.get_matched_reply_key())
                    self.primary = self.replicas[header.primary_id]
                    cancel_scope.cancel()

    def valid_reply(self, header, sender, dest_replicas):
        return self.req_seq_num.val() == header.req_seq_num and sender in dest_replicas

    def get_rsi_replies(self):
        """
        Return a dictionary of {id: data} of the replicas specific information.
        This method should be called after the send has done and before initiating a new request
        """
        return self.rsi_replies
