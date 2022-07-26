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
import ssl
import os
import random
from cryptography.hazmat.primitives import serialization

import bft_msgs
import replica_specific_info as RSI
from bft_config import bft_msg_port_from_node_id
from abc import ABC, abstractmethod

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

class BftClient(ABC):
    def __init__(self, config, replicas, ro_replicas=[]):
        self.config = config
        self.replicas = replicas
        self.req_seq_num = ReqSeqNum()
        self.client_id = config.id
        self.primary = None
        self.replies = None
        self.retries = 0
        self.msgs_sent = 0
        self.replies_manager = RSI.RepliesManager()
        self.rsi_replies = dict()
        self.comm_prepared = False
        self.ro_replicas = ro_replicas

        txn_signing_key_path = self._get_txn_signing_priv_key_path(self.client_id)
        self.signing_key = None
        if txn_signing_key_path:
            with open(txn_signing_key_path, 'rb') as f:
                self.signing_key = serialization.load_pem_private_key(f.read(), password=None)

    @abstractmethod
    def __enter__(self):
        """ Context manager method for 'with' statements """
        pass

    @abstractmethod
    def __exit__(self, *args):
        """ Context manager method for 'with' statements """
        pass

    @abstractmethod
    async def _send_data(self, data, replica, endpoint_num=0xFFFFFFFFFFFFFFFF):
        """ Send data to a replica by the client specific implementation """
        pass

    @abstractmethod
    async def _comm_prepare(self):
        """ Call before sending or receiving data. Some clients need to prepare their communication stack. """
        pass

    @abstractmethod
    async def _recv_data(self, required_replies, dest_replicas, cancel_scope):
        """
        Receive and process data from required_replies out of dest_replicas. Use cancel_scope when target achieved
        """
        pass

    def get_total_num_replicas(self):
        return len(self.replicas)

    async def write_with_result(self, msg, seq_num=None, cid=None, pre_process=False, m_of_n_quorum=None,
                    reconfiguration=False, corrupt_params={}, no_retries=False, result=1):
        """ A wrapper around sendSync for requests that mutate state """
        return await self.sendSync(
            msg, False, seq_num, cid, pre_process, m_of_n_quorum, reconfiguration,
            corrupt_params=corrupt_params, no_retries=no_retries, result=result)

    async def write(self, msg, seq_num=None, cid=None, pre_process=False, m_of_n_quorum=None,
                    reconfiguration=False, corrupt_params={}, no_retries=False, result=1):
        """ A wrapper around sendSync for requests that mutate state """
        reply = await self.sendSync(
            msg, False, seq_num, cid, pre_process, m_of_n_quorum, reconfiguration,
            corrupt_params=corrupt_params, no_retries=no_retries, result=result)
        return reply[0] if reply else None

    async def read(self, msg, seq_num=None, cid=None, m_of_n_quorum=None,
                   reconfiguration=False, include_ro=False, corrupt_params={}, no_retries=False, result=1):
        """ A wrapper around sendSync for requests that do not mutate state """
        reply = await self.sendSync(
            msg, True, seq_num, cid, m_of_n_quorum=m_of_n_quorum,
            reconfiguration=reconfiguration, include_ro=include_ro, corrupt_params=corrupt_params, no_retries=no_retries, result=result)
        return reply[0] if reply else None

    async def sendSync(self, msg, read_only, seq_num=None, cid=None, pre_process=False, m_of_n_quorum=None, \
        reconfiguration=False, include_ro=False, corrupt_params={}, no_retries=False, result=1):
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
        # Call an abstract function to allow each client type to set-up its communication before starting
        if not self.comm_prepared:
            await self._comm_prepare()

        if seq_num is None:
            seq_num = self.req_seq_num.next()

        if cid is None:
            cid = str(seq_num)

        signature = b''
        client_id = self.client_id
        if self.signing_key:
            signature = self.signing_key.sign(bytes(msg))
            if corrupt_params:
                msg, signature, client_id = self._corrupt_signing_params(msg, signature, client_id, corrupt_params)

        data = bft_msgs.pack_request(client_id, seq_num, read_only, self.config.req_timeout_milli, cid, msg, result,
                                     pre_process, reconfiguration=reconfiguration, signature=signature)

        if m_of_n_quorum is None:
            m_of_n_quorum = MofNQuorum.LinearizableQuorum(self.config, [r.id for r in self.replicas])

        # Raise a trio.TooSlowError exception if a quorum of replies
        try:
            with trio.fail_after(self.config.req_timeout_milli / 1000):
                self._reset_on_new_request([seq_num])
                replies = await self._send_receive_loop(
                    data, read_only, m_of_n_quorum, include_ro=include_ro, no_retries=no_retries)
                return next(iter(self.replies.values())).get_common_data_with_result() if replies else None
        except trio.TooSlowError:
            raise trio.TooSlowError(f"client_id: {self.client_id}, seq_num: {seq_num}")
        finally:
            pass

    async def write_batch(self, msg_batch, batch_seq_nums=None, m_of_n_quorum=None, corrupt_params=None, no_retries=False):
        if not self.comm_prepared:
            await self._comm_prepare()

        cid = str(self.req_seq_num.next())
        batch_size = len(msg_batch)

        if batch_seq_nums is None:
            batch_seq_nums = []
            for n in range(batch_size):
                batch_seq_nums.append(self.req_seq_num.next())

        msg_data = b''
        req_index_to_corrupt = random.randint(1, batch_size-1) # don't corrupt the 1st
        for n in range(batch_size):
            msg = msg_batch[n]
            msg_seq_num = batch_seq_nums[n]
            msg_cid = str(msg_seq_num)

            signature = b''
            client_id = self.client_id
            if self.signing_key:
                signature = self.signing_key.sign(bytes(msg))
                if corrupt_params and (req_index_to_corrupt == n):
                    msg, signature, client_id = self._corrupt_signing_params(msg, signature, client_id, corrupt_params)

            msg_data = b''.join([msg_data, bft_msgs.pack_request(
                self.client_id, msg_seq_num, False, self.config.req_timeout_milli, msg_cid, msg, 0, True,
                reconfiguration=False, span_context=b'', signature=signature)])

        data = bft_msgs.pack_batch_request(self.client_id, batch_size, msg_data, cid)

        if m_of_n_quorum is None:
            m_of_n_quorum = MofNQuorum.LinearizableQuorum(self.config, [r.id for r in self.replicas])

        # Raise a trio.TooSlowError exception if a quorum of replies
        try:
            with trio.fail_after(batch_size * self.config.req_timeout_milli / 1000):
                self._reset_on_new_request(batch_seq_nums)
                return await self._send_receive_loop(data, False, m_of_n_quorum,
                    batch_size * self.config.retry_timeout_milli / 1000, no_retries=no_retries)
        except trio.TooSlowError:
            raise trio.TooSlowError(f"client_id {self.client_id}, for batch msg {cid} {batch_seq_nums}")
        finally:
            pass

    def _reset_on_retry(self):
        """Reset any state that must be reset during retries"""
        self.primary = None
        self.retries += 1
        if self.replies_manager.num_distinct_replies() > self.config.f:
            self.rsi_replies = dict()
            self.replies_manager.clear_replies()

    def _reset_on_new_request(self, seq_nums):
        """Reset any state that must be reset during new requests"""
        self.replies = None
        self.retries = 0
        self.rsi_replies = dict()
        self.replies_manager.clear_replies()
        self.replies_manager.set_seq_nums(seq_nums)

    async def _send_receive_loop(self, data, read_only, m_of_n_quorum, timeout = None, include_ro=False, no_retries=False):
        """
        Send and wait for a quorum of replies. Keep retrying if a quorum
        isn't received. Eventually the max request timeout from the
        outer scope will fire cancelling all sub-scopes and their coroutines
        including this one.
        """
        if timeout is None:
            timeout = self.config.retry_timeout_milli / 1000
        dest_replicas = [r for r in self.replicas if r.id in m_of_n_quorum.replicas]
        if include_ro is False:
            dest_replicas = [r for r in dest_replicas if r.id not in self.ro_replicas]
        while self.replies is None:
            with trio.move_on_after(timeout):
                async with trio.open_nursery() as nursery:
                    if read_only or self.primary is None:
                        await self._send_to_replicas(data, dest_replicas)
                    else:
                        await self._send_to_primary(data)
                    nursery.start_soon(self._recv_data, m_of_n_quorum.required, dest_replicas, nursery.cancel_scope)
            if self.replies is None:
                if no_retries:
                    break
                self._reset_on_retry()
                await trio.sleep(0.1)
        return self.replies

    async def _send_to_primary(self, request):
        """Send a serialized request to the primary"""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._sendto_common, request, self.primary)

    async def _send_to_replicas(self, request, replicas):
        """Send a serialized request to all replicas"""
        async with trio.open_nursery() as nursery:
            for replica in replicas:
                nursery.start_soon(self._sendto_common, request, replica)

    async def _sendto_common(self, request, replica):
        """Send a request by invoking child class function"""
        if await self._send_data(request, replica):
            self.msgs_sent += 1

    def _valid_reply(self, header, sender, dest_replicas):
        """Check if received reply is valid - sequence number match and source is in dest_replicas"""
        return self.replies_manager.expects_seq_num(header.req_seq_num) and sender in dest_replicas

    def get_rsi_replies(self):
        """
        Return a dictionary of {id: data} of the replicas specific information.
        This method should be called after the send has done and before initiating a new request
        """
        return self.rsi_replies

    def _process_received_msg(self, data, sender, replicas_addr, required_replies, cancel_scope):
        """Called by child class to process a received message. At this point it's unknown if message is valid"""
        rsi_msg = RSI.MsgWithReplicaSpecificInfo(data, sender)
        header, reply = rsi_msg.get_common_reply()
        if self._valid_reply(header, rsi_msg.get_sender_id(), replicas_addr):
            self.replies_manager.add_reply(rsi_msg)
            if self.replies_manager.has_quorum_on_all(required_replies):
                self.replies = self.replies_manager.get_all_replies()
                rsi_replies = self.replies_manager.get_rsi_replies(rsi_msg.get_matched_reply_key())
                for r in rsi_replies:
                    rsi_reply = rsi_replies[r]
                    self.rsi_replies[r] = rsi_reply.get_rsi_data()
                    if r not in self.ro_replicas:
                        self.primary = self.replicas[rsi_reply.get_primary()]
                cancel_scope.cancel()

    def _get_txn_signing_priv_key_path(self, client_id):
        """
        This method finds the correct participant mapped to the given client_id.
        Then it finds the appropriate private key file and returns the read key from it.
        Transaction signing private key is under:
        <txn_signing_keys_path>/transaction_signing_keys/<participant>/transaction_signing_priv.pem
        """
        txn_signing_keys_path = self.config.txn_signing_keys_path
        principals_to_participant_map = self.config.principals_to_participant_map

        key_path = ""
        if txn_signing_keys_path and principals_to_participant_map:
            if int(client_id) in principals_to_participant_map:
                participant = str(principals_to_participant_map[client_id])
                key_path = os.path.join(txn_signing_keys_path, "transaction_signing_keys", participant, "transaction_signing_priv.pem")
            else:
                raise AttributeError("Client id {} not found in bft config property principals_to_participant_map".format(client_id))
        # If either one is empty, then throw an error
        elif (bool(txn_signing_keys_path) != bool(principals_to_participant_map)):
            raise AttributeError("Bft configs txn_signing_keys_path and principals_to_participant_map should be set/unset at the same time")

        return key_path

    def _corrupt_signing_params(self, msg, signature, client_id, corrupt_params={}):

        if "corrupt_msg" in corrupt_params:
            pos = random.randint(1, len(msg)-2)
            val = (msg[pos] + 1)  % 256     # to avoid ValueError: bytes must be in range(0, 256)
            msg = bytes(msg[0:pos]) + bytes([val]) + bytes(msg[pos+1:])
        if "corrupt_signature" in corrupt_params:
            pos = random.randint(1, len(signature)-2)
            val = (signature[pos] + 1) % 256    # to avoid ValueError: bytes must be in range(0, 256)
            signature = bytes(signature[0:pos]) + bytes([val]) + bytes(signature[pos+1:])
        if "wrong_msg_length" in corrupt_params:
            pos = random.randint(1, len(msg)-2)
            val = msg[pos] % 256    # to avoid ValueError: bytes must be in range(0, 256)
            msg = bytes(msg) + bytes([val])
        if "wrong_signature_length" in corrupt_params:
            pos = random.randint(1, len(signature)-2)
            val = signature[pos]
            signature = bytes(signature) + bytes([val])
        if "wrong_client_id_as_replica_id" in corrupt_params:
            client_id = corrupt_params["wrong_client_id_as_replica_id"]
        if "wrong_client_id_as_unknown_id" in corrupt_params:
            client_id = corrupt_params["wrong_client_id_as_unknown_id"]
        if "wrong_client_id_as_other_participant_client_id" in corrupt_params:
            client_id = corrupt_params["wrong_client_id_as_other_participant_client_id"]

        return msg, signature, client_id

class UdpClient(BftClient):
    """
    Define a UDP client - sends and receive all data via a single port
    (connectionless / stateless datagram communication)
    """
    def __init__(self, config, replicas, background_nursery, ro_replicas=[]):
        super().__init__(config, replicas, ro_replicas)
        self.sock = trio.socket.socket(trio.socket.AF_INET, trio.socket.SOCK_DGRAM)
        self.port = bft_msg_port_from_node_id(self.client_id)

    async def _comm_prepare(self):
        """ Bind the socket to 'localhost':port, where each port is a function of its client_id """
        await self.sock.bind(('localhost', self.port))
        self.comm_prepared = True

    async def _send_data(self, data, replica):
        """Send data. This operation always succeed after blocking, so True is always returned."""
        await self.sock.sendto(data, (replica.ip, replica.port))
        return True

    async def _recv_data(self, required_replies, dest_replicas, cancel_scope):
        """ Receive reply messages until a quorum is achieved or the enclosing cancel_scope times out. """
        replicas_addr = [(r.ip, r.port) for r in dest_replicas]
        while True:
            data, sender = await self.sock.recvfrom(self.config.max_msg_size)
            self._process_received_msg(data, sender, replicas_addr, required_replies, cancel_scope)

    def __enter__(self):
        """ Context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """ Context manager method for 'with' statements"""
        self.sock.close()

class TcpTlsClient(BftClient):
    """
    Define a TCP/TLS client. This client communicates as a TLS over TCP client to all replicas.
    It uses TCP to guarantee in-order data arrival and data reliability (using re-transmissions) and TLS to guarantee
    authenticity and integrity. To enable a TLS-handshake, it uses self-signed SSL X.509 certificates.
    """
    # In create_tls_certs.sh - openssl command line utility uses CN(certificate name) in the subj field.
    # This is the host name (domain name) to be verified.
    CERT_DOMAIN_FORMAT="node%dser"
    CERT_UNIFIED_FMT="node%d"
    # Taken from TlsTCPCommunication.cpp (we prefer hard-code and not to parse the file)
    MSG_LEN_SIZE = 4
    ENDPOINT_SIZE = 8
    MSG_HEADER_SIZE = MSG_LEN_SIZE + ENDPOINT_SIZE 

    def __init__(self, config, replicas, background_nursery, ro_replicas=[]):
        super().__init__(config, replicas, ro_replicas)
        self.ssl_streams = dict()
        self.reconnect_nursery = background_nursery
        self.exit_flag = False
        self.establish_ssl_stream_parklot = dict()
        for replica in replicas:
            # For each TCP destination replica, start a background task to keep connection established
            # Upon successful connection, task will wait on an event (trio.lowlevel.ParkingLot to try to reconnect
            lot = trio.lowlevel.ParkingLot()
            background_nursery.start_soon(self._establish_ssl_stream, replica, lot)
            self.establish_ssl_stream_parklot[(replica.ip, replica.port)] = lot

    def _get_private_key_path(self, replica_id, *, is_client):
        """
        Private key is under <certificate root path>/replica_id/<node type>/pk.pem,
        where node type is "server" or "client".
        """
        if self.config.use_unified_certs :
            return os.path.join(self.config.certs_path, str(replica_id), "pk.pem")

        cert_type = "client" if is_client else "server"
        return os.path.join(self.config.certs_path, str(replica_id), cert_type, "pk.pem")

    def _get_cert_path(self, replica_id, *, is_client):
        """
        Certificate is under <certificate root path>/replica_id/<node type>/cert.pem,
        where node type is "server" or "client".
        """
        if self.config.use_unified_certs :
            return os.path.join(self.config.certs_path, str(replica_id), "node.cert")

        cert_type = "client" if is_client else "server"
        return os.path.join(self.config.certs_path, str(replica_id), cert_type, cert_type + ".cert")

    async def _close_ssl_stream(self, dest_addr):
        """ Delete and close SSL stream from self.ssl_streams """
        stream = self.ssl_streams[dest_addr]
        del self.ssl_streams[dest_addr]
        await stream.aclose()

    async def _establish_ssl_stream(self, dest_replica, lot):
        """
        A task try to connect to dest_replica on an infinite loop until informed to quit (on BFT client exit).
        There hare 2 states:
        1) Connected to dest_replica - in that case, park in the lot.
        2) Disconnected from dest_replica - in that case, try to connect to it. On success, insert the new SSL stream
        into self.ssl_streams and move to parking until un-parked. On failure, sleep for 0.1 sec, and retry.

        SSL stream might be remove from self.ssl_streams while sending or receiving data, after finding out that
        connection is closed or broken.
        """
        if self.exit_flag:
            return
        server_cert_path = self._get_cert_path(dest_replica.id, is_client=False)
        client_cert_path = self._get_cert_path(self.client_id, is_client=True)
        client_pk_path = self._get_private_key_path(self.client_id, is_client=True)
        # Create an SSl context - enable CERT_REQUIRED and check_hostname = True
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # Verify server certificate using this trusted path
        ssl_context.load_verify_locations(cafile=server_cert_path)
        # Load my private key and certificate
        ssl_context.load_cert_chain(client_cert_path, client_pk_path)
        # Server hostname to be verified must be taken from create_tls_certs.sh
        if self.config.use_unified_certs:
            server_hostname = self.CERT_UNIFIED_FMT % dest_replica.id
        else:
            server_hostname = self.CERT_DOMAIN_FORMAT % dest_replica.id
        dest_addr = (dest_replica.ip, dest_replica.port)
        ssl_stream = tcp_stream = None
        # initial state of the event should be True, we want to connect
        while not self.exit_flag:
            try:
                # Open TCP stream and connect to server
                tcp_stream = await trio.open_tcp_stream(str(dest_replica.ip), int(dest_replica.port))
                # Wrap this stream with SSL stream, pass server_hostname to be verified
                ssl_stream = trio.SSLStream(tcp_stream, ssl_context, server_hostname=server_hostname, https_compatible=False)
                # Wait for handshake to finish (we want to be on the safe side - after this we are sure
                # connection is open)
                await ssl_stream.do_handshake()
                # Success! keep stream in dictionary and break out
                if not self.exit_flag:
                    self.ssl_streams[dest_addr] = ssl_stream
                    tcp_stream = ssl_stream = None
                    # park the task till it is woken by unpark()
                    await lot.park()
                    if dest_addr in self.ssl_streams:
                        # delete and close the stream
                        await self._close_ssl_stream(dest_addr)
            except (OSError, trio.BrokenResourceError):
                await trio.sleep(0.1)
            if ssl_stream:
                await ssl_stream.aclose()
            elif tcp_stream:
                await tcp_stream.aclose()
        if dest_addr in self.ssl_streams:
            await self._close_ssl_stream(dest_addr)

    async def _send_data(self, data, dest_replica, endpoint_num=0xFFFFFFFFFFFFFFFF):
        """ Try to send data to dest_replica. On exception - close the SSL stream. """
        dest_addr = (dest_replica.ip, dest_replica.port)
        if dest_addr not in self.ssl_streams.keys():
            # dest_replica is not connected (crushed? isolated?)
            self.establish_ssl_stream_parklot[dest_addr].unpark()
            return
        # first 4 bytes include the data header (message size), then comes the endpoint and data
        data_len = len(data)
        out_buff = bytearray(data_len.to_bytes(self.MSG_LEN_SIZE, "big"))
        out_buff += bytearray(endpoint_num.to_bytes(self.ENDPOINT_SIZE, "big"))
        out_buff += bytearray(data)
        stream = self.ssl_streams[dest_addr]
        try:
            await stream.send_all(out_buff)
            return True
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            # Failed! close the stream and return failure.
            if dest_addr in self.ssl_streams:
                self.establish_ssl_stream_parklot[dest_addr].unpark()
            return False

    async def _stream_recv_some(self, out_data, dest_addr, stream, num_bytes):
        """ Try to receive data from replica. On failure - close the SSL stream. Return True if got at least 1 byte """
        try:
            data = await stream.receive_some(num_bytes)
            if len(data) > 0:
                out_data += data
                return True
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            # We got EOF or an exception - close the stream
            pass
        except OverflowError:
            # The size of the payload is too big, something is wrong. Close the stream and restart.
            pass
        if dest_addr in self.ssl_streams:
            self.establish_ssl_stream_parklot[dest_addr].unpark()
        return False

    async def _receive_from_replica(self, dest_addr, replicas_addr, required_replies, cancel_scope):
        """
        Receive from a single replica. 3 stages:
        1) Wait for the 1st 12 bytes which contains the header (payload length and endpoint number)
        2) Receive the rest of the payload.
        3) Process the received message payload.
        If there is an error in stages 1 or 2 - exit straight (connection is closed inside _stream_recv_some)
        """
        while True:
            if dest_addr not in self.ssl_streams:
                self.establish_ssl_stream_parklot[dest_addr].unpark()
                return
            data = bytearray()
            stream = self.ssl_streams[dest_addr]
            while len(data) < self.MSG_HEADER_SIZE:
                if not await self._stream_recv_some(data, dest_addr, stream, self.MSG_HEADER_SIZE):
                    return
            payload_size = int.from_bytes(data[:self.MSG_LEN_SIZE], "big")
            endpoint_num = int.from_bytes(data[self.MSG_LEN_SIZE:self.MSG_HEADER_SIZE], "big")
            del data[:self.MSG_HEADER_SIZE]
            while len(data) < payload_size:
                if not await self._stream_recv_some(data, dest_addr, stream, payload_size - len(data)):
                    return
            try:
                self._process_received_msg(bytes(data), dest_addr, replicas_addr, required_replies, cancel_scope)
            except (bft_msgs.MsgError , struct.error) as ex:
                # TCP is a stream protocol and we can receive in a certain period of time a broken stream of bytes
                return

    async def _recv_data(self, required_replies, dest_replicas, cancel_scope):
        """
        Receive reply messages until a quorum is achieved or the enclosing cancel_scope times out.
        """
        replicas_addr = [(r.ip, r.port) for r in dest_replicas]
        async with trio.open_nursery() as nursery:
            for dest_addr in replicas_addr:
                if dest_addr in self.ssl_streams.keys():
                    nursery.start_soon(self._receive_from_replica, dest_addr, replicas_addr, required_replies,
                                       nursery.cancel_scope)
                else:
                    self.establish_ssl_stream_parklot[dest_addr].unpark()

    def __enter__(self):
        """ Context manager method for 'with' statements """
        pass

    def __exit__(self, *args):
        """ Context manager method for 'with' statements """
        self.exit_flag = True
        for lot in self.establish_ssl_stream_parklot.values():
            lot.unpark()

    async def _comm_prepare(self):
        """
        Do nothing, all connections are already established during on_started_replicas
        """
        self.comm_prepared = True
        return

    def get_connections_port_list(self):
        """ Duck typing, no overriding. Call this function only if you are sure the client is of type TcpTlsClient """
        l = list()
        for strm in self.ssl_streams.values():
            (_ , port) = strm.transport_stream.socket._sock.getsockname()
            l.append(port)
        return l
