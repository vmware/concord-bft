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

from collections import namedtuple
from enum import IntEnum
import struct

class MsgError(Exception):
    """ Exception raised when dealing with an invalid Msg"""
    def __init__(self, message):
        self.message = message

PRE_PROCESS_TYPE = 500
REQUEST_MSG_TYPE = 700
BATCH_REQUEST_MSG_TYPE = 750
REPLY_MSG_TYPE = 800
RECONFIG_FLAG = 0x20

# A Request type precedes all headers. It can be viewed as part of the headers
# as in the C++ code, but it's easier to work with headers and messages if we
# treat it separately during reads/unpack.
MSG_TYPE_FMT = "<H"
MSG_TYPE_SIZE = struct.calcsize(MSG_TYPE_FMT)

# The struct definition of the client request msg header
# Little Endian format with no padding
# We don't include the msg type here, since we have to read it first to
# understand what message is incoming.
REQUEST_HEADER_FMT = "<LHQLQLQLHL"
REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_HEADER_FMT)

# The struct definition of the client batch request msg header
# Little Endian format with no padding
# We don't include the msg type here, since we have to read it first to
# understand what message is incoming.
BATCH_REQUEST_HEADER_FMT = "<LHLL"

# The struct definition of the client reply msg header
# Little Endian format with no padding
# We don't include the msg type here, since we have to read it first to
# understand what message is incoming.
REPLY_HEADER_FMT = "<LHQLLL"
REPLY_HEADER_SIZE = struct.calcsize(REPLY_HEADER_FMT)

RequestHeader = namedtuple('RequestHeader', ['span_context_size', 'client_id', 'flags', 'op_result', 'req_seq_num',
                                             'length', 'timeout_milli', 'cid', 'req_sig_len', 'extra_data_length'])

BatchRequestHeader = namedtuple('BatchRequestHeader', ['cid', 'client_id', 'num_of_messages_in_batch', 'data_size'])

# Unless bft clients are blocking there is no actual need for span context
# The field was added for compatibility with other messages
# So the span context size must be 0
#
# Replica specific information is not used yet, so rsi_length is always 0
ReplyHeader = namedtuple('ReplyHeader', ['span_context_size', 'primary_id', 'req_seq_num', 'result', 'length',
                                         'rsi_length'])

class OperationResult(IntEnum):
    SUCCESS = 0
    UNKNOWN = 1
    INVALID_REQUEST = 2
    NOT_READY = 3
    TIMEOUT = 4
    EXEC_DATA_TOO_LARGE = 5
    EXEC_DATA_EMPTY = 6
    CONFLICT_DETECTED = 7
    OVERLOADED = 8
    EXEC_ENGINE_REJECT_ERROR = 9
    INTERNAL_ERROR = 10

def pack_request(client_id, req_seq_num, read_only, timeout_milli, cid, msg, op_result = 0, pre_process=False,
                 reconfiguration=False, span_context=b'', signature=b''):
    """Create and return a buffer with a header and message"""
    flags = 0x0
    if read_only:
        flags = 0x1
    elif pre_process:
        flags = 0x2
    if reconfiguration:
        flags |= RECONFIG_FLAG
    sig_len = len(signature) if signature else 0
    extra_data_len = 0
    header = RequestHeader(len(span_context), client_id, flags, op_result, req_seq_num, len(msg), timeout_milli,
                           len(cid), sig_len, extra_data_len)
    data = b''.join([pack_request_header(header, pre_process), span_context, msg, cid.encode(), signature])
    return data

def pack_batch_request(client_id, num_of_messages_in_batch, msg_data, cid):
    """Create and return a buffer with a header and message"""
    header = BatchRequestHeader(len(cid), client_id, num_of_messages_in_batch, len(msg_data))
    data = b''.join([pack_batch_request_header(header), cid.encode(), msg_data])
    return data

def pack_request_header(header, pre_process=False):
    """Take a RequestHeader and return a buffer"""
    msg_type = PRE_PROCESS_TYPE if pre_process else REQUEST_MSG_TYPE
    return b''.join([struct.pack(MSG_TYPE_FMT, msg_type),
                     struct.pack(REQUEST_HEADER_FMT, *header)])

def pack_batch_request_header(header):
    msg_type = BATCH_REQUEST_MSG_TYPE
    return b''.join([struct.pack(MSG_TYPE_FMT, msg_type),
                     struct.pack(BATCH_REQUEST_HEADER_FMT, *header)])

def unpack_request(data):
    """Take a buffer and return a pair of the RequestHeader and app data"""
    header = unpack_request_header(data)

    start = MSG_TYPE_SIZE + REQUEST_HEADER_SIZE
    end = start + header.span_context_size
    span_context = data[start:end]

    start = end
    end = start + header.length
    msg = data[start:end]

    start = end
    cid = data[start:].decode()
    return header, span_context, msg, cid

def unpack_request_header(data):
    """
    Take a buffer and return a request header.
    Throws MsgError if type is not a request or struct.error if structure can't
    be unpacked.
    """
    msg_type = unpack_msg_type(data)
    if msg_type != REQUEST_MSG_TYPE:
        raise MsgError("Expected a request message")
    end = REQUEST_HEADER_SIZE + MSG_TYPE_SIZE
    return RequestHeader._make(struct.unpack(REQUEST_HEADER_FMT,
                                             data[MSG_TYPE_SIZE:end]))

def pack_reply(primary_id, req_seq_num, msg, result=0, rsi_length=0):
    """
    Take message information and a message and return a construct a buffer
    containing a serialized reply header and message.
    """

    header = ReplyHeader(0, primary_id, req_seq_num, result, len(msg), rsi_length)
    return b''.join([pack_reply_header(header), msg])

def pack_reply_header(header):
    """Take a ReplyHeader and return a buffer"""
    return b''.join([struct.pack(MSG_TYPE_FMT, REPLY_MSG_TYPE),
                     struct.pack(REPLY_HEADER_FMT, *header)])

def unpack_msg_type(data):
    return struct.unpack(MSG_TYPE_FMT, data[0:MSG_TYPE_SIZE])[0]

def unpack_reply(data):
    """Take a buffer and return a pair of the ReplyHeader and app data"""
    start = MSG_TYPE_SIZE + REPLY_HEADER_SIZE
    return (unpack_reply_header(data), data[start:])

def unpack_reply_header(data):
    """
    Take a buffer and return a reply header.
    Throws MsgError if type is not a reply or struct.error if structure can't be
    unpacked.
    """
    msg_type = unpack_msg_type(data)
    if msg_type != REPLY_MSG_TYPE:
        raise MsgError("Expected a reply message")
    end = REPLY_HEADER_SIZE + MSG_TYPE_SIZE
    return ReplyHeader._make(struct.unpack(REPLY_HEADER_FMT,
                                           data[MSG_TYPE_SIZE:end]))
