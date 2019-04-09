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
import struct

class MsgError(Exception):
    """ Exception raised when dealing with an invalid Msg"""
    def __init__(self, message):
        self.message = message

REQUEST_MSG_TYPE = 700
REPLY_MSG_TYPE = 800

# A Request type precedes all headers. It can be viewed as part of the headers
# as in the C++ code, but it's easier to work with headers and messages if we
# treat it separately during reads/unpack.
MSG_TYPE_FMT = "<H"
MSG_TYPE_SIZE = struct.calcsize(MSG_TYPE_FMT)

# The struct definition of the client request msg header
# Little Endian format with no padding
# We don't include the msg type here, since we have to read it first to
# understand what message is incoming.
REQUEST_HEADER_FMT = "<HBQL"
REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_HEADER_FMT)

# The struct definition of the client reply msg header
# Little Endian format with no padding
# We don't include the msg type here, since we have to read it first to
# understand what message is incoming.
REPLY_HEADER_FMT = "<HQL"
REPLY_HEADER_SIZE = struct.calcsize(REPLY_HEADER_FMT)

RequestHeader = namedtuple('RequestHeader', ['client_id', 'flags',
    'req_seq_num', 'length'])

ReplyHeader = namedtuple('ReplyHeader', ['primary_id',
    'req_seq_num', 'length'])

def pack_request(client_id, req_seq_num, read_only, msg):
    """Create and return a buffer with a header and message"""
    flags = 0
    if read_only:
        flags = 1
    header = RequestHeader(client_id, flags, req_seq_num, len(msg))
    data = b''.join([pack_request_header(header), msg])
    return data

def pack_request_header(header):
    """Take a RequestHeader and return a buffer"""
    return b''.join([struct.pack(MSG_TYPE_FMT, REQUEST_MSG_TYPE),
                     struct.pack(REQUEST_HEADER_FMT, *header)])

def unpack_request(data):
    """Take a buffer and return a pair of the RequestHeader and app data"""
    start = MSG_TYPE_SIZE + REQUEST_HEADER_SIZE
    return (unpack_request_header(data), data[start:])

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

def pack_reply(primary_id, req_seq_num, msg):
    """
    Take message information and a message and return a construct a buffer
    containing a serialized reply header and message.
    """
    header = ReplyHeader(primary_id, req_seq_num, len(msg))
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
