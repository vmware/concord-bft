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

from collections import namedtuple
import bft_msgs

CommonReplyHeader = namedtuple('CommonReplyHeader', ['span_context_size', 'primary_id',
                                                     'req_seq_num', 'length'])

MatchedReplyKey = namedtuple('MatchedReplyKey', ['header', 'data'])


class MsgWithReplicaSpecificInfo:
    def __init__(self, bare_message, sender_id):
        orig_header, orig_data = bft_msgs.unpack_reply(bare_message)
        self.common_header = CommonReplyHeader(orig_header.span_context_size, orig_header.primary_id,
                                               orig_header.req_seq_num, orig_header.length - orig_header.rsi_length)
        rsi_loc = orig_header.length - orig_header.rsi_length
        self.rsi_data = orig_data[rsi_loc:]
        self.common_data = orig_data[:rsi_loc]
        self.sender_id = sender_id

    def get_common_reply(self):
        return self.common_header, self.common_data

    def get_rsi_data(self):
        return self.rsi_data

    def get_matched_reply_key(self):
        return MatchedReplyKey(self.common_header, self.common_data)

    def get_sender_id(self):
        return self.sender_id

    def is_valid(self, req_seq_num):
        return self.common_header.req_seq_num == req_seq_num


class RepliesManager:
    def __init__(self):
        self.replies = dict()

    def add_reply(self, rsi_message):
        key = rsi_message.get_matched_reply_key()
        if key not in self.replies.keys():
            self.replies[key] = dict()
        self.replies[key][rsi_message.get_sender_id()] = rsi_message.get_rsi_data()
        return len(self.replies[key])

    def pop(self, matched_reply_key):
        return self.replies.pop(matched_reply_key)

    def num_distinct_replies(self):
        return len(self.replies)

    def num_matching_replies(self, matched_reply_key):
        if matched_reply_key not in self.replies.keys():
            return 0
        return len(self.replies[matched_reply_key])

    def get_rsi_replies(self, matched_reply_key):
        if matched_reply_key not in self.replies.keys():
            return dict()
        return self.replies[matched_reply_key]

    def clear_replies(self):
        self.replies = dict()

