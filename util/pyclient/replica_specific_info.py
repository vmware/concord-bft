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

CommonReplyHeader = namedtuple('CommonReplyHeader', ['req_seq_num'])

MatchedReplyKey = namedtuple('MatchedReplyKey', ['header', 'data'])


class MsgWithReplicaSpecificInfo:
    def __init__(self, bare_message, sender_id):
        orig_header, orig_data = bft_msgs.unpack_reply(bare_message)
        self.common_header = CommonReplyHeader(orig_header.req_seq_num)
        rsi_loc = orig_header.length - orig_header.rsi_length
        self.rsi_data = orig_data[rsi_loc:]
        self.common_data = orig_data[:rsi_loc]
        self.sender_id = sender_id
        self.primary_id = orig_header.primary_id
        self.result = orig_header.result

    def get_common_reply(self):
        return self.common_header, self.common_data

    def get_common_data(self):
        return self.common_data

    def get_rsi_data(self):
        return self.rsi_data

    def get_matched_reply_key(self):
        return MatchedReplyKey(self.common_header, self.common_data)

    def get_sender_id(self):
        return self.sender_id

    def is_valid(self, req_seq_num):
        return self.common_header.req_seq_num == req_seq_num

    def get_primary(self):
        return self.primary_id

    def get_common_data_with_result(self):
        return self.common_data, self.result

class RepliesManager:
    def __init__(self):
        self.replies = dict()
        self.seq_nums = dict()

    def add_reply(self, rsi_message):
        """
        Add a new reply to replies. We want to return only the replies that belongs the same common data.
        Thus, we have a key of (header, data) to which we insert the reply.
        Also, to avoid double replies from the same replica (due to retries or malicious activity) we keep a reply
        per-replica in a dictionary.
        """
        key = rsi_message.get_matched_reply_key()
        self.seq_nums[rsi_message.common_header.req_seq_num] = key
        if key not in self.replies.keys():
            self.replies[key] = dict()
        self.replies[key][rsi_message.get_sender_id()] = rsi_message
        return len(self.replies[key])

    def set_seq_nums(self, seq_nums):
        self.seq_nums = dict()
        for s in seq_nums:
            self.seq_nums[s] = None

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
        rsi_replies = dict()
        for k,v in self.replies[matched_reply_key].items():
            rsi_replies[k] = v
        return rsi_replies

    def get_all_replies(self):
        replies = dict()
        for seq_num, key in self.seq_nums.items():
            replies[seq_num] = next(iter(self.replies[key].values()))
        return replies

    def clear_replies(self):
        self.replies = dict()

    def expects_seq_num(self, seq_num):
        return seq_num in self.seq_nums.keys()

    def has_quorum_on_all(self, required_replies):
        has_quorum = True
        for seq_num, key in self.seq_nums.items():
            if key is None:
                has_quorum = False
            elif self.num_matching_replies(key) < required_replies:
                has_quorum = False
        return has_quorum
