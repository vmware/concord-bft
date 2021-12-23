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

import bft_msgs
import replica_specific_info as rsi


class TestRepliesManager(unittest.TestCase):

    def _build_msg(self, msg, primary_id=0, req_seq_num=1, result=0, rsi_length=0):
        return bft_msgs.pack_reply(primary_id, req_seq_num, msg, result, rsi_length)

    def test_add_message_to_manager(self):
        replies_manager = rsi.RepliesManager()
        packed = self._build_msg(b'hello')
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, 0)
        num_of_replies = replies_manager.add_reply(rsi_reply)
        self.assertEqual(num_of_replies, 1)

    def test_add_same_message_twice_to_manager(self):
        replies_manager = rsi.RepliesManager()
        packed = self._build_msg(b'hello')
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, sender_id=0)

        num_of_replies = replies_manager.add_reply(rsi_reply)
        self.assertEqual(num_of_replies, 1)

        num_of_replies = replies_manager.add_reply(rsi_reply)
        self.assertEqual(num_of_replies, 1)

    def test_add_two_identical_from_different_senders_message_to_manager(self):
        replies_manager = rsi.RepliesManager()
        packed = self._build_msg(b'hello')
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, sender_id=0)
        rsi_reply2 = rsi.MsgWithReplicaSpecificInfo(packed, sender_id=1)
        num_of_replies = replies_manager.add_reply(rsi_reply)
        self.assertEqual(num_of_replies, 1)

        num_of_replies = replies_manager.add_reply(rsi_reply2)
        self.assertEqual(num_of_replies, 2)

    def test_add_two_message_with_different_rsi_to_manager(self):
        replies_manager = rsi.RepliesManager()
        packed = self._build_msg(b'hello0', rsi_length=1)
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, sender_id=1)

        num_of_replies = replies_manager.add_reply(rsi_reply)
        self.assertEqual(num_of_replies, 1)

        packed2 = self._build_msg(b'hello1', rsi_length=1)
        rsi_reply2 = rsi.MsgWithReplicaSpecificInfo(packed2, sender_id=0)

        num_of_replies = replies_manager.add_reply(rsi_reply2)
        self.assertEqual(num_of_replies, 2)

        common_key = rsi_reply.get_matched_reply_key()
        rsi_replies = replies_manager.pop(common_key)
        self.assertEqual(replies_manager.num_matching_replies(common_key), 0)
        self.assertEqual(common_key.header.req_seq_num, 1)
        self.assertEqual(common_key.data, b'hello')
        self.assertEqual(b'1', rsi_replies[0].get_rsi_data())
        self.assertTrue(b'0', rsi_replies[1].get_rsi_data())

    def test_add_message_with_two_seq_num_to_manager(self):
        replies_manager = rsi.RepliesManager()
        packed = self._build_msg(b'hello')
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, sender_id=0)

        num_of_replies = replies_manager.add_reply(rsi_reply)
        self.assertEqual(num_of_replies, 1)

        packed2 = self._build_msg(b'hello', req_seq_num=2)
        rsi_reply2 = rsi.MsgWithReplicaSpecificInfo(packed2, 0)

        num_of_replies = replies_manager.add_reply(rsi_reply2)
        self.assertEqual(num_of_replies, 1)
        self.assertEqual(replies_manager.num_distinct_replies(), 2)

        key1 = rsi_reply.get_matched_reply_key()
        replies_manager.pop(key1)
        self.assertEqual(replies_manager.num_matching_replies(key1), 0)
        self.assertEqual(replies_manager.num_distinct_replies(), 1)

        key2 = rsi_reply2.get_matched_reply_key()
        replies_manager.pop(key2)
        self.assertEqual(replies_manager.num_matching_replies(key2), 0)
        self.assertEqual(replies_manager.num_distinct_replies(), 0)


class TestSpecificReplyInfo(unittest.TestCase):

    def test_create_empty_rsi_message(self):
        msg = b'hello'
        primary_id = 0
        req_seq_num = 1
        packed = bft_msgs.pack_reply(primary_id, req_seq_num, msg, 0, 0)
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, 0)
        self.assertEqual(rsi_reply.sender_id, 0)
        common_header, common_data = rsi_reply.get_common_reply()
        self.assertEqual(req_seq_num, common_header.req_seq_num)
        self.assertEqual(common_data, b'hello')

    def test_create_non_empty_rsi_message(self):
        msg = b'hellorsi'
        primary_id = 0
        req_seq_num = 1
        packed = bft_msgs.pack_reply(primary_id, req_seq_num, msg, 0, 3)
        rsi_reply = rsi.MsgWithReplicaSpecificInfo(packed, 0)
        self.assertEqual(rsi_reply.sender_id, 0)
        common_header, common_data = rsi_reply.get_common_reply()
        self.assertEqual(req_seq_num, common_header.req_seq_num)
        self.assertEqual(rsi_reply.get_rsi_data(), b'rsi')
        self.assertEqual(common_data, b'hello')


class TestPackUnpack(unittest.TestCase):

    def test_unpack_reply(self):
        msg = b'hello'
        primary_id = 0
        req_seq_num = 1
        result = 1
        rsi_length = 5
        packed = bft_msgs.pack_reply(primary_id, req_seq_num, msg, result, rsi_length)
        (header, unpacked_msg) = bft_msgs.unpack_reply(packed)
        self.assertEqual(primary_id, header.primary_id)
        self.assertEqual(req_seq_num, header.req_seq_num)
        self.assertEqual(result, header.result)
        self.assertEqual(rsi_length, header.rsi_length)
        self.assertEqual(msg, unpacked_msg)

    def test_unpack_request(self):
        client_id = 4
        req_seq_num = 1
        read_only = True
        timeout_milli = 5000
        span_context = b'span context'
        msg = b'hello'
        op_result = 5
        cid = str(req_seq_num)

        packed = bft_msgs.pack_request(client_id, req_seq_num, read_only, timeout_milli, cid, msg, op_result,
                                       pre_process=False, span_context=span_context)
        header, unpacked_span_context, unpacked_msg, unpacked_cid = bft_msgs.unpack_request(packed)

        self.assertEqual(len(span_context), header.span_context_size)
        self.assertEqual(client_id, header.client_id)
        self.assertEqual(1, header.flags)  # read_only = True
        self.assertEqual(req_seq_num, header.req_seq_num)
        self.assertEqual(len(msg), header.length)
        self.assertEqual(timeout_milli, header.timeout_milli)
        self.assertEqual(len(cid), header.cid)
        self.assertEqual(op_result, header.op_result)

        self.assertEqual(span_context, unpacked_span_context)
        self.assertEqual(msg, unpacked_msg)
        self.assertEqual(cid, unpacked_cid)

    def test_expect_msg_error(self):
        data = b'someinvalidmsg'
        self.assertRaises(bft_msgs.MsgError, bft_msgs.unpack_request, data)
        self.assertRaises(bft_msgs.MsgError, bft_msgs.unpack_reply, data)

    def test_expect_struct_error(self):
        # Give a valid msg type but invalid data size
        data = b''.join([struct.pack(bft_msgs.MSG_TYPE_FMT,
                                     bft_msgs.REQUEST_MSG_TYPE), b'invalid'])
        self.assertRaises(struct.error, bft_msgs.unpack_request, data)
        data = b''.join([struct.pack(bft_msgs.MSG_TYPE_FMT,
                                     bft_msgs.REPLY_MSG_TYPE),
                         b'invalid'])
        self.assertRaises(struct.error, bft_msgs.unpack_reply, data)


if __name__ == '__main__':
    unittest.main()
