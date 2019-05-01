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

class TestPackUnpack(unittest.TestCase):

    def test_unpack_reply(self):
        msg = b'hello'
        primary_id = 0
        req_seq_num = 1
        packed = bft_msgs.pack_reply(primary_id, req_seq_num, msg)
        (header, unpacked_msg) = bft_msgs.unpack_reply(packed)
        self.assertEqual(primary_id, header.primary_id)
        self.assertEqual(req_seq_num, header.req_seq_num)
        self.assertEqual(msg, unpacked_msg)

    def test_unpack_request(self):
        msg = b'hello'
        client_id = 4
        req_seq_num = 1
        read_only = True
        packed = bft_msgs.pack_request(client_id, req_seq_num, read_only, msg)
        (header, unpacked_msg) = bft_msgs.unpack_request(packed)
        self.assertEqual(client_id, header.client_id)
        self.assertEqual(1, header.flags) # read_only = True
        self.assertEqual(req_seq_num, header.req_seq_num)
        self.assertEqual(msg, unpacked_msg)

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
