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

import struct
import time

from collections import namedtuple

## SimpleKVBC requies fixed size keys and values right now
KV_LEN = 21

READ_LATEST = 0xFFFFFFFFFFFFFFFF

WriteReply = namedtuple('WriteReply', ['success', 'last_block_id'])

class SimpleKVBCProtocol:
    """
    An implementation of the wire protocol for SimpleKVBC requests.
    SimpleKVBC requests are application data embedded inside sbft client
    requests.
    """
    def __init__(self):
        self.READ = 1
        self.WRITE = 2
        self.GET_LAST_BLOCK = 3
        self.GET_BLOCK_DATA = 4

    def write_req(self, readset, writeset, block_id):
        data = bytearray()
        # A conditional write request type
        data.append(self.WRITE)
        # SimpleConditionalWriteHeader
        data.extend(
                struct.pack("<QQQ", block_id, len(readset), len(writeset)))
        # SimpleKey[numberOfKeysInReadSet]
        for r in readset:
            data.extend(r)
        # SimpleKV[numberOfWrites]
        for kv in writeset:
            data.extend(kv[0])
            data.extend(kv[1])

        return data

    def read_req(self, readset, block_id=READ_LATEST):
        data = bytearray()
        data.append(self.READ)
        # SimpleReadHeader
        data.extend(struct.pack("<QQ", block_id, len(readset)))
        # SimpleKey[numberOfKeysToRead]
        for r in readset:
            data.extend(r)
        return data

    def get_last_block_req(self):
        data = bytearray()
        data.append(self.GET_LAST_BLOCK)
        return data

    def get_block_data_req(self, block_id):
        data = bytearray()
        data.append(self.GET_BLOCK_DATA)
        data.extend(struct.pack("<Q", block_id))
        return data

    def parse_reply(self, data):
        reply_type = data[0]
        if reply_type == self.WRITE:
            return self.parse_write_reply(data[1:])
        elif reply_type == self.READ:
            return self.parse_read_reply(data[1:])
        elif reply_type == self.GET_LAST_BLOCK:
            return self.parse_get_last_block_reply(data[1:])
        else:
            raise BadReplyError

    def parse_write_reply(self, data):
        return WriteReply._make(struct.unpack("<?Q", data))

    def parse_read_reply(self, data):
        num_kv_pairs = struct.unpack("<Q", data[0:8])[0]
        data = data[8:]
        kv_pairs = {}
        for i in range(num_kv_pairs):
            kv_pairs[data[0:KV_LEN]] = data[KV_LEN:2*KV_LEN]
            if i+1 != num_kv_pairs:
                data = data[2*KV_LEN:]
        return kv_pairs

    def parse_get_last_block_reply(self, data):
        return struct.unpack("<Q", data)[0]

class Client:
    """A wrapper around bft_client that uses the SimpleKVBCProtocol"""
    def __init__(self, bft_client):
        self.client = bft_client
        self.protocol = SimpleKVBCProtocol()

    async def write(self, readset, writeset, block_id=0):
        """Create an skvbc write message and send it via the bft client."""
        req = self.protocol.write_req(readset, writeset, block_id)
        return self.protocol.parse_reply(await self.client.write(req))

    async def read(self, readset, block_id=READ_LATEST):
        """Create an skvbc read message and send it via the bft client."""
        req = self.protocol.read_req(readset, block_id)
        return self.protocol.parse_reply(await self.client.read(req))
