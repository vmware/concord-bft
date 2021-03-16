# Concord
#
# Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the 'License').
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import enum
import struct


def is_primitive(s):
    return s in [
        'bool', 'string', 'bytes', 'uint8', 'uint16', 'uint32', 'uint64',
        'int8', 'int16', 'int32', 'int64'
    ]


class CmfSerializeError(Exception):
    def __init__(self, msg):
        self.message = f'CmfSerializeError: {msg}'

    def __str__(self):
        return self.message


class CmfDeserializeError(Exception):
    def __init__(self, msg):
        self.message = f'CmfDeserializeError: {msg}'

    def __str__(self):
        return self.message


class NoDataLeftError(CmfDeserializeError):
    def __init__(self):
        super().__init__(
            'Data left in buffer is less than what is needed for deserialization'
        )


class BadDataError(CmfDeserializeError):
    def __init__(self, expected, actual):
        super().__init__(f'Expected {expected}, got {actual}')


class CMFSerializer():
    def __init__(self):
        self.buf = bytearray()

    def validate_int(self, val, min, max):
        if not type(val) is int:
            raise CmfSerializeError(f'Expected integer value, got {type(val)}')
        if val < min:
            raise CmfSerializeError(
                f'Expected integer value less than {min}, got {val}')
        if val > max:
            raise CmfSerializeError(
                f'Expected integer value less than {max}, got {val}')

    def serialize(self, val, serializers, fixed_size=None):
        '''
        Serialize any nested types by applying the methods in `serializers` at each level.
        This method interacts with those below in a mutually recursive manner for nested types.
        '''
        s = serializers[0]
        if s in ['fixedlist'] and len(serializers) > 1:
            getattr(self, s)(val, serializers[1:], fixed_size)
        elif s in ['list', 'optional'] and len(serializers) > 1:
            getattr(self, s)(val, serializers[1:])
        elif s in ['kvpair', 'map'] and len(serializers) > 2:
            getattr(self, s)(val, serializers[1:])
        elif type(s) is tuple and len(s) == 2 and s[0] == 'oneof' and type(
                s[1]) is dict:
            self.oneof(val, s[1])
        elif type(s) is tuple and len(s) == 2 and s[0] == 'msg' and type(
                s[1]) is str:
            self.msg(val, s[1])
        elif type(s) is tuple and len(s) == 2 and s[0] == 'enum' and type(
                s[1]) is str:
            self.enum(val, s[1])
        elif is_primitive(s):
            getattr(self, s)(val)
        else:
            raise CmfSerializeError(f'Invalid serializer: {s}, val = {val}')

    ###
    # Serialization functions for types that compose fields
    ###
    def bool(self, val):
        if not type(val) is bool:
            raise CmfSerializeError(f'Expected bool, got {type(val)}')
        if val:
            self.buf.append(1)
        else:
            self.buf.append(0)

    def uint8(self, val):
        self.validate_int(val, 0, 255)
        self.buf.extend(struct.pack('B', val))

    def uint16(self, val):
        self.validate_int(val, 0, 65536)
        self.buf.extend(struct.pack('>H', val))

    def uint32(self, val):
        self.validate_int(val, 0, 4294967296)
        self.buf.extend(struct.pack('>I', val))

    def uint64(self, val):
        self.validate_int(val, 0, 18446744073709551616)
        self.buf.extend(struct.pack('>Q', val))

    def int8(self, val):
        self.validate_int(val, -128, 127)
        self.buf.extend(struct.pack('b', val))

    def int16(self, val):
        self.validate_int(val, -32768, 32767)
        self.buf.extend(struct.pack('>h', val))

    def int32(self, val):
        self.validate_int(val, -2147483648, 2147483647)
        self.buf.extend(struct.pack('>i', val))

    def int64(self, val):
        self.validate_int(val, -9223372036854775808, 9223372036854775807)
        self.buf.extend(struct.pack('>q', val))

    def string(self, val):
        if not type(val) is str:
            raise CmfSerializeError(f'Expected string, got {type(val)}')
        self.uint32(len(val))
        self.buf.extend(bytes(val, 'utf-8'))

    def bytes(self, val):
        if not type(val) in [bytes, bytearray]:
            raise CmfSerializeError(f'Expected bytes, got {type(val)}')
        self.uint32(len(val))
        self.buf.extend(val)

    def msg(self, msg, msg_name):
        if msg.__class__.__name__ != msg_name:
            raise CmfSerializeError(
                f'Expected {msg_name}, got {msg.__class__.__name__}')
        self.buf.extend(msg.serialize())

    def kvpair(self, pair, serializers):
        if not type(pair) is tuple:
            raise CmfSerializeError(f'Expected tuple, got {type(pair)}')
        self.serialize(pair[0], serializers)
        self.serialize(pair[1], serializers[1:])

    def list(self, items, serializers):
        if not type(items) is list:
            raise CmfSerializeError(f'Expected list, got {type(items)}')
        self.uint32(len(items))
        for val in items:
            self.serialize(val, serializers)

    def fixedlist(self, items, serializers, fixed_size):
        if not type(items) is list:
            raise CmfSerializeError(f'Expected list, got {type(items)}')
        if len(items) != fixed_size:
            raise CmfSerializeError(f'Expected list size of {fixed_size}, got {len(items)}')
        for val in items:
            self.serialize(val, serializers)

    def map(self, dictionary, serializers):
        if not type(dictionary) is dict:
            raise CmfSerializeError(f'Expected dict, got {type(dictionary)}')
        self.uint32(len(dictionary))
        for k, v in sorted(dictionary.items()):
            self.serialize(k, serializers)
            self.serialize(v, serializers[1:])

    def optional(self, val, serializers):
        if val is None:
            self.bool(False)
        else:
            self.bool(True)
            self.serialize(val, serializers)

    def oneof(self, val, msgs):
        if val.__class__.__name__ in msgs.keys():
            self.uint32(val.id)
            self.buf.extend(val.serialize())
        else:
            raise CmfSerializeError(
                f'Invalid msg in oneof: {val.__class__.__name__}')

    def enum(self, val, name):
        if val.__class__.__name__ != name:
            raise CmfSerializeError(
                f'Expected {name}, got {val.__class__.__name__}')
        if not isinstance(val, enum.Enum):
            raise CmfSerializeError(f'{val} of class {name} is not an Enum')
        self.uint8(val.value)


class CMFDeserializer():
    def __init__(self, buf):
        self.buf = buf
        self.pos = 0

    def deserialize(self, serializers, fixed_size=None):
        '''
        Recursively deserialize `self.buf` using `serializers`
        '''
        s = serializers[0]
        if s in ['fixedlist'] and len(serializers) > 1:
            return getattr(self, s)(serializers[1:], fixed_size)
        elif s in ['list', 'optional'] and len(serializers) > 1:
            return getattr(self, s)(serializers[1:])
        elif s in ['kvpair', 'map'] and len(serializers) > 2:
            return getattr(self, s)(serializers[1:])
        elif type(s) is tuple and len(s) == 2 and s[0] == 'oneof' and type(
                s[1]) is dict:
            return self.oneof(s[1])
        elif type(s) is tuple and len(s) == 2 and s[0] == 'msg' and type(
                s[1]) is str:
            return self.msg(s[1])
        elif type(s) is tuple and len(s) == 2 and s[0] == 'enum' and type(
                s[1]) is str:
            return self.enum(s[1])
        elif is_primitive(s):
            return getattr(self, s)()
        else:
            raise CmfDeserializeError(f'Invalid serializer: {s}')

    def bool(self):
        if self.pos + 1 > len(self.buf):
            raise NoDataLeftError()
        val = self.buf[self.pos]
        if val == 1:
            self.pos += 1
            return True
        elif val == 0:
            self.pos += 1
            return False
        raise BadDataError('0 or 1', val)

    def uint8(self):
        if self.pos + 1 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('B', self.buf, self.pos)
        self.pos += 1
        return val[0]

    def uint16(self):
        if self.pos + 2 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('>H', self.buf, self.pos)
        self.pos += 2
        return val[0]

    def uint32(self):
        if self.pos + 4 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('>I', self.buf, self.pos)
        self.pos += 4
        return val[0]

    def uint64(self):
        if self.pos + 8 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('>Q', self.buf, self.pos)
        self.pos += 8
        return val[0]

    def int8(self):
        if self.pos + 1 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('b', self.buf, self.pos)
        self.pos += 1
        return val[0]

    def int16(self):
        if self.pos + 2 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('>h', self.buf, self.pos)
        self.pos += 2
        return val[0]

    def int32(self):
        if self.pos + 4 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('>i', self.buf, self.pos)
        self.pos += 4
        return val[0]

    def int64(self):
        if self.pos + 8 > len(self.buf):
            raise NoDataLeftError()
        val = struct.unpack_from('>q', self.buf, self.pos)
        self.pos += 8
        return val[0]

    def string(self):
        size = self.uint32()
        if self.pos + size > len(self.buf):
            raise NoDataLeftError()
        val = str(self.buf[self.pos:self.pos + size], 'utf-8')
        self.pos += size
        return val

    def bytes(self):
        size = self.uint32()
        if self.pos + size > len(self.buf):
            raise NoDataLeftError()
        val = self.buf[self.pos:self.pos + size]
        self.pos += size
        return val

    def msg(self, msg_name):
        cls = globals()[msg_name]
        val, bytes_read = cls.deserialize(self.buf[self.pos:])
        self.pos += bytes_read
        return val

    def kvpair(self, serializers):
        key = self.deserialize(serializers)
        val = self.deserialize(serializers[1:])
        return (key, val)

    def list(self, serializers):
        size = self.uint32()
        return [self.deserialize(serializers) for _ in range(0, size)]

    def fixedlist(self, serializers, fixed_size):
        return [self.deserialize(serializers) for _ in range(0, fixed_size)]

    def map(self, serializers):
        size = self.uint32()
        # We can't use a dict comprehension here unless we rely on python 3.8, since order of
        # evaluation of dict comprehensions constructs values first.
        # See: https://stackoverflow.com/questions/42201932/order-of-operations-in-a-dictionary-comprehension
        rv = dict()
        for _ in range(0, size):
            key = self.deserialize(serializers)
            val = self.deserialize(serializers[1:])
            rv[key] = val
        return rv

    def optional(self, serializers):
        if not self.bool():
            return None
        return self.deserialize(serializers)

    def oneof(self, msgs):
        id = self.uint32()
        if id not in msgs.values():
            raise CmfDeserializeError(f'Invalid msg id for oneof: {id}')
        for name, msg_id in msgs.items():
            if msg_id == id:
                return self.msg(name)

    def enum(self, name):
        cls = globals()[name]
        value = self.uint8()
        return cls(value)
