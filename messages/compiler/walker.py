# Concord
#
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

from exceptions import CmfParseError


def is_primitive(type):
    """Return true if a type is a primitive"""
    return type in [
        "uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32",
        "int64", "bool", "string", "bytes"
    ]


def field_type(type):
    """Extract the outermost type from a field"""
    if not isinstance(type, dict):
        if is_primitive(type):
            return type
        return "msg"
    for compound in ["list", "kvpair", "map", "optional", "oneof"]:
        if compound in type:
            return compound
    raise CmfParseError(type.parseinfo, f"Invalid field type: {type}")


class Walker:
    """ An AST Walker for code generation """
    def __init__(self, ast, visitor):
        self.ast = ast
        self.visitor = visitor

        # A map of msg names to ids for all seen messages while walking the AST.
        self.msgs = dict()

    def walk(self):
        """ Walk the AST and call the visitor to generate code """
        for msg in self.ast.msgs:
            self.msgs[msg.name] = msg.id
            self.visitor.msg_start(msg.name, msg.id)
            for field in msg.fields:
                type = field_type(field.type)
                self.visitor.field_start(field.name, type)
                self.walk_type(field.type)
                self.visitor.field_end()
            self.visitor.msg_end()

    def walk_type(self, type):
        if not isinstance(type, dict):
            if is_primitive(type):
                getattr(self.visitor, type)()
            else:
                self.visitor.msgname_ref(type)
        elif "list" in type:
            self.visitor.list_start()
            self.walk_type(type.list.type)
            self.visitor.list_end()
        elif "kvpair" in type:
            self.visitor.kvpair_start()
            self.walk_type(type.kvpair.key)
            self.visitor.kvpair_key_end()
            self.walk_type(type.kvpair.value)
            self.visitor.kvpair_end()
        elif "map" in type:
            self.visitor.map_start()
            self.walk_type(type.map.key)
            self.visitor.map_key_end()
            self.walk_type(type.map.value)
            self.visitor.map_end()
        elif "optional" in type:
            self.visitor.optional_start()
            self.walk_type(type.optional.type)
            self.visitor.optional_end()
        elif "oneof" in type:
            self.visitor.oneof(
                dict([(n, self.msgs[n]) for n in type.oneof.msg_names]))
        else:
            raise CmfParseError(type.parseinfo, "Invalid field type")
