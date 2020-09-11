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

from visitor import Visitor


def struct_start(name, id):
    return f"""
struct {name} {{
  static constexpr uint32_t id = {id};

"""


def serialize_start(name):
    return f"""
void serialize(std::vector<uint8_t>& output, const {name}& t) {{
"""


def deserialize_start(name):
    return f"""
void deserialize(uint8_t*& input, const uint8_t* end, {name}& t) {{
"""


def deserialize_end(name):
    return f"""}}

void deserialize(const std::vector<uint8_t>& input, {name}& t) {{
    auto* begin = const_cast<uint8_t*>(input.data());
    deserialize(begin, begin + input.size(), t);
}}
"""


def serialize_field(name, type):
    # All messages except oneofs and messages exist in the cmf namespace, and are provided in
    # serialize.h
    if type in ["oneof", "msg"]:
        return f"  serialize(output, t.{name});\n"
    return f"  cmf::serialize(output, t.{name});\n"


def deserialize_field(name, type):
    # All messages except oneofs and messages exist in the cmf namespace, and are provided in
    # serialize.h
    if type in ["oneof", "msg"]:
        return f"  deserialize(input, end, t.{name});\n"
    return f"  cmf::deserialize(input, end, t.{name});\n"


def variant_serialize(variant):
    return f"""\
void serialize(std::vector<uint8_t>& output, const {variant}& val) {{
  std::visit([&output](auto&& arg){{
    cmf::serialize(output, arg.id);
    serialize(output, arg);
  }}, val);
}}"""


def variant_deserialize(variant, msgs):
    s = f"""
void deserialize(uint8_t*& start, const uint8_t* end, {variant}& val) {{
  uint32_t id;
  cmf::deserialize(start, end, id);
"""
    for (name, id) in msgs.items():
        s += f"""
  if (id == {id}) {{
    {name} value;
    deserialize(start, end, value);
    val = value;
    return;
  }}
"""
    s += """
  throw cmf::DeserializeError(std::string("Invalid Message id in variant: ") + std::to_string(id));
}
"""
    return s


def equalop_str(msg_name, fields):
    """ Create an 'operator==' function for the current message struct """
    comparison = " && ".join([f"l.{f} == r.{f}" for f in fields])
    return f"""\
bool operator==(const {msg_name}& l, const {msg_name}& r) {{
  return {comparison};
}}"""


class CppVisitor(Visitor):
    """ A visitor that generates C++ code. """
    def __init__(self):
        # All output currently constructed
        self.output = ""

        # For each message, all four of the following C++ code strings are constructed in the same
        # pass.

        # The current message being processed
        self.msg_name = ''

        # The current field being processed for a message
        self.field = {'type': '', 'name': ''}

        # All fields currently seen for the given message
        self.fields_seen = []

        # The struct being created for the current message. This includes the fields of the struct.
        self.struct = ""

        # The 'serialize' function for the current message
        self.serialize = ""

        # The 'deserialize' function for the current message
        self.deserialize = ""

        # Each oneof in a message corresponds to a variant. Since we don't need duplicate
        # serialization functions, in case there are multiple messages or fields with the same
        # variants, we only generate a single serialization and deserialization function for each
        # variant.
        #
        # This set keeps track of which variants already had their serialization and
        # deserialization functions generated.
        self.oneofs_seen = set()

        # The `serialize` member functions for all oneofs in the current message
        self.oneof_serialize = ""

        # The `deserialize` member functions for all oneofs in the current message
        self.oneof_deserialize = ""

    def _reset(self):
        self.__init__()

    def msg_start(self, name, id):
        self.msg_name = name
        self.struct = struct_start(name, id)
        self.serialize = serialize_start(name)
        self.deserialize = deserialize_start(name)

    def msg_end(self):
        self.struct += "};\n"
        self.serialize += "}"
        self.deserialize += deserialize_end(self.msg_name)
        self.output += "\n".join([
            s for s in [
                self.struct,
                self.oneof_serialize,
                self.oneof_deserialize,
                equalop_str(self.msg_name, self.fields_seen),
                self.serialize,
                self.deserialize,
            ] if s != ''
        ])
        # output and oneofs_seen accumulate across messages
        output = self.output
        oneofs = self.oneofs_seen
        self._reset()
        self.output = output
        self.oneofs_seen = oneofs

    def field_start(self, name, type):
        self.struct += "  "  # Indent fields
        self.field['name'] = name
        self.fields_seen.append(name)
        self.serialize += serialize_field(name, type)
        self.deserialize += deserialize_field(name, type)

    def field_end(self):
        # The field is preceeded by the type in the struct definition. Close it with the name and
        # necessary syntax.
        self.struct += f" {self.field['name']};\n"


### The following callbacks generate types for struct fields, recursively when necessary.

    def bool(self):
        self.struct += "bool"

    def uint8(self):
        self.struct += "uint8_t"

    def uint16(self):
        self.struct += "uint16_t"

    def uint32(self):
        self.struct += "uint32_t"

    def uint64(self):
        self.struct += "uint64_t"

    def int8(self):
        self.struct += "int8_t"

    def int16(self):
        self.struct += "int16_t"

    def int32(self):
        self.struct += "int32_t"

    def int64(self):
        self.struct += "int64_t"

    def string(self):
        self.struct += "std::string"

    def bytes(self):
        self.struct += "std::vector<uint8_t>"

    def msgname_ref(self, name):
        self.struct += name

    def kvpair_start(self):
        self.struct += "std::pair<"

    def kvpair_key_end(self):
        self.struct += ", "

    def kvpair_end(self):
        self.struct += ">"

    def list_start(self):
        self.struct += "std::vector<"

    def list_end(self):
        self.struct += ">"

    def map_start(self):
        self.struct += "std::map<"

    def map_key_end(self):
        self.struct += ", "

    def map_end(self):
        self.struct += ">"

    def optional_start(self):
        self.struct += "std::optional<"

    def optional_end(self):
        self.struct += ">"

    def oneof(self, msgs):
        variant = "std::variant<" + ", ".join(msgs.keys()) + ">"
        self.struct += variant
        oneof = frozenset(msgs.keys())
        if oneof in self.oneofs_seen:
            return
        self.oneofs_seen.add(oneof)
        self.oneof_serialize += variant_serialize(variant)
        self.oneof_deserialize += variant_deserialize(variant, msgs)
