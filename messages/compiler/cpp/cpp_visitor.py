# Concord
#
# Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
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


serialize_byte_buffer_fn = "void serialize(std::vector<uint8_t>& output, const {name}& t)"


def serialize_byte_buffer_declaration(name):
    return serialize_byte_buffer_fn.format(name=name) + ";\n"


def serialize_byte_buffer_start(name):
    return serialize_byte_buffer_fn.format(name=name) + " {\n"


serialize_string_fn = "void serialize(std::string& output, const {name}& t)"


def serialize_string_declaration(name):
    return serialize_string_fn.format(name=name) + ";\n"


def serialize_string_start(name):
    return serialize_string_fn.format(name=name) + " {\n"


deserialize_fn = "void deserialize(const uint8_t*& input, const uint8_t* end, {name}& t)"


def deserialize_declaration(name):
    return deserialize_fn.format(name=name) + ";\n"


def deserialize_start(name):
    return deserialize_fn.format(name=name) + " {\n"


deserialize_byte_buffer_fn = "void deserialize(const std::vector<uint8_t>& input, {name}& t)"


def deserialize_byte_buffer_declaration(name):
    return deserialize_byte_buffer_fn.format(name=name) + ";\n"


def deserialize_byte_buffer(name):
    return deserialize_byte_buffer_fn.format(name=name) + f""" {{
    auto begin = input.data();
    deserialize(begin, begin + input.size(), t);
}}
"""


deserialize_string_fn = "void deserialize(const std::string& input, {name}& t)"


def deserialize_string_declaration(name):
    return deserialize_string_fn.format(name=name) + ";\n"


def deserialize_string(name):
    return deserialize_string_fn.format(name=name) + f""" {{
    auto begin = reinterpret_cast<const unsigned char*>(input.data());
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


variant_serialize_byte_buffer_fn = "void serialize(std::vector<uint8_t>& output, const {variant}& val)"


def variant_serialize_byte_buffer_declaration(variant):
    return variant_serialize_byte_buffer_fn.format(variant=variant) + ";\n"


def variant_serialize_byte_buffer(variant):
    return variant_serialize_byte_buffer_fn.format(variant=variant) + """ {
  std::visit([&output](auto&& arg){
    cmf::serialize(output, arg.id);
    serialize(output, arg);
  }, val);
}"""


variant_serialize_string_fn = "void serialize(std::string& output, const {variant}& val)"


def variant_serialize_string_declaration(variant):
    return variant_serialize_string_fn.format(variant=variant) + ";\n"


def variant_serialize_string(variant):
    return variant_serialize_string_fn.format(variant=variant) + """ {
  std::visit([&output](auto&& arg){
    cmf::serialize(output, arg.id);
    serialize(output, arg);
  }, val);
}"""


variant_deserialize_fn = "void deserialize(const uint8_t*& start, const uint8_t* end, {variant}& val)"


def variant_deserialize_declaration(variant):
    return variant_deserialize_fn.format(variant=variant) + ";\n"


def variant_deserialize(variant, msgs):
    s = variant_deserialize_fn.format(variant=variant) + """ {
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


equalop_str_fn = "bool operator==(const {msg_name}& l, const {msg_name}& r)"


def equalop_str_declaration(msg_name):
    return equalop_str_fn.format(msg_name=msg_name) + ";\n"


def equalop_str(msg_name, fields):
    """ Create an 'operator==' function for the current message struct """
    comparison = "true"
    if fields:
        comparison = " && ".join([f"l.{f} == r.{f}" for f in fields])
    return equalop_str_fn.format(msg_name=msg_name) + f""" {{
  return {comparison};
}}"""

class CppVisitor(Visitor):
    """ A visitor that generates C++ code. """
    def __init__(self):
        # All output currently constructed
        self.output = ""

        # All output declarations currently constructed
        self.output_declaration = ""

        # The current message being processed
        self.msg_name = ''

        # The current field being processed for a message
        self.field = {'type': '', 'name': ''}

        # All fields currently seen for the given message
        self.fields_seen = []

        # The struct being created for the current message. This includes the fields of the struct.
        self.struct = ""

        # The 'serialize' function for the current message
        self.serialize_byte_buffer = ""
        self.serialize_string = ""

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
        self.oneof_serialize_byte_buffer = ""
        self.oneof_serialize_byte_buffer_declaration = ""
        self.oneof_serialize_string = ""
        self.oneof_serialize_string_declaration = ""

        # The `deserialize` member functions for all oneofs in the current message
        self.oneof_deserialize = ""
        self.oneof_deserialize_declaration = ""

    def _reset(self):
        # output and oneofs_seen accumulate across messages
        output = self.output
        output_declaration = self.output_declaration
        oneofs = self.oneofs_seen
        self.__init__()
        self.output = output
        self.output_declaration = output_declaration
        self.oneofs_seen = oneofs

    def create_enum(self, name, tags):
        enumstr = 'enum class {name} : uint8_t {{ {tagstr} }};\n'
        enumsize_decl = 'uint8_t enumSize({name} _);\n'
        enumsize_def = 'uint8_t enumSize({name} _) {{ (void)_; return {num_tags}; }}\n'
        self.output_declaration += enumstr.format(name=name, tagstr=", ".join(tags))
        self.output_declaration += enumsize_decl.format(name=name)
        self.output += enumsize_def.format(name=name, num_tags = len(tags))

    def msg_start(self, name, id):
        self.msg_name = name
        self.struct = struct_start(name, id)
        self.serialize_byte_buffer = serialize_byte_buffer_start(name)
        self.serialize_string = serialize_string_start(name)
        self.deserialize = deserialize_start(name)

    def msg_end(self):
        self.struct += "};\n"
        self.serialize_byte_buffer += "}"
        self.serialize_string += "}"
        self.deserialize += "}\n"
        self.deserialize += deserialize_byte_buffer(self.msg_name)
        self.deserialize += "\n"
        self.deserialize += deserialize_string(self.msg_name)
        self.output += "\n".join([
            s for s in [
                self.oneof_serialize_byte_buffer,
                self.oneof_serialize_string,
                self.oneof_deserialize,
                equalop_str(self.msg_name, self.fields_seen),
                self.serialize_byte_buffer,
                self.serialize_string,
                self.deserialize,
            ] if s != ''
        ]) + "\n"
        self.output_declaration += "".join([
            s for s in [
                self.struct,
                "\n",
                serialize_byte_buffer_declaration(self.msg_name),
                serialize_string_declaration(self.msg_name),
                deserialize_declaration(self.msg_name),
                deserialize_byte_buffer_declaration(self.msg_name),
                deserialize_string_declaration(self.msg_name),
                self.oneof_serialize_byte_buffer_declaration,
                self.oneof_serialize_string_declaration,
                self.oneof_deserialize_declaration,
                equalop_str_declaration(self.msg_name),
            ] if s != ''
        ]) + "\n"
        self._reset()

    def field_start(self, name, type):
        self.struct += "  "  # Indent fields
        self.field['name'] = name
        self.fields_seen.append(name)
        self.serialize_byte_buffer += serialize_field(name, type)
        self.serialize_string += serialize_field(name, type)
        self.deserialize += deserialize_field(name, type)

    def field_end(self):
        # The field is preceeded by the type in the struct definition. Close it with the name and
        # necessary syntax.
        self.struct += f" {self.field['name']}{{}};\n"


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

    def fixedlist_start(self):
        self.struct += "std::array<"

    def fixedlist_type_end(self):
        self.struct += ", "

    def fixedlist_end(self, size):
        self.struct += f"{size}>"

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
        self.oneof_serialize_byte_buffer += variant_serialize_byte_buffer(variant)
        self.oneof_serialize_string += variant_serialize_string(variant)
        self.oneof_serialize_byte_buffer_declaration += \
          variant_serialize_byte_buffer_declaration(variant)
        self.oneof_serialize_string_declaration += \
          variant_serialize_string_declaration(variant)
        self.oneof_deserialize += variant_deserialize(variant, msgs)
        self.oneof_deserialize_declaration += variant_deserialize_declaration(variant)

    def enum(self, type_name):
        self.struct += type_name
