# Concord
#
# Copyright (c) 2022 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

from visitor import Visitor

# TODO: Helper functions for RustVisitor that don't need to be methods of RustVisitor can be declared here.

class RustVisitor(Visitor):
    """ A visitor that generates Rust code. """
    def __init__(self):
        # All output currently constructed
        self.output = ""
        
        # Strings for the name and type for the field currently being processed as this name and type appear in the
        # generated Rust code. Intentionally kept set to None at any time this state should not be considered valid and
        # current.
        self.current_field_name = None
        self.current_field_rust_type = None

        # The CMF oneof type is implemented in Rust via Rust enums; this requires us to generate an enum definition for
        # each distinct oneof type used in the CMF file being processed. These enum definitions (if any) are
        # concatenated into oneof_enum_definitions as this RustVisitor runs. Furthermore, we keep a set of the
        # generated enums' names (which are also generated based on the type of the oneof) in order to avoid
        # duplication of these enum definitions if the same oneof type is used multiple times in the CMF file being
        # processed.
        self.oneof_enum_definitions = ""
        self.oneof_enum_names = set()
        
        # TODO: Initialize all other instance variables needed for code generation by RustVisitor here.

    def _assert_state_at_field_type_processing(self):
        assert (self.current_field_name is not None) and (self.current_field_rust_type is not None), "RustVisitor " \
                "does not have the expected state for the current field being processed at the beginning of a call " \
                "to process a field's type."

    def _cmf_oneof_to_rust_enum(self, msgs):
        """
        Helper method for handling CMF oneof types, which are converted to enums in Rust.

        This method computes and returns the name of the generated Rust enum for a CMF oneof of the messages given in
        msgs, where msgs is a dict(str, int) mapping possible message types to their message IDs (the same
        representation of oneof type used in Visitor.oneof's parameters).

        Furthermore, this method will generate a definition of the Rust enum for the CMF oneof type it is given and
        concatenate it to self.oneof_enum_definitions if this is the first time this particular oneof type has been
        seen.
        """
        assert (len(msgs) > 0), "Attempting to convert CMF oneof with 0 possible variants to Rust enum."

        # TODO: Generate Serializable and Deserializable implementations for enums generated here.
        # TODO: Add disclaimer in some README.md about the limitations on possible oneof types and message names
        #       supported by the CMF Rust implementation as a result of the potential for collisions between Rust enum
        #       names generated here and existing names in use.
        rust_enum_name = "CMFOneOf" + "Or".join(list(msgs))
        if rust_enum_name not in self.oneof_enum_names:
            enum_definition = "enum " + rust_enum_name + " {\n"
            for msg_name in msgs:
                enum_definition += "    " + msg_name + "(" + msg_name + "),\n"
            enum_definition += "}\n"
            self.oneof_enum_definitions += enum_definition + "\n"
            self.oneof_enum_names.add(rust_enum_name)

        return rust_enum_name

    # TODO: Add any additional helper methods RustVisitor needs for implementing Visitor's abstract methods here.

    def create_enum(self, name, tags):
        enum_definition = "enum " + name + "{\n"
        for tag in tags:
            enum_definition += "    " + tag + ",\n"
        enum_definition += "}\n"
        self.output += enum_definition + "\n"

        # TODO: Generate Serializable and Deserializable implementations for this enum.

    def msg_start(self, name, id):
        struct_declaration = "struct " + name + " {"
        self.output += struct_declaration + "\n"

        # TODO: Add code for processing the message id however we want to process it.

        # TODO: Initialize instance variables used to build up Serializable and Deserializable implementations for
        #       this message.

    def msg_end(self):
        self.output += "}\n\n"

        # TODO: Compile/complete Serializable and Deserializable implementations for this message and add them to the
        #       output.

    def field_start(self, name, type):
        assert (self.current_field_name is None) and (self.current_field_rust_type is None), \
                "RustVisitor has unused state from another field at the beginning of a new field_start call."
        self.current_field_name = name
        self.current_field_rust_type = ""

    def field_end(self):

        assert (self.current_field_name is not None) and \
               (self.current_field_rust_type is not None) and \
               (len(self.current_field_rust_type) > 0), \
               "RustVisitor does not have the expected state for the field at the beginning of a field_end call."
        field_declaration = self.current_field_name + ": " + self.current_field_rust_type + ","
        self.output += "    " + field_declaration + "\n"

        # TODO: Update Serializable and Deserializable implementations being built for the current message with the
        #       current field being completed.

        self.current_field_name = None
        self.current_field_rust_type = None

    # TODO: Decide whether CMF message fields should be mutable or not.

    def bool(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "bool"

    def uint8(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "u8"

    def uint16(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "u16"

    def uint32(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "u32"

    def uint64(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "u64"

    def int8(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "i8"

    def int16(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "i16"

    def int32(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "i32"

    def int64(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "i64"

    def string(self):
        self._assert_state_at_field_type_processing()

        # TODO: Decide whether str or String is better for representing strings in CMF.
        self.current_field_rust_type += "String"

    def bytes(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "Vec<u8>"

    def msgname_ref(self, name):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += name

    def kvpair_start(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "("

    def kvpair_key_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += ", "

    def kvpair_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += ")"

    def list_start(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "Vec<"

    def list_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += ">"

    def fixedlist_start(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "["

    def fixedlist_type_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "; "

    def fixedlist_end(self, size):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += str(size) + "]"

    def map_start(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "BTreeMap<"

    def map_key_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += ", "

    def map_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += ">"

    def optional_start(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += "Option<"

    def optional_end(self):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += ">"

    def oneof(self, msgs):
        self._assert_state_at_field_type_processing()
        enum_name = self._cmf_oneof_to_rust_enum(msgs)
        self.current_field_rust_type += enum_name

    def enum(self, name):
        self._assert_state_at_field_type_processing()
        self.current_field_rust_type += name
