#!/usr/bin/env python

import math
import os
import random
import sys
from pprint import pprint

sys.path.append("..")
import cppgen
import cmfc
from exceptions import CmfParseError
from visitor import Visitor
from walker import Walker

MAX_SIZE = 5

OUTPUT_DIR = 'TEST_OUTPUT'


def randuint(max_value):
    return str(random.randint(0, max_value))


def randint(min_value, max_value):
    return str(random.randint(min_value, max_value))


def randstring():
    return '"' + random.choice(["a", "b", "c", "aa", "bb", "cc", "abcdef"
                                ]) + '"'


def byte_example():
    return "{0,1,2,3,4,5}"


def instance_name(msg_name, id):
    """Generate the name of an instance from a msg name"""
    return "_" + msg_name.lower() + str(id)


def test_name(msg_name):
    """Generate the name of a serialization unit test given a message name"""
    return "test_{}_serialization".format(msg_name)


def type_instance_from_variable_instance(variable_instance):
    """
    Take a generated variable instance and extract just the instance and type name for use inline
    in other message instances
    """

    # Get the type from the variable declaration
    type_end = variable_instance.index(' ')
    type = variable_instance[0:type_end]

    # Strip off the type and instance name from the variable declaration, as well as the closing semicolon
    instance_start = variable_instance.index('{')
    return type + variable_instance[instance_start:-1]


class InstanceVisitor(Visitor):
    """
    A visitor that generates instantiation of generated types along with serialization and
    deserialization code.
    """
    def __init__(self):
        # How many elements to generate for nested types.
        # This really isn't supported yet...
        self.size = 0
        # A dict keyed by a msg name that contains a set of generated instances of various sizes as strings
        # This dict should be maintained across all instantiatiions of a single visitor
        self.existing_instances = dict()

        # The current msg name of the instance being generated
        self.msg_name = ''

        # The current msg instance being generated as a string
        self.instance = ''

        # All Enum definitions.
        self.enums = dict()


    def create_enum(self, name, tags):
        self.enums[name] = tags

    def msg_start(self, name, id):
        self.msg_name = name
        self.instance = f'{name} {instance_name(name, self.size)}{{'
        if not name in self.existing_instances:
            self.existing_instances[name] = []

    def msg_end(self):
        self.instance += '};'
        self.existing_instances[self.msg_name].append(self.instance)
        self.msg_name = ''
        self.instance = ''

    def field_start(self, name, type):
        pass

    def field_end(self):
        self.instance += ","

    def bool(self):
        self.instance += random.choice(["true", "false"])

    def uint8(self):
        self.instance += randuint(0xFF) + "u"

    def uint16(self):
        self.instance += randuint(0xFFFF) + "u"

    def uint32(self):
        self.instance += randuint(0xFFFFFFFF) + "u"

    def uint64(self):
        self.instance += randuint(0xFFFFFFFFFFFFFFFF) + "u"

    def int8(self):
        self.instance += randint(-128, 127)

    def int16(self):
        self.instance += randint(-32768, 32767)

    def int32(self):
        self.instance += randint(-2147483648, 2147483647)

    def int64(self):
        self.instance += randint(-9223372036854775808, 9223372036854775807)

    def string(self):
        self.instance += randstring()

    def bytes(self):
        self.instance += byte_example()

    def msgname_ref(self, name):
        variable_instance = random.choice(self.existing_instances[name])
        self.instance += type_instance_from_variable_instance(
            variable_instance)

    def kvpair_start(self):
        self.instance += "{"

    def kvpair_key_end(self):
        self.instance += ","

    def kvpair_end(self):
        self.instance += "}"

    def list_start(self):
        self.instance += "{"

    def list_end(self):
        self.instance += "}"

    # Initialize the first std::array element with a random value only, i.e. `instance{value}`.
    # Other values are default-initialized to 0. See `map` for more information about the
    # limitations.
    def fixedlist_start(self):
        self.instance += "{"

    def fixedlist_type_end(self):
        pass

    def fixedlist_end(self, size):
        self.instance += "}"


    # Map instances are tricky to generate. Uniform initialization of maps is done by lists of
    # std::pairs, which themselves are represented as initializer lists. For this reason, we
    # actually need to know when a map starts and ends so we can generate `self.size` numbers
    # of std::pair initializer lists internally. This is further made complicated by the fact
    # that maps can be nested. This latter part is true for lists as well.
    #
    # Unfortunately there is only one callback per type, and so we'd need to build up some
    # datastructure that looks just like the AST being walked so we could easily generate multiple
    # internal pairs. This just happens to be one of the cases where walking the AST directly for
    # code generation is easier than using visitor callbacks. However, we use visitor callbacks to
    # prevent tying us to a specific AST structure and needing to modify every single code
    # generator.
    #
    # What we do because of this is just generate a single KV PAIR for now. Generating a single
    # pair just means using double brackets for map_start and map_end. We can think of more
    # sophisticated strategies later.
    def map_start(self):
        self.instance += "{{"

    def map_key_end(self):
        self.instance += ","

    def map_end(self):
        self.instance += "}}"

    def optional_start(self):
        self.instance += "{"

    def optional_end(self):
        self.instance += "}"

    def oneof(self, msgs):
        name = random.choice(list(msgs.keys()))
        variable_instance = random.choice(self.existing_instances[name])
        self.instance += type_instance_from_variable_instance(
            variable_instance)

    def enum(self, name):
        self.instance += (name + "::" + random.choice(self.enums[name]))

def testSerializationStr(msg_name):
    """
    Create a function that roundtrip serializes and deserializes all instances of a given message type.
    """
    s = "void {}() {{\n".format(test_name(msg_name))
    for i in range(0, MAX_SIZE):
        instance = instance_name(msg_name, i)
        s += f"""
  {{
    {msg_name} {instance}_computed;
    std::vector<uint8_t> output;
    serialize(output, {instance});
    deserialize(output, {instance}_computed);
    assert({instance} == {instance}_computed);
    {msg_name} {instance}_str_computed;
    std::string output_str;
    serialize(output_str, {instance});
    deserialize(output_str, {instance}_str_computed);
    assert({instance} == {instance}_str_computed);
  }}
"""
    s += "}\n"
    return s


def testIntegerSerialization():
    print("Generating integer serialization tests")
    with open("test_integer_serialization.cpp") as f:
        test_code = f.read()
    return test_code


def file_header(namespace):
    return """/***************************************
 Autogenerated by test_cppgen.py. Do not modify.
***************************************/

#include "example.hpp"

#include <cassert>

namespace {} {{

""".format(namespace)


def file_trailer(namespace, ast):
    s = """
}} // namespace {}

int main() {{
""".format(namespace)
    for t in ast:
        for msg in t.msgs or []:
            s += "  {}::{}();\n".format(namespace, test_name(msg.name))
    s += "  cmf::test::test_integer_serialization();\n"
    s += "}"
    return s

def run_command(command):
    assert os.system(command) == 0


def generate_code_and_tests(ast, header_file):
    """ Walk concord message format(CMF) AST and generate C++ code and C++ tests"""
    namespace = "cmf::test"
    print("Generating C++ Message structs and serialization code")
    header, code = cppgen.translate(ast, header_file, namespace)
    test_code = file_header(namespace)
    print("Generating C++ Message instances and serialization tests")
    visitor = InstanceVisitor()
    # We generate `max_size` msg instances for tests
    for i in range(0, MAX_SIZE):
        visitor.size = i
        walker = Walker(ast, visitor)
        walker.walk()
    for msg_name, instances in visitor.existing_instances.items():
        for instance in instances:
            test_code += instance + "\n\n"
        test_code += testSerializationStr(msg_name)
    test_code += testIntegerSerialization()
    return header, code, test_code + file_trailer(namespace, ast)

def compile_cmf_lib():
    print("Compiling CMF lib with g++")
    run_command(
        "g++ -std=c++17 -g -c -fPIC -fsanitize=undefined -fsanitize=address -fsanitize=leak -fno-sanitize-recover "
        f"-o {OUTPUT_DIR}/example.o {OUTPUT_DIR}/example.cpp"
    )
    run_command(
        "g++ -std=c++17 -shared -fsanitize=undefined -fsanitize=address -fsanitize=leak -fno-sanitize-recover "
        f"-o {OUTPUT_DIR}/libexample.so {OUTPUT_DIR}/example.o"
    )

def compile_tests():
    print("Compiling tests with g++")
    run_command(
        "g++ -std=c++17 -g -fsanitize=undefined -fsanitize=address -fsanitize=leak -fno-sanitize-recover "
        f"-o {OUTPUT_DIR}/test_serialization -L{OUTPUT_DIR} {OUTPUT_DIR}/test_serialization.cpp -lexample"
    )


def run_tests():
    print("Running tests")
    os.environ["LD_LIBRARY_PATH"] = OUTPUT_DIR
    run_command(f"./{OUTPUT_DIR}/test_serialization")


def test_serialization():
    """
    1. Generate C++ code for messages from example.cmf and write it to example.h.
    2. Generate instances of the messages as well as tests that round trip serialize and deserialize them.
    3. Run a test to verify integer serialization produces the expected results
    4. Compile that C++ code via g++
    5. Run the compiled C++ code as a test
    """
    with open("../grammar.ebnf") as f:
        print("Reading ../grammar.ebnf")
        grammar = f.read()
        with open("../../example.cmf") as f2:
            print("Reading ../../example.cmf")
            cmf = f2.read()
        ast, _ = cmfc.parse(grammar, cmf)
        # Uncomment to show the generated AST for debugging purposes
        # pprint(ast)
        run_command(f'mkdir -p {OUTPUT_DIR}')
        header, code, tests = generate_code_and_tests(ast, "example.hpp")
        with open(f"{OUTPUT_DIR}/example.hpp", "w") as f:
            f.write(header)
        with open(f"{OUTPUT_DIR}/example.cpp", "w") as f:
            f.write(code)
        with open(f"{OUTPUT_DIR}/test_serialization.cpp", "w") as f:
            f.write(tests)
    compile_cmf_lib()
    compile_tests()
    run_tests()


if __name__ == "__main__":
    test_serialization()
    print("Tests passed.")
