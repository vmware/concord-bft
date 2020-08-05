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

import os
from exceptions import CmfParseError
from cpp.cpp_visitor import CppVisitor
from walker import Walker


def file_header(namespace):
    if namespace:
        return f"namespace {namespace} {{\n"
    return ''


def file_trailer(namespace):
    if namespace:
        return f"\n}} // namespace {namespace}\n"
    return "\n"


def translate(ast, namespace=None):
    """
    Walk concord message format(CMF) AST and generate C++ code.

    Return C++ code as a string.
    """
    with open(os.path.join(os.path.dirname(__file__), "serialize.h")) as f:
        cmf_base_serialization = f.read()
    s = cmf_base_serialization + '\n' + file_header(namespace)
    visitor = CppVisitor()
    walker = Walker(ast, visitor)
    walker.walk()
    return s + visitor.output + file_trailer(namespace)
