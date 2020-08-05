This directory contains code generation for C++.

Code is generated in a single header file that can be included into the consuming project.

 * cppgen.py - The entrypoint for C++ code generation is the `translate` function.
 * cpp_visitor.py - Generate the C++ message structs and serialization code using an implementation of the [Visitor](../visitor.py) abstract base class.
 * serialize.h - Helper C++ code used across all code generation. It's serialization templates for base functions.
 * test_cppgen.py - Generates instances from [example.cmf](../../example.cmf) and roundtrip serialization functions to test them. This function will generate `example.h` from `example.cmf` and also `test_serialization.cpp` containing serialization code. It will then compile `test_serialization.cpp` into `test_serialization` using g++ and run it.
