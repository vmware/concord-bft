Concord is a message driven system. We want a standard messaging format usable
from multiple languages that also provides a canonical serialization form. The
format must be simple, so that it can be reimplemented in multiple languages in
a matter of hours or days, not weeks or months. The way to do this is to make
the format as limited as possible. It is not nearly as expressible or full
featured as other formats, and it isn't intended to be. It's the bare minimum
and is only expected to be used to implement messages, not arbitrary data
structures.

# Why another message format?

[Architecture decision record](../docs/adr-cmf.md)

# Grammar and Implementation
There is a [formal grammar](compiler/grammar.ebnf) in
[ebnf](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form). The grammar is used by a
parser generator in python called [tatsu](https://tatsu.readthedocs.io/en/stable/), that
generates an [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of
the parsed messages according to the grammar. The parser generator finds syntax errors, and some
basic typechecking is done via a [semantics plugin](compiler/semantics.py).

Code generation is performed by walking the AST and generating strings containining the messages as types in the language being generated, as well as serialization and deserialization code for each message. In order to decouple the implementation of the AST from the code generation, where generation for each language may be written by different developers, a [Visitor](compiler/visitor.py) pattern is used. Each code generator implements the visitor for a given language that allow it to take callbacks about specific types and generate the corresponding code.

The great thing about generating code via a visitor, is that [tests can be generated as well](test_cppgen.py)!

# Language Implementations

 * [C++](compiler/cpp/cppgen.py)
 * [Python](compiler/python/pygen.py)

# Usage

## C++

Messages are defined in concord message format (.cmf) files. For C++ a single `.cmf` file will generate corresponding `.hpp` and `.cpp` files. The only dependency is a C++17 standard library.

Generate C++ code:

```bash
./cmfc.py --input ../example.cmf --output example --language cpp --namespace concord::messages
```

Test C++ code generation. The following:
 1. Generates serialization code for [example.cmf](example.cmf)
 2. Generates instances of the structs from the generated example.h using uniform initialization
 3. Generates tests functions that round trip serialize and deserialize the instances
 4. Compiles the test code using g++
 5. Runs the tests

```bash
cd compiler/cpp
./test_cppgen.py
```

## Python

Generate Python code:

```bash
./cmfc.py --input ../example.cmf --output example --language python
```


# Data Types
## Primitive Data Types

* bool
* unsigned integers -  uint8, uint16, uint32, uint64
* signed integers - int8, int16, int32, int64
* string - UTF-8 encoded strings
* bytes -  an arbitrary byte buffer

## Compound Data Types

Compound data types may include primitive types and other compound types. We ensure canonical serialization by ordering

 * kvpair - Keys must be primitive types. Values can be any type
 * list - A homogeneous list of any type
 * fixedlist - A homogeneous fixed-size list of any type. Maps to `std::array` in C++. Note that `std::array` types are stored on the stack.
 * map - A lexicographically sorted list of key-value pairs
 * oneof - A sum type (tagged union) containing exactly one of the given messages. oneof types cannot contain primitives or compound types, they can only refer to messages. This is useful for deserializing a set of related messages into a given wrapper type. A oneof maps to a `std::variant` in c++.
 * optional - An optional value of any type. An optional maps to a `std::optional` in C++.
 * enum - An enumerated list of tags. The underlying representation is a uint8.

 ## C++ Struct Member Initialization
 C++ struct members are value-initialized via `{}`, effectively:
* initializing integer types to 0
* initializing booleans to false
* value-initializing std::pair members
* value-initializing std::array members

## Comments

Comments must be on their own line and start with the `#` character. Leading whitespace is allowed.

 # Serialized Representations

* `bool` - `0x00` = False, `0x01` = True
* `uint8` - The value itself
* `uint16` - The value itself
* `uint32` - The value itself
* `uint64` - The value itself
* `int8` - The value itself
* `int16` - The value itself
* `int32` - The value itself
* `int64` - The value itself
* `string` - uint32 length followed by UTF-8 encoded data
* `bytes` - uint32 length followed by arbitrary bytes
* `kvpair` - primitive key followed by primitive or compound value
* `list` - uint32 length of list followed by N homogeneous primitive or compound elements
* `fixedlist` - N homogeneous primitive or compound elements
* `map` - serialized as a list of lexicographically sorted key-value pairs
* `oneof` - uint32 message id of the contained message followed by the message
* `optional` - bool followed by the value
* `enum` - The value itself as a uint8

Integer values are serialized in `big-endian` byte order.

# Schema Format

There are two top-level types: `Msg` and `Enum`. Enums and Msgs share a namespace and therefore
they must have distinct names.

All messages start with the token `Msg`, followed the message name, the message id, and opening
brace, `{`. Each field is specified with the type name, followed by the field name. After all
field definitions, a closing brace, `}` is added. All types must be *flat*. No nested messsage
definitions are allowed. For nesting, use an existing message name as the type or multiple
compound types.

An Enum is a type containing a choice of distinct tags. It can be used as a field in one or more `Msg`s.

Previsously defined Enums and Msgs can be directly referred to by name in a field.

```
Msg DirectRefs 3 {
    SomeMsg some_msg
    SomeEnum some_enum
}
```

## Field type formats

### Primitive Types

* `bool <name>`
* `uint8 <name>`
* `uint16 <name>`
* `uint32 <name>`
* `uint64 <name>`
* `int8 <name>`
* `int16 <name>`
* `int32 <name>`
* `int64 <name>`
* `string <name>`
* `bytes <name>`

### Compound Types

* `kvpair <primitive_key_type> <val_type> <name>`

Keys of kvpairs must be primitive types. Values can be compound types. Therefore, it's permissible to have field definitions like the following:

```
kvpair uint64 list string user_tags
```

* `list <type> name`

Lists are homogeneous, but be made up of any type. Therefore, it's permissible to have field definitions like the following:

```
list kvpair int string users
```

* `fixedlist <type> <size> name`

Fixedlists are very similar to lists, but are of a fixed size:

```
fixedlist uint8 32 hash
```

* `map <primitive_key_type> <val_type> name`

Similar to kvpairs, and lists, map values may contain compound types. Therefore, it's permissible to have field definitions like the following.

```
map string map string uint64 employee_salaries_by_company
```

* `oneof { <message_name_1> <message_name_2> ... <message_name_N> }`

A oneof can only contain message names.

* `optional <type> name`

An optional may contain a value of a given type or not.

## Example
See [example.cmf](example.cmf)
