Concord is a message driven system. We want a standard messaging format usable
from multiple languages that also provides a canonical serialization form. The
format must be simple, so that it can be reimplemented in multiple languages in
a matter of hours or days, not weeks or months. The way to do this is to make
the format as limited as possible. It is not nearly as expressible or full
featured as other formats, and it isn't intended to be. It's the bare minimum
and is only expected to be used to implement messages, not arbitrary data
structures.

# Why another message format?

None of the others really met our goals:
 * Protobuf doesn't have a canonical serialization format
 * Msgpack, etc... isn't schema driven
 * Other systems have weak cross language support and some are very complex to implement
 * None of the systems with code generation create idiomatic types that are ergonomic to use in the given language.

# Goals
 * Easy to understand binary format
 * Easy to implement in *any* language
 * Canonical serialization format
 * Ability to implement zero-copy serialization/deserialization if desired. This is implementation dependent.
 * Schema based definition with code generation for each implementation

# Grammar and Implementation
There is a [formal grammar](compiler/grammar.ebnf) in
[ebnf](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form). The grammar is used by a
parser generator in python called [tatsu](https://tatsu.readthedocs.io/en/stable/), that
generates an [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of
the parsed messages according to the grammar. The parser generator finds syntax errors, and some
basic typechecking is done via a [semantics plugin](compiler/semantics.py).

Code generation is performed by walking the AST and generating strings containining the messages as types in the language being generated, as well as serialization and deserialization code for each message. In order to decouple the implementation of the AST from the code generation, where generation for each language may be written by different developers, a [Visitor](compiler/visitor.py) pattern is used. Each code generator implements the visitor for a given language that allow it to take callbacks about specific types and generate the corresponding code. Currently there is only a single [code generator for C++](compiler/cpp/cppgen.py), although python is coming soon.

The great thing about generating code via a visitor, is that [tests can be generated as well](test_cppgen.py)!

# Usage

Messages are defined in concord message format (.cmf) files. For C++ a single `.cmf` file will generate a standalone `.h` file. The only dependency is a C++17 standard library.

Generate C++ code:

```bash
./cmfc.py --input ../example.cmf --output example.h --language cpp --namespace concord::messages
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
 * map - A lexicographically sorted list of key-value pairs
 * oneof - A sum type (tagged union) containing exactly one of the given messages. oneof types cannot contain primitives or compound types, they can only refer to messages. This is useful for deserializing a set of related messages into a given wrapper type. A oneof maps to a `std::variant` in c++.
 * optional - An optional value of any type. An optional maps to a `std::optional` in C++.

## Comments

Comments must be on their own line and start with the `#` character. Leading whitespace is allowed.

 # Serialized Representations

* `bool` - `0x00` = False, `0x01` = True
* `uint8` - The value itself
* `uint16` - The value itself in little endian
* `uint32` - The value itself in little endian
* `uint64` - The value itself in little endian
* `sint8` - The value itself in little endian
* `sint16` - The value itself in little endian
* `sint32` - The value itself in little endian
* `sint64` - The value itself in little endian
* `string` - uint32 length followed by UTF-8 encoded data
* `bytes` - uint32 length followed by arbitrary bytes
* `kvpair` - primitive key followed by primitive or compound value
* `list` - uint32 length of list followed by N homogeneous primitive or compound elements
* `map` - serialized as a list of lexicographically sorted key-value pairs
* `oneof` - uint32 message id of the contained message followed by the message
* `optional` - bool followed by the value

# Schema Format

All messages start with the token `Msg`, followed the message name, the message id, and opening
brace, `{`. Each field is specified with the type name, followed by the field name. After all
field definitions, a closing brace, `}` is added. All types must be *flat*. No nested messsage
definitions are allowed. For nesting, use an existing message name as the type or multiple
compound types.

## Field type formats

### Primitive Types

* `bool <name>`
* `uint8 <name>`
* `uint16 <name>`
* `uint32 <name>`
* `uint64 <name>`
* `sint8 <name>`
* `sint16 <name>`
* `sint32 <name>`
* `sint64 <name>`
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

```
Msg NewViewElement 1 {
    uint16 replica_id
    bytes digest
}

Msg NewView 2 {
  uint16 view
  list NewViewElement element
}

Msg PrePrepare 3 {
    uint64 view
    uint64 sequence_number
    uint16_t flags
    bytes digest
    list bytes client_requests
}

Msg ConsensusMsg 4 {
    uint32 protocol_version
    bytes span
    oneof {
        NewView
        PrePrepare
    } msg_type
}

```
