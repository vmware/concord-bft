# Concord Message Format (CMF)

## Context and Problem Statement

In the context of blockchain, we want to use one message format for the BFT protocol, component level messages, application level messages, and persistence.

## Decision Drivers

* Canonical binary serialization to provide unique serialized messages that are required for cryptographic signatures
* Programming language independent
* Schema driven with code generation
* Idiomatic usage in any language
* Efficient but simple

## Considered Options

* C structs (original communication protocol)
* [Protocol Buffers](https://developers.google.com/protocol-buffers)
* [FlatBuffers](https://google.github.io/flatbuffers/)
* [Cap'n Proto](https://capnproto.org/)
* [MessagePack](https://msgpack.org/index.html)
* [ASN.1](https://www.itu.int/en/ITU-T/asn1/Pages/introduction.aspx)
* [Thrift](https://thrift.apache.org/)
* [Avro](https://avro.apache.org/docs/1.2.0/)
* [Libra Canonical Serialization](https://github.com/libra/libra/tree/6a89e827b95405066dc83eec97eca2cb75bc991d/common/canonical-serialization)
* [Concord Message Format (CMF)](../messages/README.md)

## Decision Outcome

Chosen option: "Concord Message Format (CMF)", because

* It supports all of the decision drivers above.
* It follows the State Machine Replication model where upgrades are "semantic" and "on-chain", not arbitrary.
  - Messages are fixed for the life of a given version across all replicas and clients for a given sequence number of the protocol.
* No other format met all our requirements.
* We had a working prototype that was easy to use and integrate.

### Positive Consequences

* Simple message format
* Easy to implement for any language
* It easily replaces our use of C-structs as they are generated for C++ from CMF messages.
  - Other languages also get native types rather than having to deal with C-structs/ad-hoc structure packing.
* Our messages are well documented by the schema.
* We own the implementation and can evolve the language and upgrade systems through state machine reconfiguration
* Secure - no chance of pointer overflow as with reference based zero-copy impls

### Negative Consequences

* No extensible messages
  - Changing an existing message results in a completely new message.
  - This may make CMF unsuitable for use cases other than our own.
* Ownership implies maintenance cost
* We have to justify why we created our own format

## Pros and Cons of other options

### Packed C Structs

#### Pros

* Easy and idiomatic to use in C/C++
* No serialization/deserialization overhead when implemented carefully

#### Cons

* No schema
* Canonical serialization is implicit, rather than explicit
* Not portable
  - No support for different endianness
  - Messages must be reimplemented for each language by looking at the C-structs
* Easy to make mistakes
  - Using non-portable types like bool or int
  - Hard to visually inspect the struct since it's mixed with construction, destruction, copy, validation code in C++

### Protocol Buffers

#### Pros

* De-facto industry standard for many years
* Extensible, schema based format
* Good support and documentation for many programming languages

#### Cons

* No canonical serialization for binary format
  - Protobuf does support canonical serialization in JSON but this is runtime inefficient
* Complex code-gen
* Provides non-idiomatic usage. It "infects" the code base with protobuf types.

### FlatBuffers

#### Pros

* Zero-copy serialization/deserialization
  - No need to copy data or serialize/deserialize if using flatbuffer types directly
* Extensible, schema based format
* Good support for many programming languages

#### Cons

* No canonical serialization
* Inherently insecure due to zero-copy deserialization
  - Must trust messages or verify received buffers such that:
    - References don't go outside bounds of message
    - References don't infinitely recurse
  - Automatic verification only exists for C++
* No built in support for maps and pairs

### Cap'n Proto

#### Pros

* Zero-copy serialization/deserialization
  - No need to copy data or serialize/deserialize if using cap'n proto types directly
* Extensible, schema based format

#### Cons

* Complex implementation
* Only a few languages supported (C++, Rust, Go)
* Same security issues as Flatbuffers
* Focus on extraneous things like RPC, that are orthogonal to messages
* Infection with non-idiomatic types like protobuf

### MessagePack

#### Pros

* Simple, efficient implementation
* Very good language support
* Focus on messages only

#### Cons

* No support for schemas - more like a binary JSON
* [No canonical serialization](https://github.com/msgpack/msgpack/issues/215)

### ASN.1

#### Pros

* IEEE standard
* Schema based
* Canonical serialization

#### Cons

* Insanely complex
  - Multiple different encoding formats to choose from
  - Tons of options
* Lack of language support/full implementations
* Non-idiomatic types for existing languages
* Non-ergonomic code gen
* Multiple members of our team used it in the past and are strictly against it

### Thrift

### Pros

* Schema/IDL based language
* Good language support

### Cons

* Complex
  - Includes not just messages, but [transport, protocol, etc](https://thrift.apache.org/docs/concepts)
  - Multiple serialization types (non-canonical)
  - [Undecided choices about default values](https://thrift.apache.org/docs/idl#field-requiredness)

### Avro

#### Pros

* Self documenting schemas

#### Cons

* Focused around dynamic languages
* No canonical format
* Coupled with an RPC mechanism
* No codegen

### Libra Canonical Serialization

#### Pros

* Canonical Serialization format
* Efficient but simple
* Language independent

#### Cons

* No schemas
  - language support is performed via declaring native types and adding LCS serializers
  - No codegen makes it harder to build cross language messaging, due to need to copy a "reference" type in another language
* Minimal language support (Rust and Go)
