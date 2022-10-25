# Overview

Concord-bft is an implementation of the [SBFT
Protocol](https://arxiv.org/abs/1804.01626). SBFT implements State Machine
Replication (SMR) in the byzantine model. Concord-bft is a generic protocol,
with pluggable interfaces for communication, storage, state transfer, and
abstract state machine execution. This document covers the implementation
of concord-bft. When the implementation differs from the paper, it will be
mentioned explicitly.

It is important to note that this document covers the code as it exists at the
time of writing. When the code changes substantially this document should be
updated.

# Public Interfaces

Concord-bft is a library meant to be used by application code. The external user
API for the core engine is described by a [set of structs and abstract
interfaces](../bftengine/include/bftengine). The file names are pretty self
explanatory. Important structures will be mentioned explicitly below.

All user interfaces live in the `bftEngine` namespace.

# Clients
A default client,
[SimpleClient](../bftengine/include/bftengine/SimpleClient.hpp), is included
with concord-bft. It is just one manner of implementing a client, and is
implemented in C++ utilizing the `ICommunication` and `IReceiver` interfaces
below. Additional clients can be implemented by utilizing the client messages
described in the next subsection. In fact, we have a [client in
Python](../util/pyclient/bft_client.py) that we use for testing that does just
that.

## Client Messages
Client request and reply messages are prefixed with a `ClientRequestMsgHeader` or
`ClientReplyMsgHeader` respectively. Each header, like all other messages in
concord-bft is implemented as a packed C struct, meaning that there are no padding
bytes between struct fields. This is accomplished via enclosing the structure
definitions between `#pragma pack(push,1)` and `#pragma pack(pop)` lines.
Packing ensures that the size of the structure is the sum of the size of the
fields, and allows them to be sent over the wire and interpreted
deterministically, given both machines have the same endianess.

## Communication

The `ICommunication` interface is used by the bftengine code to send
asynchronous messages across the network. It is implemented for different
transport layers such as UDP, TCP, and TLS in
[bftengine/src/communication](../bftengine/src/communication).

The `setReceiver` method is called by the bftengine code to let implementers of
the `ICommunication` interface know where received messages should be handled.
Handlers for received messages implement the `IReceiver` interface. The two sole
implementers of `IReceiver` are the [ReplicaImp](../bftengine/src/bftengine/ReplicaImp.hpp)
and the [SimpleClient](../bftengine/src/bftengine/SimpleClient.cpp)

## State transfer

State transfer refers to the mechanism via which out of date replicas can
retrieve already committed data efficiently and bring themselves in sync with
the rest of the cluster. State transfer is done outside of the ordering and
replication protocol performed by SBFT, and can often be achieved by data transfer
between only two replicas.

As state transfer is outside of the byzantine agreement protocol, it must be
built as a function of the instantiation of the specific state machine
implementation. Concord-bft treats state machine operations and data to be
replicated/executed upon commit as opaque. Since the data being stored by the
application is opaque, and can be arbitrarily large, concord-bft cannot provide
a universally efficient mechanism for transferring such state, and relies on
state machine specific implementations for optimal distribution.

For applications to implement state transfer, and have concord-bft properly be
able to initiate state transfer when needed, and also know when there is more
state that needs to be transferred, there must be hooks in both the application
code and concord-bft code that allow interaction between the application and
bftengine in a generic manner. This is the purpose of the state transfer
interfaces in
[IStateTransfer.hpp](../bftengine/include/bftengine/IStateTransfer.hpp).

The `IStateTransfer` interface is implemented by the application developer for a given
state transfer/checkpoint mechanism. It is called by the main replica thread of
the core bftengine implementation. The concord-bft library contains two
implementations of `IStateTransfer`:
[SimpleStateTransfer](../bftengine/src/simplestatetransfer) and
[BcStateTransfer](../bftengine/src/bcstatetransfer). These will be described
further in the detailed design section for state transfer.

The `IReplicaForStateTransfer` interface allows the state transfer
implementations to call functions in the bftengine replica.

## Metadata Storage

The bftengine has internal metadata that must be persisted to disk. The
[MetadataStorage](../bftengine/include/bftengine/MetadataStorage.hpp) interface
should be implemented by the application developer for a given storage system
such as RocksDB.

## Replica

The [Replica](../bftengine/include/bftengine/Replica.hpp) interface is the API
used to instantiate a new replica given specific implementations of the other
public interfaces.

## RequestsHandler

The [RequestsHandler](../bftengine/include/bftengine/Replica.hpp) interface
contains a single callback function to be implemented by the application and
called by the bftengine when an operation at a given sequence number has
committed. This is the hook that allows arbitrary SMR implemented in concord-bft
to result in execution of application specific operations at all replicas in a
total order.

# ReplicaConfig
ReplicaConfig contains most configurable attributes of concord-bft and should be
created by the application and passed into `Replica::createReplica(...)`.
Note that other configurable attributes are set as part of the communication
interface.
