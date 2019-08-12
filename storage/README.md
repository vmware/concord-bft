# Overview
This directory contains **optional** storage libraries that serve as examples of how
to use the persistent storage interfaces of concord-bft.

Applications must implement the MetadataStorage API, and so we have provided a
RocksDB implementation in the `rocksdb` directory. The rocksdb directory also
contains a `rocksdb_client` that implements the interfaces in
`database_interface.h`. This can be used by higher level storage code to build
complete systems on top of RocksDB. Again, this is only one way to use RocksDB
to build a system, and the rocksdb library is completely optional.

Additionally, users may wish to build a blockchain on top of concord-bft. The
`blockchain` directory contains an example of how to structure the keyspace and
use the database interfaces to build a blockchain storage layer. Currently, the
only provided implementation of the database interfaces is the rocksdb library,
and it is required for linking. However, users can provide alternate
implementations of these interfaces and use a different low level storage
mechanism, making the blockchain code independent of the rocksdb library.

Both libraries rely on the util library in `../util` for types such as `Status`
and `Sliver`.

# Building and testing

The storage libraries rely on RocksDB.

First install RocksDB dependencies:

```shell
sudo apt-get install libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
```

Build and install RocksDB:

```shell
cd
wget https://github.com/facebook/rocksdb/archive/v5.7.3.tar.gz
tar -xzf v5.7.3.tar.gz
cd rocksdb-5.7.3
make shared_lib
sudo make install-shared
```

Configure CMake to build the storage code and tests based on RocksDB. Some tests
require LOG4CPP, so that also must be installed and enabled. Installation of
LOG4CPP is detailed in the [main
README](https://github.com/vmware/concord-bft/blob/master/README.md).

```shell
cd
cd concord-bft/build
cmake -DBUILD_ROCKSDB_STORAGE=TRUE -DBUILD_TESTING=TRUE -DUSE_LOG4CPP=TRUE ..
make
```

If testing is enabled, cmake will add automated test targets for the storage
library. These can be run individually with with `ctest -R <TEST>` or via `make
test` to run all tests in concord-bft.
