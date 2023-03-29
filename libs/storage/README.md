# Overview
This directory contains **optional** storage libraries that serve as examples of how
to use the persistent storage interfaces of concord-bft.

The code is structured such that the headers are divided into the individually
usable subsystems of the storage library. `include/storage` contains common code,
including generic database interfaces that are implemented by the `memorydb` and
`rocksdb` components. The `blockchain` code is a layer of code useful for
building blockchain systems that utilize the database interfaces and is usable with
either the `memorydb` or `rocksdb` implementations of these interfaces.

Source code for all subsystems lives in `storage/src`. Applications must
implement the MetadataStorage API, and so we have provided a an implementation
in `storage/src/db_metadata_strorage` that also uses the database interfaces.

Both libraries rely on the util library in the `util` directory for types such as `Status`
and `Sliver`.

# Building and testing

The storage library may optionally use RocksDB. The tests currently require
RocksDB.

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

Configure CMake to build the storage code and tests based on RocksDB.

```shell
cd
cd concord-bft/build
cmake -DBUILD_ROCKSDB_STORAGE=TRUE -DBUILD_TESTING=TRUE ..
make
```

If testing is enabled, cmake will add automated test targets for the storage
library. These can be run individually with with `ctest -R <TEST>` or via `make
test` to run all tests in concord-bft.
