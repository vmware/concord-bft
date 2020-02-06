#!/bin/bash
cd build
export CC=/usr/bin/clang-7
export CXX=/usr/bin/clang++-7
conan install -s compiler="clang" -s compiler.version="7.0" -s compiler.libcxx=libstdc++11 --build missing ..
cmake -DBUILD_OBJECTSTORE_TEST=1 -DBUILD_TESTING=1 \
-DBUILD_ROCKSDB_STORAGE=1 -USE_OBJECTSTORE_FOR_ROR_TEST=1 $@ ..
make -j8

