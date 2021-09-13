#!/bin/bash -ex

# Helper script for invoking cmake with various options.
# The list of options is obtained using the following command and are needed to be updated when options change:
# cmake -LA ..| grep -vE "^--|^CMAKE|_DIR|_LIBRARY|NOTFOUND|^LIB|FILEPATH"
#

[[ -d build ]] || mkdir build
cd build

#CC=clang CXX=clang++ \
cmake \
--graphviz=concord-bft.dot \
-DBUILD_COMM_TCP_PLAIN=OFF \
-DBUILD_COMM_TCP_TLS=ON \
-DBUILD_ROCKSDB_STORAGE=ON \
-DBUILD_SLOWDOWN=OFF \
-DBUILD_TESTING=OFF \
-DBUILD_THIRDPARTY=ON \
-DKEEP_APOLLO_LOGS=OFF \
-DLEAKCHECK=OFF \
-DOMIT_TEST_OUTPUT=OFF \
-DTHREADCHECK=OFF \
-DCODECOVERAGE=OFF \
-DTXN_SIGNING_ENABLED=ON \
-DUSE_FAKE_CLOCK_IN_TIME_SERVICE=OFF \
-DUSE_OPENTRACING=OFF \
-DUSE_S3_OBJECT_STORE=OFF \
-DUSE_GRPC=OFF \
-DUSE_OPENSSL=ON \
-DUSE_LOG4CPP=OFF \
..
