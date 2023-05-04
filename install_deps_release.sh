#!/bin/bash

# This script installs and builds all the tools needed for building and running Concord-BFT
# It is used for building a docker image that is used by CI/CD and may be used in the
# development environment.
# If you prefer working w/o docker at your dev station just run the script with sudo.
# If you need to add any tool or dependency this is the right place to do it.
# Please bear in mind that we want to keep our list of dependencies as small as possible.
set -ex

APT_GET_FLAGS="-y --no-install-recommends"
WGET_FLAGS="--https-only"

echo ca_certificate=/etc/ssl/certs/ca-certificates.crt > ~/.wgetrc

# Install 3rd parties
install_third_party_libraries() {
        apt-get update && apt-get ${APT_GET_FLAGS} install \
        libbz2-dev \
        liblz4-dev \
        libs3-dev \
        libsnappy-dev \
        libz-dev \
        libzstd-dev

    pip3 install --upgrade wheel && pip3 install --upgrade trio && pip3 install --upgrade pip
    pip3 install \
        eliot eliot-tree \
        tatsu \
        pytest \
        pycryptodome \
        ecdsa \
        protobuf==3.15.8 \
        grpcio==1.37.1 \
        grpcio-tools==1.37.1 \
        psutil==5.9.1 \
        cryptography==3.3.2
}

# Build 3rd parties

install_boost() {
    cd ${HOME}
    git clone --recurse-submodules --depth=1 --single-branch --branch=boost-1.80.0 https://github.com/boostorg/boost.git
    cd boost
    mkdir build
    cd build
    cmake -DBOOST_INCLUDE_LIBRARIES="program_options;thread;locale;asio;lockfree;bimap;histogram" ..
    cmake ..
    make install
    cd ${HOME}
    rm -rf boost
}

install_yaml() {
    cd ${HOME}
    git clone -b yaml-cpp-0.7.0 --single-branch  https://github.com/jbeder/yaml-cpp.git
    mkdir yaml-cpp/build
    cd yaml-cpp/build
    cmake -DYAML_CPP_BUILD_TESTS=OFF -DYAML_CPP_BUILD_TOOLS=OFF -DYAML_BUILD_SHARED_LIBS=OFF ..
    make -j$(nproc) install
    cd ${HOME}
    rm -rf xvzf yaml-cpp
}

install_googletest() {
    cd ${HOME}
    git clone -b v1.12.0 https://github.com/google/googletest.git
    cd googletest
    mkdir build && cd build
    cmake -DCMAKE_CXX_FLAGS="-std=c++11 -march=x86-64 -mtune=generic" ..
    make -j$(nproc) install
    cd ${HOME} && rm -r googletest
}

install_rocksdb_lib() {
    cd ${HOME}
    wget ${WGET_FLAGS} \
        https://github.com/facebook/rocksdb/archive/v6.8.1.tar.gz && \
        tar -xzf v6.8.1.tar.gz && \
        rm v6.8.1.tar.gz && \
        cd rocksdb-6.8.1 && \
        EXTRA_CXXFLAGS="-fno-omit-frame-pointer -Wno-range-loop-construct -Wno-maybe-uninitialized -g " \
        EXTRA_CFLAGS="-fno-omit-frame-pointer -g " \
        PORTABLE=1 make -j$(nproc) USE_RTTI=1 shared_lib && \
        PORTABLE=1 make install-shared && \
        cd ${HOME} && \
        rm -r rocksdb-6.8.1

}

install_rapidcheck() {
    cd ${HOME}
    git clone https://github.com/emil-e/rapidcheck.git && \
        cd rapidcheck && \
        git checkout 258d907da00a0855f92c963d8f76eef115531716 && \
        mkdir build && cd build && \
        cmake -DRC_ENABLE_GTEST=ON -DRC_ENABLE_GMOCK=ON .. && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r rapidcheck

}

# In order to be compatible with the native build
install_minio() {
    cd ${HOME}
    wget ${WGET_FLAGS} https://dl.min.io/server/minio/release/linux-amd64/minio
    mv minio /usr/local/bin
    chmod 755 /usr/local/bin/minio

    pip3 install minio
}

install_openssl() {
    cd ${HOME}
    git clone -b OpenSSL_1_1_1-stable --recurse-submodules https://github.com/openssl/openssl.git
    cd openssl
    ./config --prefix=/usr/local/ --openssldir=/usr/local/ssl shared zlib
    make -j$(nproc) 
    make install_sw install_ssldirs
    cd ${HOME} && rm -rf openssl
}

install_grpc() {
    cd ${HOME}
    git clone -b v1.51.1 https://github.com/grpc/grpc
    cd grpc
    git submodule update --init
     mkdir -p cmake/build
    cd cmake/build
    cmake   -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            -DCMAKE_BUILD_TYPE=Release \
            -DgRPC_BUILD_TESTS=OFF \
            -DgRPC_INSTALL=ON \
            -DgRPC_ZLIB_PROVIDER=package \
            -DgRPC_SSL_PROVIDER=package \
            ../..
    make -j$(nproc)
    make install
    cd ${HOME} && rm -r grpc
}

install_prometheus() {
    cd ${HOME}
    git clone -b v1.1.0 https://github.com/jupp0r/prometheus-cpp.git
    cd prometheus-cpp
    git submodule init
    git submodule update
    mkdir _build
    cd _build
    cmake .. -DBUILD_SHARED_LIBS=OFF -DENABLE_PUSH=OFF -DENABLE_COMPRESSION=OFF -DCMAKE_INSTALL_PREFIX=/usr/local
    cmake --build . --parallel $(nproc)
    cmake --install .
    cd ${HOME} && rm -r prometheus-cpp
}

install_json_lib(){
    cd ${HOME}
    git clone -b v3.11.2 --depth 1 https://github.com/nlohmann/json
    cd json
    mkdir build && cd build
    cmake ..
    make -j$(nproc)
    make install
    cd ${HOME} && rm -r json
}

install_httplib() {
    cd ${HOME}
    git clone -b v0.11.2 --depth 1 https://github.com/yhirose/cpp-httplib
    cd cpp-httplib
    mkdir build && cd build
    cmake -DCMAKE_BUILD_TYPE=Release ..
    cmake --build . --target install
    cd ${HOME} && rm -r cpp-httplib
}

install_jaegertracing_cpp_lib(){
    # TODO: Upgrade to opentelemetry-cpp
    # Tracing via Jaeger and Thrift protocol
    # Copy FindThrift.cmake because installing Thrift does not include a CMake definition 
    #depends on opentracing, thrift, nlohmann, httplib
    apt-get install ${APT_GET_FLAGS} libopentracing-dev thrift-compiler libthrift-dev
    cd $HOME
    git clone -b v0.9.0 https://github.com/jaegertracing/jaeger-client-cpp
    cd jaeger-client-cpp
    git submodule update --init
    mkdir build && cd build
    cp ../cmake/Findthrift.cmake /usr/share/cmake-3.24/Modules/
    cmake     -DHUNTER_ENABLED=OFF -DHUNTER_BUILD_SHARED_LIBS=OFF -DBUILD_TESTING=OFF \
              -DBUILD_SHARED_LIBS=OFF -DJAEGERTRACING_BUILD_EXAMPLES=OFF \
              -DJAEGERTRACING_PLUGIN=OFF -DJAEGERTRACING_COVERAGE=OFF \
              -DJAEGERTRACING_BUILD_CROSSDOCK=OFF -DJAEGERTRACING_WITH_YAML_CPP=OFF \
                ..
  make -j$(nproc) install
  cd ${HOME}
  rm -r jaeger-client-cpp
  sed -i '/boost_components/d' /usr/local/lib/cmake/jaegertracing/jaegertracingConfig.cmake
}

install_third_party_libraries
install_boost
install_yaml
install_googletest
install_rocksdb_lib
install_rapidcheck
install_minio
install_openssl
install_grpc
install_json_lib
install_httplib
install_prometheus
install_jaegertracing_cpp_lib

# After installing all libraries, let's make sure that they will be found at compile time
ldconfig -v
