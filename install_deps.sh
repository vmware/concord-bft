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

# Install tools
install_build_tools() {
    apt-get update && apt-get ${APT_GET_FLAGS} install \
        autoconf \
        automake \
        bison \
        build-essential \
        clang-9 \
        clang-format-10 \
        clang-tidy-10 \
        curl \
        flex \
        gdb \
        gdbserver \
        git \
        iproute2 \
        iptables \
        less \
        libtool \
        llvm-9 \
        lzip \
        net-tools \
        parallel \
        pkg-config \
        psmisc \
        python3-pip \
        python3-setuptools \
        python3-dev \
        sudo \
        vim \
        wget

    update-alternatives --install /usr/bin/clang clang /usr/lib/llvm-9/bin/clang 100
    update-alternatives --install /usr/bin/clang++ clang++ /usr/lib/llvm-9/bin/clang++ 100
    update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-10 100
    update-alternatives --install /usr/bin/clang-format-diff clang-format-diff /usr/bin/clang-format-diff-10 100
    update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-10 100
    update-alternatives --install /usr/bin/llvm-symbolizer llvm-symbolizer /usr/lib/llvm-9/bin/llvm-symbolizer 100
    update-alternatives --install /usr/bin/llvm-profdata llvm-profdata /usr/lib/llvm-9/bin/llvm-profdata 100
    update-alternatives --install /usr/bin/llvm-cov llvm-cov /usr/lib/llvm-9/bin/llvm-cov 100

}

# Install 3rd parties
install_third_party_libraries() {
    apt-get ${APT_GET_FLAGS} install \
        libbz2-dev \
        liblz4-dev \
        libs3-dev \
        libsnappy-dev \
        libssl-dev \
        libz-dev \
        libzstd-dev

    pip3 install --upgrade wheel && pip3 install --upgrade trio && pip3 install --upgrade pip
    pip3 install \
        eliot eliot-tree \
        tatsu==4.4.0 \
        pytest \
        pycryptodome \
        ecdsa \
        protobuf==3.15.8 \
        grpcio==1.37.1 \
        grpcio-tools==1.37.1 \
        cryptography==3.3.2
}


# Build 3rd parties
install_cmake() {
    wget ${WGET_FLAGS} -O cmake-linux.sh \
    https://github.com/Kitware/CMake/releases/download/v3.24.2/cmake-3.24.2-linux-x86_64.sh &&\
    sh cmake-linux.sh -- --skip-license --prefix=/usr
}

install_boost() {
    cd ${HOME}
    git clone --recurse-submodules --depth=1 --single-branch --branch=boost-1.80.0 https://github.com/boostorg/boost.git
    cd boost
    mkdir build
    cd build
    cmake -DBOOST_INCLUDE_LIBRARIES="program_options;thread;locale;asio;lockfree;bimap" ..
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

install_HdrHistogram_lib() {
    cd ${HOME}
    wget ${WGET_FLAGS} \
        https://github.com/HdrHistogram/HdrHistogram_c/archive/0.9.12.tar.gz && \
        tar -xzf 0.9.12.tar.gz && \
        rm 0.9.12.tar.gz && \
        cd HdrHistogram_c-0.9.12 && \
        mkdir build && cd build && \
        cmake .. && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r HdrHistogram_c-0.9.12

}

install_log4cpp_lib() {
    cd ${HOME}
    wget ${WGET_FLAGS} \
        https://github.com/log4cplus/log4cplus/releases/download/REL_2_0_4/log4cplus-2.0.4.tar.gz && \
        tar -xzf log4cplus-2.0.4.tar.gz && \
        rm log4cplus-2.0.4.tar.gz && \
        cd log4cplus-2.0.4 && \
        autoreconf -f -i && \
        ./configure CXXFLAGS="--std=c++11 -march=x86-64 -mtune=generic" \
                    --enable-static && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r log4cplus-2.0.4

}


install_googletest() {
    cd ${HOME}
    git clone https://github.com/google/googletest.git && \
        cd googletest && \
        git checkout e93da23920e5b6887d6a6a291c3a59f83f5b579e && \
        mkdir _build && cd _build && \
        cmake -DCMAKE_CXX_FLAGS="-std=c++11 -march=x86-64 -mtune=generic" .. && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r googletest

    cd ${HOME}
    git clone https://github.com/google/benchmark.git && \
      cd benchmark && \
      git checkout f91b6b42b1b9854772a90ae9501464a161707d1e && \
      cmake -E make_directory "build" && \
      cmake -E chdir "build" cmake -DBENCHMARK_DOWNLOAD_DEPENDENCIES=on -DCMAKE_BUILD_TYPE=Release ../ && \
      cmake --build "build" --config Release && \
      cmake --build "build" --config Release --target install
      cd ${HOME} && \
      rm -r benchmark

}

install_gmp_lib() {
    cd ${HOME}
    wget ${WGET_FLAGS} https://gmplib.org/download/gmp/gmp-6.1.2.tar.lz && \
        tar --lzip -xf gmp-6.1.2.tar.lz && \
        cd gmp-6.1.2/ && \
        ./configure --with-pic --enable-cxx --disable-fat --build x86_64-linux-gnu && \
        make -j$(nproc) && \
        make check && \
        make install && \
        ldconfig && \
        cd ${HOME} && \
        rm -r gmp-6.1.2.tar.lz gmp-6.1.2

}

install_relic_toolkit() {
    cd ${HOME}
    git clone https://github.com/relic-toolkit/relic && \
        cd relic && \
        git checkout 0998bfcb6b00aec85cf8d755d2a70d19ea3051fd && \
        mkdir build && \
        cd build && \
        cmake   -DALLOC=AUTO -DWSIZE=64 \
                -DWORD=64 -DRAND=UDEV \
                -DSHLIB=ON -DSTLIB=ON \
                -DSTBIN=OFF -DTIMER=HREAL \
                -DCHECK=on -DVERBS=on \
                -DARITH=x64-asm-254 -DFP_PRIME=254 \
                -DFP_METHD="INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" \
                -DCOMP="-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=x86-64 -mtune=generic -fPIC" \
                -DFP_PMERS=off -DFP_QNRES=on \
                -DFPX_METHD="INTEG;INTEG;LAZYR" -DPP_METHD="LAZYR;OATEP" .. && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r relic

}

install_rocksdb_lib() {
    cd ${HOME}
    wget ${WGET_FLAGS} \
        https://github.com/facebook/rocksdb/archive/v6.8.1.tar.gz && \
        tar -xzf v6.8.1.tar.gz && \
        rm v6.8.1.tar.gz && \
        cd rocksdb-6.8.1 && \
        EXTRA_CXXFLAGS="-fno-omit-frame-pointer -g " \
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
    wget ${WGET_FLAGS} \
        https://dl.min.io/server/minio/release/linux-amd64/minio && \
        chmod 755 ${HOME}/minio

    pip3 install minio

}


install_opentracing_lib() {
    cd ${HOME}
    wget ${WGET_FLAGS} \
        https://github.com/opentracing/opentracing-cpp/archive/v1.6.0.tar.gz && \
        tar -xzf v1.6.0.tar.gz && \
        rm v1.6.0.tar.gz && \
        cd opentracing-cpp-1.6.0 && \
        mkdir build && \
        cd build && \
        cmake .. && \
        make -j$(nproc) && \
        make install && \
        cd ../.. && \
        rm -r opentracing-cpp-1.6.0

}

# ASIO only (without boost). This allows us to upgrade ASIO independently.
install_asio() {
    cd ${HOME}
    wget ${WGET_FLAGS} \
    https://sourceforge.net/projects/asio/files/asio/1.18.1%20%28Stable%29/asio-1.18.1.tar.bz2 && \
        tar -xf asio-1.18.1.tar.bz2 && \
        cd asio-1.18.1 && \
        ./configure && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -rf asio*

}

# Get the newest openSSL installation (as of 9/2020)
install_openssl() {
    OPENSSL_VER='1.1.1g'
    cd /usr/local/src/
    wget ${WGET_FLAGS} https://www.openssl.org/source/openssl-${OPENSSL_VER}.tar.gz && \
        tar -xf openssl-${OPENSSL_VER}.tar.gz && \
        rm openssl-${OPENSSL_VER}.tar.gz && \
        cd openssl-${OPENSSL_VER} && \
        ./config --prefix=/usr/local/ssl --openssldir=/usr/local/ssl shared zlib && \
        make -j$(nproc) && \
        make install && \
        echo "/usr/local/ssl/lib" > /etc/ld.so.conf.d/openssl-${OPENSSL_VER}.conf && \
        rm -rf /usr/local/src/openssl-${OPENSSL_VER}

}

# gRPC
# https://github.com/grpc/grpc/blob/master/test/distrib/cpp/run_distrib_test_cmake.sh
install_grpc() {
    cd ${HOME}
    git clone -b v1.37.1 --depth 1 --recurse-submodules https://github.com/grpc/grpc && \
        cd grpc && \
        mkdir -p ${HOME}/grpc/third_party/abseil-cpp/cmake/build && \
        cd ${HOME}/grpc/third_party/abseil-cpp/cmake/build && \
        cmake -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            ../.. && \
        make -j$(nproc) install && \
        mkdir -p ${HOME}/grpc/third_party/protobuf/cmake/build && \
        cd ${HOME}/grpc/third_party/protobuf/cmake/build && \
        cmake -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            .. && \
        make -j$(nproc) install && \
        mkdir -p ${HOME}/grpc/cmake/build && \
        cd ${HOME}/grpc/cmake/build && \
        cmake -DgRPC_INSTALL=ON \
            -DgRPC_ABSL_PROVIDER=package \
            -DgRPC_PROTOBUF_PROVIDER=package \
            -DgRPC_SSL_PROVIDER=package \
            -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            ../.. && \
        make -j$(nproc) install &&
        cd ${HOME} && \
        rm -r ${HOME}/grpc

}

install_prometheus() {
    # libcurl needed for Prometheus-cpp
    cd ${HOME}
    curl --ssl-reqd --output curl.tar.xz https://curl.se/download/curl-7.66.0.tar.xz && \
        tar xf curl.tar.xz && \
        cd curl-7.66.0 && \
        ./configure --with-ssl --prefix=/usr/local && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r curl.tar.xz curl-7.66.0

    # Prometheus-cpp
    cd ${HOME}
    git clone -b v0.13.0 --depth 1 --recurse-submodules https://github.com/jupp0r/prometheus-cpp.git && \
        cd prometheus-cpp && \
        mkdir _build && \
        cd _build && \
        cmake -DBUILD_SHARED_LIBS=OFF \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            .. && \
        cmake --build . --parallel $(nproc) && \
        cmake --install . && \
        cd ${HOME} && \
        rm -r prometheus-cpp

}

install_json_lib(){
    cd ${HOME}
    git clone -b v3.9.1 --depth 1 https://github.com/nlohmann/json && \
        cd json && \
        mkdir build && \
        cd build && \
        cmake .. && \
        make -j$(nproc) && \
        make install && \
        cd ${HOME} && \
        rm -r json

}

install_httplib() {
    cd ${HOME}
    git clone -b v0.11.2 --depth 1 https://github.com/yhirose/cpp-httplib && \
        cd cpp-httplib && \
        mkdir build && \
        cd build && \
        cmake -DCMAKE_BUILD_TYPE=Release .. && \
        cmake --build . --target install && \
        cd ${HOME} && \
        rm -r cpp-httplib

}

# Thrift is the protocol used by Jaeger to export metrics
#depends: boost
install_thrift_lib(){
cd $HOME
  git clone -b v0.13.0 https://github.com/apache/thrift.git
  cd thrift
  ./bootstrap.sh
  ./configure CXXFLAGS='-g -O2' \
      --without-python --enable-static --disable-shared \
      --disable-tests --disable-tutorial --disable-coverage
  make -j$(nproc) install
  cd ${HOME}
  rm -r thrift
}

install_jaegertracing_cpp_lib(){
  # TODO: Upgrade to opentelemetry-cpp
  # Tracing via Jaeger and Thrift protocol
  # Copy FindThrift.cmake because installing Thrift does not include a CMake definition
  
  #depends on opentracing, thrift, nlohmann, httplib
  cd $HOME
  git clone -b v0.9.0 --depth 1 https://github.com/jaegertracing/jaeger-client-cpp
  cd jaeger-client-cpp && mkdir build && cd build
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

install_cppcheck(){
  cd ${HOME}
  CPPCHECK_VER="2.8"
  wget ${WGET_FLAGS} https://sourceforge.net/projects/cppcheck/files/cppcheck/${CPPCHECK_VER}/cppcheck-${CPPCHECK_VER}.tar.gz/download -O ./cppcheck.tar.gz && \
  tar -xvzf cppcheck.tar.gz && rm ./cppcheck.tar.gz  && \
  cd cppcheck-${CPPCHECK_VER} && \
  mkdir build && cd build && \
  cmake .. && \
  cmake --build . && \
  make install && \
  cd ${HOME} && \
  rm -rf cppcheck-${CPPCHECK_VER}
}


install_ccache(){
  # ccache is used to accelerate C/C++ recompilation
  CCACHE_VER=4.6.1
  cd "$HOME"
  wget https://github.com/ccache/ccache/releases/download/v${CCACHE_VER}/ccache-${CCACHE_VER}.tar.xz && \
    tar xvf ccache-${CCACHE_VER}.tar.xz && \
    cd ccache-${CCACHE_VER} && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DREDIS_STORAGE_BACKEND=OFF .. && \
    make && \
    make install && \
    cd "${HOME}" && \
    rm -rf ccache-${CCACHE_VER} ccache-${CCACHE_VER}.tar.xz
  mkdir -p /mnt/ccache/
}

install_build_tools
install_third_party_libraries
install_cmake
install_boost
install_yaml
install_HdrHistogram_lib
install_log4cpp_lib
install_googletest
install_gmp_lib
install_relic_toolkit
install_rocksdb_lib
install_rapidcheck
install_minio
install_opentracing_lib
install_asio
install_openssl
install_grpc
install_prometheus
install_json_lib
install_httplib
install_thrift_lib
install_jaegertracing_cpp_lib
install_cppcheck
install_ccache

# After installing all libraries, let's make sure that they will be found at compile time
ldconfig -v
