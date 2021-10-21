#!/bin/bash

# This script installs and builds all the tools needed for building and running Concord-BFT
# It is used for building a docker image that is used by CI/CD and may be used in the
# development environment.
# If you prefer working w/o docker at your dev station just run the script with sudo.
# If you need to add any tool or dependency this is the right place to do it.
# Please bear in mind that we want to keep our list of dependencies as small as possible.
set -e

APT_GET_FLAGS="-y --no-install-recommends"
WGET_FLAGS="--https-only"

# Install tools
apt-get update && apt-get ${APT_GET_FLAGS} install \
    autoconf \
    automake \
    build-essential \
    ccache \
    clang-9 \
    clang-format-9 \
    clang-tidy-10 \
    curl \
    gdb \
    gdbserver \
    git \
    iptables \
    less \
    llvm-9 \
    lzip \
    net-tools \
    parallel \
    pkg-config \
    psmisc \
    python3-pip \
    python3-setuptools \
    sudo \
    vim \
    iproute2 \
    wget

update-alternatives --install /usr/bin/clang clang /usr/lib/llvm-9/bin/clang 100
update-alternatives --install /usr/bin/clang++ clang++ /usr/lib/llvm-9/bin/clang++ 100
update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-9 100
update-alternatives --install /usr/bin/clang-format-diff clang-format-diff /usr/bin/clang-format-diff-9 100
update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-10 100
update-alternatives --install /usr/bin/llvm-symbolizer llvm-symbolizer /usr/lib/llvm-9/bin/llvm-symbolizer 100
update-alternatives --install /usr/bin/llvm-profdata llvm-profdata /usr/lib/llvm-9/bin/llvm-profdata 100
update-alternatives --install /usr/bin/llvm-cov llvm-cov /usr/lib/llvm-9/bin/llvm-cov 100

# Install 3rd parties
apt-get ${APT_GET_FLAGS} install \
    libboost-filesystem1.65-dev \
    libboost-system1.65-dev \
    libboost-program-options1.65-dev \
    libboost1.65-dev \
    libbz2-dev \
    liblz4-dev \
    libs3-dev \
    libsnappy-dev \
    libssl-dev \
    libtool \
    libyaml-cpp-dev \
    libz-dev \
    libzstd-dev

pip3 install --upgrade wheel && pip3 install --upgrade trio
pip3 install \
    eliot eliot-tree \
    tatsu==4.4.0 \
    pytest \
    pycryptodome \
    ecdsa

# Build 3rd parties
wget ${WGET_FLAGS} -O cmake-linux.sh \
    https://github.com/Kitware/CMake/releases/download/v3.20.2/cmake-3.20.2-linux-x86_64.sh && \
    sh cmake-linux.sh -- --skip-license --prefix=/usr

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

# Here we compile cryptopp and cryptoopp-pem in a single library libcryptopp
# cryptopp-pem provides PEM parsing for Wei Dai's Crypto++ (cryptopp).
cd ${HOME}
wget ${WGET_FLAGS} \
    https://github.com/weidai11/cryptopp/archive/CRYPTOPP_8_2_0.tar.gz && \
    tar -xzf CRYPTOPP_8_2_0.tar.gz && \
    rm CRYPTOPP_8_2_0.tar.gz && \
    git clone https://github.com/noloader/cryptopp-pem.git && \
    cd cryptopp-pem && \
    git checkout CRYPTOPP_8_2_0 && \
    cp *.h *.cpp ${HOME}/cryptopp-CRYPTOPP_8_2_0 && \
    cd ${HOME}/cryptopp-CRYPTOPP_8_2_0 && \
    rm -r ${HOME}/cryptopp-pem && \
    CXX_FLAGS="-march=x86-64 -mtune=generic" make -j$(nproc) && \
    make install && \
    rm -r ${HOME}/cryptopp-CRYPTOPP_8_2_0

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

# In order to be compatible with the native build
cd ${HOME}
wget ${WGET_FLAGS} \
    https://dl.min.io/server/minio/release/linux-amd64/minio && \
    chmod 755 ${HOME}/minio

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

# ASIO only (without boost). This allows us to upgrade ASIO independently.
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

# Get the newest openSSL installation (as of 9/2020)
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

# gRPC
# https://github.com/grpc/grpc/blob/master/test/distrib/cpp/run_distrib_test_cmake.sh
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
git clone -b v0.8.0 --depth 1 --recurse-submodules https://github.com/jupp0r/prometheus-cpp.git && \
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

cd ${HOME}
git clone https://github.com/gabime/spdlog.git && \
    cd spdlog && git checkout eb3220622e73a4889eee355ffa37972b3cac3df5 &&  mkdir build && cd build && \
    cmake .. && make -j$(nproc) && \
    make install && \
    cd ${HOME} && \
    rm -r ${HOME}/spdlog

# After installing all libraries, let's make sure that they will be found at compile time
ldconfig -v
