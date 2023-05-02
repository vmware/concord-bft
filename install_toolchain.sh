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

# Install building tools
install_toolchain() {
    apt-get update && apt-get ${APT_GET_FLAGS} install \
        autoconf \
        automake \
        bison \
        build-essential \
        clang-format \
        clang-tidy \
        curl \
        flex \
        gdb \
        gdbserver \
        git \
        iproute2 \
        iptables \
        less \
        libtool \
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

}

install_cmake() {
    wget ${WGET_FLAGS} -O cmake-linux.sh \
    https://github.com/Kitware/CMake/releases/download/v3.24.2/cmake-3.24.2-linux-x86_64.sh &&\
    sh cmake-linux.sh -- --skip-license --prefix=/usr
}

install_cppcheck(){
  cd ${HOME}
  CPPCHECK_VER="2.8"
  wget ${WGET_FLAGS} https://sourceforge.net/projects/cppcheck/files/cppcheck/${CPPCHECK_VER}/cppcheck-${CPPCHECK_VER}.tar.gz/download -O ./cppcheck.tar.gz
  tar -xvzf cppcheck.tar.gz && rm ./cppcheck.tar.gz
  cd cppcheck-${CPPCHECK_VER}
  mkdir build && cd build
  cmake ..
  cmake --build .
  make -j$(nproc) install
  cd ${HOME} && rm -rf cppcheck-${CPPCHECK_VER}
}

install_ccache(){
  # ccache is used to accelerate C/C++ recompilation
  CCACHE_VER="4.6.1"
  cd "$HOME"
  wget https://github.com/ccache/ccache/releases/download/v${CCACHE_VER}/ccache-${CCACHE_VER}.tar.xz
  tar xvf ccache-${CCACHE_VER}.tar.xz
  cd ccache-${CCACHE_VER}
  mkdir build && cd build
  cmake -DCMAKE_BUILD_TYPE=Release -DREDIS_STORAGE_BACKEND=OFF -DZSTD_FROM_INTERNET=ON ..
  make -j$(nproc)
  make install
  cd "${HOME}" 
  rm -rf ccache-${CCACHE_VER} ccache-${CCACHE_VER}.tar.xz
  mkdir -p /mnt/ccache/
}

install_toolchain
install_cmake
install_cppcheck
install_ccache

# After installing all libraries, let's make sure that they will be found at compile time
ldconfig -v
