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

echo ca_certificate=/etc/ssl/certs/ca-certificates.crt > ~/.wgetrc

# Install tools
apt-get update && apt-get ${APT_GET_FLAGS} install \
    git \
    wget

# Install 3rd parties

NUM_CPUS=$(cat /proc/cpuinfo | grep processor | wc -l)

# Install libsodium
cd ${HOME}
wget ${WGET_FLAGS} https://download.libsodium.org/libsodium/releases/libsodium-1.0.18.tar.gz -O libsodium.tar.gz
tar -xvzf libsodium.tar.gz
cd libsodium-1.0.18
./configure
make -j${NUM_CPUS} && make check
sudo make install
cd ..
rm -rf libsodium.tar.gz libsodium-1.0.18

# Install ntl (Use whatever homebrew is using, since Alin uses Homebrew)
cd ${HOME}
wget ${WGET_FLAGS} https://libntl.org/ntl-11.5.1.tar.gz -O ntl.tar.gz
tar -xvzf ntl.tar.gz
cd ntl-11.5.1/src 
./configure
make -j${NUM_CPUS}
sudo make install
cd ../..
rm -rf ntl.tar.gz ntl-11.5.1

# install libxassert
echo "Installing libxassert..."
(
    cd ${HOME}
    git clone https://github.com/alinush/libxassert.git libxassert
    cd libxassert/
    git checkout d5f04e0b0df0b84a1256fee7ed0eceba387077dc

    mkdir -p build/
    cd build/
    cmake -DCMAKE_BUILD_TYPE=Release ..
    cmake --build .
    sudo cmake --build . --target install

    cd ${HOME}
    rm -rf libxassert/
)

# install libxutils
echo "Installing libxutils..."
(
    cd ${HOME}
    git clone https://github.com/alinush/libxutils.git libxutils
    cd libxutils/
    git checkout 7b7c985fedf5dc7001c704bf50ffc4975105f093

    mkdir -p build/
    cd build/
    cmake -DCMAKE_BUILD_TYPE=Release ..
    cmake --build .
    sudo cmake --build . --target install

    cd ${HOME}
    rm -rf libxutils/
)

BINARY_OUTPUT=OFF
NO_PT_COMPRESSION=ON
MULTICORE=OFF

# echo "Installing ate-pairing..."
(
    cd ${HOME}
    git clone https://github.com/herumi/ate-pairing.git ate-pairing
    cd ate-pairing/
    git checkout 530223d7502e95f6141be19addf1e24d27a14d50

    ATE_PAIRING_FLAGS="DBG=on"

    make -j $NUM_CPUS -C src \
        SUPPORT_SNARK=1 \
        $ATE_PAIRING_FLAGS

    INCL_DIR=/usr/local/include/ate-pairing/include
    sudo mkdir -p "$INCL_DIR"
    sudo cp include/bn.h  "$INCL_DIR"
    sudo cp include/zm.h  "$INCL_DIR"
    sudo cp include/zm2.h "$INCL_DIR"

    LIB_DIR=/usr/local/lib
    sudo cp lib/libzm.a "$LIB_DIR"

    cd ${HOME}
    rm -rf ate-pairing/
)

# echo "Installing libff..."
(
    cd ${HOME}
    git clone https://github.com/scipr-lab/libff.git libff
    cd libff/
    git checkout a152abfcef21b7778cece96fe77f5e0f819ba79e

    mkdir -p build/
    cd build/
    # WARNING: Does not link correctly with -DPERFORMANCE=ON
    cmake \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DIS_LIBFF_PARENT=OFF \
        -DBINARY_OUTPUT=$BINARY_OUTPUT \
        -DNO_PT_COMPRESSION=$NO_PT_COMPRESSION \
        -DCMAKE_CXX_FLAGS="-Wno-unused-parameter -Wno-unused-value -Wno-unused-variable -I$sourcedir $CMAKE_CXX_FLAGS" \
        -DUSE_ASM=ON \
        -DPERFORMANCE=OFF \
        -DMULTICORE=$MULTICORE \
        -DCURVE="BN128" \
        -DWITH_PROCPS=OFF ..

    make -j $NUM_CPUS
    sudo make install

    cd ${HOME}
    rm -rf libff/
)

# echo "Installing libfqfft (header-only library)..."
(
    cd ${HOME}
    git clone https://github.com/alinush/libfqfft.git libfqfft
    cd libfqfft/
    git checkout 1ebd069d2a00254558998c93767efbbbd51f250a

    INCL_DIR=/usr/local/include/libfqfft
    sudo mkdir -p "$INCL_DIR"
    sudo cp -r libfqfft/* "$INCL_DIR/"
    sudo rm "$INCL_DIR/CMakeLists.txt"

    cd ${HOME}
    rm -rf libfqfft/
)

# After installing all libraries, let's make sure that they will be found at compile time
ldconfig -v
