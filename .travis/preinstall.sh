#!/bin/bash

# Install needed packages
if [[ "$OSTYPE" == "darwin"* ]]; then
# Nothing needed for macos
echo "macos homebrew packages handled in .travis.yml"
else
# If on Linux, install necessary packages using apt
sudo apt-get update
sudo apt-get install -y ccache cmake clang-format libgmp3-dev parallel \
python3-pip python3-setuptools openssl libssl-dev
fi

# build and install relic
cd /tmp
git clone https://github.com/relic-toolkit/relic
cd relic
git checkout b984e901ba78c83ea4093ea96addd13628c8c2d0
mkdir -p build
cd build
CC=/usr/bin/gcc CXX=/usr/bin/g++ cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DALLOC=AUTO -DWSIZE=64 -DRAND=UDEV -DSHLIB=ON -DSTLIB=ON -DSTBIN=OFF -DTIMER=HREAL -DCHECK=on -DVERBS=on -DARITH=x64-asm-254 -DFP_PRIME=254 -DFP_METHD="INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" -DCOMP="-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=native -mtune=native" -DFP_PMERS=off -DFP_QNRES=on -DFPX_METHD="INTEG;INTEG;LAZYR" -DPP_METHD="LAZYR;OATEP" ..
CC=/usr/bin/gcc CXX=/usr/bin/g++ make
sudo make install

# build and install cryptopp
cd /tmp
git clone https://github.com/weidai11/cryptopp.git
cd cryptopp
git checkout CRYPTOPP_8_2_0
make
sudo make install

# build and install boost
cd /tmp
wget --https-only https://dl.bintray.com/boostorg/release/1.64.0/source/boost_1_64_0.tar.gz
tar -xf boost_1_64_0.tar.gz
cd boost_1_64_0
./bootstrap.sh --with-libraries=system,filesystem
./b2
sudo ./b2 install

# build and install RocksDB and its dependencies
if [ -n "$USE_ROCKSDB" ]; then
    # Install RocksDB dependencies
    sudo apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev

    # Install RocksDB
    cd /tmp
    wget --https-only https://github.com/facebook/rocksdb/archive/v5.7.3.tar.gz
    tar -xzf v5.7.3.tar.gz
    cd rocksdb-5.7.3
    make shared_lib
    sudo make install-shared
fi

# trio is need for tests
python3 -m pip install --upgrade trio
