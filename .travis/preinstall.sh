#!/bin/bash

# Install needed packages
if [[ "$OSTYPE" == "darwin"* ]]; then
# Nothing needed for macos
echo "macos homebrew packages handled in .travis.yml"
else
# If on Linux, install necessary packages using apt
sudo apt-get update
sudo apt-get install -y ccache cmake clang-format libgmp3-dev s3-dev \
python3-pip python3-setuptools
fi

# Install Conan package manager
python3 -m pip install --upgrade wheel
python3 -m pip install --upgrade conan
source ~/.profile
conan profile new default --detect
conan profile update settings.compiler.libcxx=libstdc++11 default


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

if [ -n "$USE_LOG4CPP" ]; then
    cd
    cd log4cplus/
    ./configure CXXFLAGS="--std=c++11"
    make
    sudo make install
fi

# trio is need for tests
python3 -m pip install --upgrade trio
