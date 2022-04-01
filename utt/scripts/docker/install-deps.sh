#!/bin/bash
set -e

# A script to run before running ./scripts/linux/install-libs.sh

NUM_CPUS=$(cat /proc/cpuinfo | grep processor | wc -l)

# Install clang-12
# See https://apt.llvm.org/ (Automatic installation script)
sudo apt install software-properties-common wget ca-certificates -y
alias wget='wget --no-check-certificate'
wget --no-check-certificate -O llvm.sh https://apt.llvm.org/llvm.sh
bash llvm.sh

# Install dependencies
sudo apt install libgmp-dev cmake libboost-dev libc++-12-dev libc++-12-dev -y

# Install libsodium
wget https://download.libsodium.org/libsodium/releases/libsodium-1.0.18.tar.gz -O libsodium.tar.gz
tar -xvzf libsodium.tar.gz
cd libsodium-1.0.18
./configure
make -j${NUM_CPUS} && make check
sudo make install
cd ..
rm -rf libsodium.tar.gz libsodium-1.0.18

# Install ntl (Use whatever homebrew is using, since Alin uses Homebrew)
wget https://libntl.org/ntl-11.5.1.tar.gz -O ntl.tar.gz
tar -xvzf ntl.tar.gz
cd ntl-11.5.1/src 
./configure
make -j${NUM_CPUS}
sudo make install
cd ../..
rm -rf ntl.tar.gz ntl-11.5.1