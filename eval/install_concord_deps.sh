#!/bin/bash
echo "installing ubuntu packages"
sudo apt-get update
sudo apt-get install -y git clang g++ cmake clang-format libgmp3-dev parallel

#boost
wget https://dl.bintray.com/boostorg/release/1.64.0/source/boost_1_64_0.tar.gz
tar -xf boost_1_64_0.tar.gz
cd boost_1_64_0
./bootstrap.sh --with-libraries=system,filesystem
./b2
sudo ./b2 install
cd ..
rm -rf boost_1_64_0.tar.gz

#relic
echo "Installing relic"
rm -rf relic
git clone https://github.com/relic-toolkit/relic
cd relic/
git checkout b984e901ba78c83ea4093ea96addd13628c8c2d0
mkdir -p build/
cd build/
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DALLOC=AUTO -DWSIZE=64 -DRAND=UDEV -DSHLIB=ON -DSTLIB=ON -DSTBIN=OFF -DTIMER=HREAL -DCHECK=on -DVERBS=on -DARITH=x64-asm-254 -DFP_PRIME=254 -DFP_METHD="INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" -DCOMP="-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=native -mtune=native" -DFP_PMERS=off -DFP_QNRES=on -DFPX_METHD="INTEG;INTEG;LAZYR" -DPP_METHD="LAZYR;OATEP" ..
make -j 16
sudo make install
rm -rf relic
cd ..

#cryptopp
echo "installing cryptopp"
git clone https://github.com/weidai11/cryptopp.git
cd cryptopp/
git checkout CRYPTOPP_5_6_5;
mkdir build/
cd build/
cmake ..
make -j 16
sudo make install
cd ..
rm -rf cryptopp
cd ..

sudo ldconfig
