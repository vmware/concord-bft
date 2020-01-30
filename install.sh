#!/usr/bin/env sh

sudo apt-get update
sudo apt-get install python3 python3-pip g++ cmake clang clang-format

python3 -m pip install --upgrade wheel
python3 -m pip install --upgrade conan
python3 -m pip install conan

conan profile new default --detect
conan profile update settings.compiler.libcxx=libstdc++11 default

source ~/.profile

sh conan/install_conan_pkgs.sh
