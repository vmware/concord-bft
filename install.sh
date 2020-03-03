#!/usr/bin/env sh

sudo apt-get update
sudo apt-get install -y ccache cmake clang-format python3-pip python3-setuptools libgmp3-dev parallel

python3 -m pip install --upgrade wheel
python3 -m pip install --upgrade conan
python3 -m pip install --upgrade trio

conan profile new default --detect
conan profile update settings.compiler.libcxx=libstdc++11 default

source ~/.profile

sh .conan/install_conan_pkgs.sh
