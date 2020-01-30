#!/usr/bin/env sh

if [[ "$OSTYPE" == "darwin"* ]]; then
# Nothing needed for macos
echo "macos homebrew packages handled in .travis.yml"
else
# If on Linux, install necessary packages using apt
sudo apt-get update
sudo apt-get install -y ccache cmake clang-format \
python3-pip python3-setuptools
fi

sudo apt-get update
sudo apt-get install python3 python3-pip g++ cmake clang clang-format

python3 -m pip install --upgrade wheel
python3 -m pip install --upgrade conan
python3 -m pip install --upgrade trio

conan profile new default --detect
conan profile update settings.compiler.libcxx=libstdc++11 default

source ~/.profile

sh .conan/install_conan_pkgs.sh
