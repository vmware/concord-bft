#!/bin/bash

# Install needed packages
if [[ "$OSTYPE" == "darwin"* ]]; then
# Nothing needed for macos
echo "macos homebrew packages handled in .travis.yml"
else
# If on Linux, install necessary packages using apt
sudo apt-get update
sudo apt-get install -y ccache cmake clang-format libgmp3-dev \
python3-pip python3-setuptools
fi

# Install Conan package manager
python3 -m pip install --upgrade wheel
python3 -m pip install --upgrade conan
source ~/.profile
conan profile new default --detect
conan profile update settings.compiler.libcxx=libstdc++11 default

# trio is need for tests
python3 -m pip install --upgrade trio
