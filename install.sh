#!/usr/bin/env sh
set -e
set -u

install_basic_deps() {
  sudo apt-get update
  sudo apt-get install -y ccache cmake clang clang-format-7 python3-pip python3-setuptools libgmp3-dev g++ parallel
  python3 -m pip install --upgrade wheel
  python3 -m pip install --upgrade trio
}

install_conan() {
  echo "Using Conan build..."
  echo "Installing Conan..."
  python3 -m pip install --upgrade conan
  conan profile new default --detect
  conan profile update settings.compiler.libcxx=libstdc++11 default
}

install_basic_deps
install_conan