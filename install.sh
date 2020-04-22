#!/bin/bash
set -e
set -u

install_basic_deps() {
  sudo apt-get update
  sudo apt-get install -y ccache cmake clang clang-format-9 python3-pip python3-setuptools libgmp3-dev g++ parallel
  python3 -m pip install --upgrade wheel
  python3 -m pip install --upgrade trio
}

default_profile_exists() {
  local exists="false"
  for profile in `conan profile list`; do
    if [[ "$profile" == "default" ]]; then
      exists="true"
      break
    fi
  done
  echo $exists
}

install_conan() {
  echo "Using Conan build..."
  echo "Installing Conan..."
  python3 -m pip install --upgrade conan
  result=$(default_profile_exists)
  if [[ "$result" == "true" ]]; then
     echo "Default profile exists. Skipping."
  else
    echo "Creating default profile..."
    conan profile new default --detect
    conan profile update settings.compiler.libcxx=libstdc++11 default
  fi
}

install_basic_deps
install_conan
