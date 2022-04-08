#!/bin/sh

set -e

# To build, create a `build/` directory (will be .gitignore'd by default) as follows:
mkdir -p build/
cd build/
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .

# You can install using:
sudo cmake --build . --target install
