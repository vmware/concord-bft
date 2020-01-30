#!/usr/bin/env sh

cd "$(dirname "$0")"

# The order is important because relic uses gmp
conan create "gmp_pkg/conanfile.py"
conan create "relic_pkg/conanfile.py"
conan create "hdrhistogram_pkg/conanfile.py"

for d in ./packages/* ; do
    rm -r -f "${d}/test_package/build"
done