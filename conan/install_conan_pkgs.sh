#!/usr/bin/env sh

cd "$(dirname "$0")"

for d in ./packages/* ; do
    conan create "${d}/conanfile.py"
#    rm -r -f "${d}/test_package/build"
done