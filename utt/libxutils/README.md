libxutils
=========

A utility library with various useful functions.

`libxutils` is easily importable into CMake projects using `find_package(xutils)`, when installed on the system.

## Dependencies

You need to install `libxassert` from [here](https://github.com/alinush/libxassert):

    git clone https://github.com/alinush/libxassert
    cd libxassert/
    mkdir -p build/
    cd build/
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make
    sudo make install

## Build

To build, create a `build/` directory (will be .gitignore'd by default) as follows:

    mkdir build/
    cd build/
    cmake -DCMAKE_BUILD_TYPE=Release ..
    cmake --build .

You can install using:

    sudo cmake --build . --target install

## Test

To run some tests:

    cd build/
    ctest

## TODO

 - [DONE] Cannot install libxutils due to misconfigured libxassert dependency
   + From now on libxutils expects libxassert to be installed
