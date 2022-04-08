libxassert
==========

A simple assertion libraries that I use, which prints the values that failed the assertion.

`libxassert` is easily importable into CMake projects using `find_package(xassert)` when installed on the system.

## Build

    mkdir build/
    cd build/
    cmake -DCMAKE_BUILD_TYPE=Release ..
    cmake --build .

# Install

    cd build/
    sudo cmake --build . --target install    
