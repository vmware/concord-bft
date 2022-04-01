#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$(cd $scriptdir/../..; pwd -P)

. $scriptdir/shlibs/os.sh

CMAKE_CXX_FLAGS=
if [ "$OS" = "OSX" -a "$OS_FLAVOR" = "Catalina" ]; then
    echo "Setting proper env. vars for compiling against OpenSSL in OS X Catalina"
    export PKG_CONFIG_PATH="/usr/local/opt/openssl@1.1/lib/pkgconfig"

    # NOTE: None of these seem to help cmake find OpenSSL
    #export LDFLAGS="-L/usr/local/opt/openssl@1.1/lib"
    #export CXXFLAGS="-I/usr/local/opt/openssl@1.1/include"
    #CMAKE_CXX_FLAGS="$CMAKE_CXX_FLAGS -I/usr/local/opt/openssl@1.1/include"
fi

CLEAN_BUILD=0
if [ "$1" == "debug" ]; then
    echo "Debug build..."
    echo
    ATE_PAIRING_FLAGS="DBG=on"
elif [ "$1" == "clean" ]; then
    CLEAN_BUILD=1
fi

# NOTE: Seemed like setting this to OFF caused a simple cout << G1::zero(); to segfault but it was just that I forgot to call init_public_params()
BINARY_OUTPUT=OFF
NO_PT_COMPRESSION=ON
MULTICORE=OFF
if [ "$OS" == "OSX" ]; then
    # libff's multicore implementation uses OpenMP which clang on OS X apparently doesn't support
    MULTICORE=OFF
fi

(
    cd $sourcedir/
    git submodule init
    git submodule update
)

# install libxassert and libxutils

read -p "Install libxassert and libxutils? [y/N]: " ANS

if [ "$ANS" == "y" ]; then
(
    cd /tmp
    mkdir -p libutt-deps/
    cd libutt-deps/

    for lib in `echo libxassert; echo libxutils`; do
        if [ ! -d $lib ]; then
            git clone https://github.com/alinush/$lib.git
        fi

        echo
        echo "Installing $lib..."
        echo

        (
            cd $lib/
            mkdir -p build/
            cd build/
            cmake -DCMAKE_BUILD_TYPE=Release ..
            cmake --build .
            sudo cmake --build . --target install
        )
    done
    
)
fi

cd $sourcedir/depends

# NOTE TO SELF: After one day of trying to get CMake to add these using ExternalProject_Add (or add_subdirectory), I give up.

read -p "Install ate-pairing? [y/N]: " ANS
if [ "$ANS" == "y" ]; then
echo "Installing ate-pairing..."
(
    cd ate-pairing/
    if [ $CLEAN_BUILD -eq 1 ]; then
        echo "Cleaning previous build..."
        echo
        make clean
    fi
    make -j $NUM_CPUS -C src \
        SUPPORT_SNARK=1 \
        $ATE_PAIRING_FLAGS

    INCL_DIR=/usr/local/include/ate-pairing/include
    sudo mkdir -p "$INCL_DIR"
    sudo cp include/bn.h  "$INCL_DIR"
    sudo cp include/zm.h  "$INCL_DIR"
    sudo cp include/zm2.h "$INCL_DIR"

    # WARNING: Need this because (the newer 'develop' branch of) libff uses #include "ate-pairing/include/bn.h"
    # Actually, no longer the case, so removing
    #sudo ln -sf "$INCL_DIR" "$INCL_DIR/include"

    # WARNING: Need this due to a silly #include "depends/[...]" directive from libff
    # (/usr/local/include/libff/algebra/curves/bn128/bn128_g1.hpp:12:44: fatal error: depends/ate-pairing/include/bn.h: No such file or directory)
    # Actually, no longer the case, so removing
    #sudo mkdir -p "$INCL_DIR/../depends/ate-pairing/"
    #sudo ln -sf "$INCL_DIR" "$INCL_DIR/../depends/ate-pairing/include"

    LIB_DIR=/usr/local/lib
    sudo cp lib/libzm.a "$LIB_DIR"
    # NOTE: Not sure why, but getting error at runtime that this cannot be loaded. Maybe it should be zm.so?
    #sudo cp lib/zm.so "$LIB_DIR/libzm.so"
)
fi

read -p "Install libff? [y/N]: " ANS
if [ "$ANS" == "y" ]; then
echo "Installing libff..."
(
    cd libff/
    if [ $CLEAN_BUILD -eq 1 ]; then
        echo "Cleaning previous build..."
        echo
        rm -rf build/
    fi

    #git submodule init && git submodule update

    mkdir -p build/
    cd build/
    # WARNING: Does not link correctly with -DPERFORMANCE=ON
    cmake \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DIS_LIBFF_PARENT=OFF \
        -DBINARY_OUTPUT=$BINARY_OUTPUT \
        -DNO_PT_COMPRESSION=$NO_PT_COMPRESSION \
        -DCMAKE_CXX_FLAGS="-Wno-unused-parameter -Wno-unused-value -Wno-unused-variable -I$sourcedir $CMAKE_CXX_FLAGS" \
        -DUSE_ASM=ON \
        -DPERFORMANCE=OFF \
        -DMULTICORE=$MULTICORE \
        -DCURVE="BN128" \
        -DWITH_PROCPS=OFF ..

    make -j $NUM_CPUS
    sudo make install
)
fi

echo "Always installing libfqfft (because it's fast)..."
(
    # NOTE: Headers-only library
    cd libfqfft/
    
    INCL_DIR=/usr/local/include/libfqfft
    sudo mkdir -p "$INCL_DIR"
    sudo cp -r libfqfft/* "$INCL_DIR/"
    sudo rm "$INCL_DIR/CMakeLists.txt"

    # This is required by libfqfft because it depends on an older version of libff
    # but when built as part of libutt a newer version is used. Between the two versions
    # the location of a particular header has changed.
    # (Not needed when the submodule commits match the original, however leaving this fix
    # as a documentation in case we decide to upgrade some dependency)
    # USR_INCL_DIR=/usr/local/include
    # sudo ln -sf "$USR_INCL_DIR/libff/algebra/field_utils/field_utils.hpp" "$USR_INCL_DIR/libff/algebra/fields/field_utils.hpp"
    # sudo ln -sf "$USR_INCL_DIR/libff/algebra/field_utils/field_utils.tcc" "$USR_INCL_DIR/libff/algebra/fields/field_utils.tcc"
)
