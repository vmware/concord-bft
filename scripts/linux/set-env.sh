#!/bin/bash

if [ $# -lt 1 ]; then
    name=${BASH_SOURCE[@]}
    echo "Usage: source $name <buildtype>"
    echo
    echo "Examples:"
    echo
    echo "source $name debug"
    echo "source $name release"
    echo
    if [ -n "$C_BFT_HAS_ENV_SET" ]; then
        echo "Currently-set environment"
        echo "========================="
        echo "Build directory: $C_BFT_BUILD_DIR"
        echo "CMake args: $C_BFT_CMAKE_ARGS"
    fi
else
    invalid_buildtype=0
    buildtype=`echo "$1" | tr '[:upper:]' '[:lower:]'`

    scriptdir=$(cd $(dirname ${BASH_SOURCE[@]}); pwd -P)
    sourcedir=$(cd $scriptdir/../..; pwd -P)
    #echo "Source dir: $sourcedir"

    builddir_base=~/builds/concord-bft
    case "$buildtype" in
    debug)
        builddir=$builddir_base/debug
        cmake_args="-DCMAKE_BUILD_TYPE=Debug"
        ;;
    release)
        builddir=$builddir_base/release
        cmake_args="-DCMAKE_BUILD_TYPE=Release"
        ;;
    *)
        invalid_buildtype=1
        ;;
    esac

    #
    # grep-code alias
    #
    alias grep-code='grep --exclude-dir=.git'

    if [ $invalid_buildtype -eq 0 ]; then
        echo "Configuring for build type '$1' ..."
        echo

        threshsign=$builddir/lib/threshsign
        bftlib=$builddir/bftengine

        export PATH="$builddir/bin:$scriptdir:$sourcedir/lib/threshold-sign/scripts:$threshsign/bin/app:$threshsign/bin/test:$threshsign/bin/bench:$bftlib/bench/scripts:$PATH"


        echo "Build directory: $builddir"
        echo "CMake args: $cmake_args"
        echo "PATH envvar: $PATH" 

        export C_BFT_BUILD_DIR=$builddir
        export C_BFT_CMAKE_ARGS=$cmake_args
        export C_BFT_HAS_ENV_SET=1
    else
        echo "ERROR: Invalid build type '$buildtype'"
    fi
fi
