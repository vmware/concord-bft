#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$(cd $scriptdir/../..; pwd -P)
. $scriptdir/shlibs/check-env.sh
. $scriptdir/shlibs/os.sh

$scriptdir/submodule-update.sh

builddir=$UTT_BUILD_DIR
mkdir -p "$builddir"
cd "$builddir"

if [ "$OS" = "Linux" ]; then
   # Note(Adithya): Better be specific whenever possible
   # In containers like docker, there can exist multiple versions of clang.
   # Use clang-12 if available.
   if [ which clang++-12 2>/dev/null || true ]; then 
      export CXX=clang++-12
   else
      export CXX=clang++
   fi
fi
cmake $UTT_CMAKE_ARGS $@ "$sourcedir"
