#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$(cd $scriptdir/../..; pwd -P)
. $scriptdir/shlibs/check-env.sh

builddir=$C_BFT_BUILD_DIR

echo "Making in '$builddir' ..."
echo
[ ! -d "$builddir" ] && $scriptdir/cmake.sh
cd "$builddir"
make $@
