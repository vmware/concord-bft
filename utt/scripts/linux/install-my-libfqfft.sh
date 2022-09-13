#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$(cd $scriptdir/../..; pwd -P)

. $scriptdir/shlibs/os.sh

#(
    #cd $sourcedir/
    #git submodule init
    #git submodule update
#)
#cd $sourcedir/depends

cd /tmp/
git clone https://github.com/alinush/libfqfft

echo "Installing libfqfft..."
(
    # NOTE: Headers-only library
    cd libfqfft/
    git checkout staging
    
    INCL_DIR=/usr/local/include/libfqfft
    sudo mkdir -p "$INCL_DIR"
    sudo cp -r libfqfft/* "$INCL_DIR/"
    sudo rm "$INCL_DIR/CMakeLists.txt"
)
