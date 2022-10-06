#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$(cd $scriptdir/../..; pwd -P)

. $scriptdir/shlibs/os.sh

if [ "$OS" == "OSX" ]; then
    brew install cmake openssl pkg-config ntl
elif [ "$OS" == "Linux" ]; then 
    sudo apt-get install cmake clang
else
    echo "Unsupported OS!"
fi
