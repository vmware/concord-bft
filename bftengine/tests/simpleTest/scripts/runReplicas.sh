#!/bin/sh
set -e

scriptdir=$(cd $(dirname $0); pwd -P)

parallel -j0 ::: \
    "$scriptdir/../server 0" \
    "$scriptdir/../server 1" \
    "$scriptdir/../server 2" \
    "$scriptdir/../server 3"
