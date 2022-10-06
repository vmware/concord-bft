#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$(cd $scriptdir/../..; pwd -P)
linux_script_dir=$(cd $sourcedir/scripts/linux/; pwd -P)
. $linux_script_dir/shlibs/os.sh
. $linux_script_dir/shlibs/check-env.sh

export UTT_BUILD_DIR=$(mkdir -p $sourcedir/build; cd $sourcedir/build; pwd -P)
builddir="${UTT_BUILD_DIR}"

echo "Script dir: ${scriptdir}"
echo "Source dir: ${sourcedir}"
echo "Linux scripts dir: ${linux_script_dir}"
echo "Making in '$builddir' ..."
$linux_script_dir/cmake.sh
cd "$builddir"
make -j $NUM_CPUS $@
