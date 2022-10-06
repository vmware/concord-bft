#!/bin/bash
set -e

scriptdir=$(cd $(dirname $0); pwd -P)
sourcedir=$scriptdir

. $scriptdir/scripts/linux/shlibs/os.sh

if [ $# -lt 1 ]; then
    echo "Usage: $0 <new-name-WITHOUT-lib-PREFIX>"
    echo 
    echo "Example: $0 vectorcomm"
    exit 1
fi

old_name=utt
new_name=$1

sed_cmd=sed
if [ "$OS" = "OSX" ]; then
    sed_cmd=gsed
fi

find $sourcedir \( -type d -name .git -prune \) -o -type f -print0 | xargs -0 $sed_cmd -i "s/lib$old_name/lib$new_name/g"
find $sourcedir \( -type d -name .git -prune \) -o -type f -print0 | xargs -0 $sed_cmd -i "s/$old_name/$new_name/g"

OLD_NAME=`echo $old_name | tr [a-z] [A-Z]`
NEW_NAME=`echo $new_name | tr [a-z] [A-Z]`

find $sourcedir \( -type d -name .git -prune \) -o -type f -print0 | xargs -0 $sed_cmd -i "s/$OLD_NAME/$NEW_NAME/g"

(
    cd "$scriptdir/lib${old_name}/include/";

    mv "${old_name}" "${new_name}";
)
mv "$scriptdir/lib${old_name}" "$scriptdir/lib${new_name}"
