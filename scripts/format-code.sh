#!/usr/bin/env sh

BFT="concord-bft"

if [ -z "$1" ]; then
  >&2 echo "Usage: format-code.sh <path>"
  return 1
fi

ABS_CONCORD_PATH=`realpath $1`

# Overly cautious saftey check
IS_EXPECTED_NAME=$(echo ${ABS_CONCORD_PATH} | grep "${BFT}")

if [ ! -e ${ABS_CONCORD_PATH} ] || [ -z ${IS_EXPECTED_NAME} ]; then
  >&2 echo "ERROR: Couldn't find \"${ABS_CONCORD_PATH}\" in ${BFT} directory"
  return 1;
fi

FILES_TO_FORMAT=$(find ${ABS_CONCORD_PATH} \
  -type f \( \
    -iname "*.c" -o \
    -iname "*.cc" -o \
    -iname "*.cpp" -o \
    -iname "*.h" -o \
    -iname "*.ipp" -o \
    -iname "*.hpp" \) \
  -a -not -path "${ABS_CONCORD_PATH}/deps/*" \
  -a -not -path "${ABS_CONCORD_PATH}/build/*")

if [ -n "$2" ]; then
  if [ "$2" = "--is-required" ]; then
    $(clang-format \
      -style=file \
      -fallback-style=none \
      --dry-run --Werror ${FILES_TO_FORMAT})

    if [ $? -ne 0 ]; then
      # Note: exit_code = return_value % 255
      echo "Code format changes needed"
      return 1
    else
      echo "No format changes needed"
      return 0
    fi
  fi
  >&2 echo "ERROR: Unknown parameter \"$2\""
  return 1
else
  clang-format -style=file -fallback-style=none -i ${FILES_TO_FORMAT}
fi
