#!/usr/bin/env bash

set -e

! grep -rn \
    --color=auto \
    --include=\*.{h,hpp,cpp} \
    --exclude-dir={build,.git,.github,messages} \
    -E \
    "NOLINTNEXTLINE(\s+|$)|NOLINT(\s+|$)|\sassert\(" $1
