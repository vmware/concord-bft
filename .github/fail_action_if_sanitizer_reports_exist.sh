#!/usr/bin/env bash

set -e

if [ ! -z "$(ls -A ${1})" ]; then
  exit 1;
fi
