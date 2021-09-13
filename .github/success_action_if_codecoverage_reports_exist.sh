#!/usr/bin/env bash
# This script used to check if coveragereport directory is present after a successful run.

set -e

if [ -n "$(ls -A ${1})" ] 
then
  echo "Directory is present"
else
  echo "Directory is not present"
  exit 1;
fi
