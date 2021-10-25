#!/usr/bin/env bash
# This script is used to check if the coveragereport directory exists after a successful run.

set -e

if [ -n "$(ls -A ${1})" ] 
then
  echo -e "\033[0;32mcoveragereport exists at below location. Please check artifacts\033[m"
  echo -e "$(find . -type f -name index.html)"
else
  echo -e echo -e "\033[0;31coveragereport does not exist\033[m"
fi
