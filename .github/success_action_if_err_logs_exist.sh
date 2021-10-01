#!/usr/bin/env bash
# This script used to check if ERROR/FATAL logs are present in apollo replica logs.

set -e

file_name=ReplicaErrorLogs.txt
file_count=$(find ${1} -name $file_name | wc -l)

if [[ $file_count -gt 0 ]]; then
    echo -e "\033[31mERROR/FATAL logs found in below paths. Please check artifacts\033[m"
    echo -e "$(find . -type f -name ReplicaErrorLogs.txt)"
else 
    echo "File not found"
fi
