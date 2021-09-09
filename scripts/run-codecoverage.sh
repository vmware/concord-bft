#!/usr/bin/env sh
################################################################################
#                              run-codecoverage                                #
#                                                                              #
# This script is used to run prepare-code-coverage-artifact.py python file to  #
# to generate code coverage report for apollo tests.                           #
#                                                                              #
# Help:                                                                        #
#  If do not want to preserve raw profile data with the coverage report.       #
#  > python3 $dir_path$py_file $raw_file_path_arg                              #
#                                                                              #
#  If want to preserve raw profile data with the coverage report.              #
#  > python3 $dir_path$py_file $raw_file_path_arg $optional_arg                #
#                                                                              #
################################################################################

echo "Usage: run-codecoverage.sh"

dir_path="scripts"
py_file="/prepare-code-coverage-artifact.py"
apollo_build_path_arg="build/tests/apollo/"
optional_arg="--preserve-profiles"

python3 $dir_path$py_file $apollo_build_path_arg
