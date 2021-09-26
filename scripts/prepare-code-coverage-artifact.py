#!/usr/bin/python
# -*- coding: utf-8 -*-

# Concord
#
# Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

from __future__ import print_function

import argparse
import os
import subprocess
import sys
import shutil


def get_llvm_tools(coveragedir):
    """
    Return the path of llvm-profdata and llvm-cov
    """
    profdata_exe = os.path.join(coveragedir, 'llvm-profdata')
    cov_exe = os.path.join(coveragedir, 'llvm-cov')
    if os.path.exists(profdata_exe):
        return profdata_exe, cov_exe
    if os.path.exists('/usr/lib/llvm-9/bin/llvm-profdata'):
        return '/usr/lib/llvm-9/bin/llvm-profdata', '/usr/lib/llvm-9/bin/llvm-cov'
    if os.path.exists('/usr/bin/llvm-profdata-6.0'):
        return '/usr/bin/llvm-profdata-6.0', '/usr/bin/llvm-cov-6.0'
    print('Could not find LLVM tools for code coverage')
    sys.exit(1)


def change_permissions_recursive(path, mode):
    """
    Set permissions for all the files/folders under path
    :param path: Path to the folder
    :param mode: Permission
    """
    for root, dirs, files in os.walk(path, topdown=False):
        for directory in [os.path.join(root, d) for d in dirs]:
            os.chmod(directory, mode)
        for file in [os.path.join(root, f) for f in files]:
            os.chmod(file, mode)


def check_for_raw_profile_files(manifest_path):
    """
    Function is used to check for any .profraw files present.
    """
    manifest_file = open(manifest_path)
    profraw_ext = '.profraw'
    if(profraw_ext in manifest_file.read()):
        return True
    else:
        return False


def merge_raw_profiles(host_llvm_profdata, profile_data_dir):
    """
    Function is used to generate the index profdata file after merging all profraw files.
    profdata file is used in the generation of code coverage report.
    """
    print('\n:: Merging raw profiles...', end='')
    sys.stdout.flush()
    manifest_path = os.path.join(profile_data_dir, 'profiles.manifest')
    profdata_path = os.path.join(profile_data_dir, 'Coverage.profdata')
    with open(manifest_path, 'w') as manifest:
        for (root, dirs, files) in os.walk(profile_data_dir):
            for file in files:
                if file.endswith('.profraw'):
                    manifest.write(os.path.join(root, file))
                    manifest.write('\n')
        manifest.close()

    if not check_for_raw_profile_files(manifest_path):
        print('\n:: No raw profiles present')
        sys.exit(1)

    merge_command = [
        host_llvm_profdata,
        'merge',
        '-sparse',
        '-f',
        manifest_path,
        '-o',
        profdata_path,
    ]
    merge_process = subprocess.run(merge_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   universal_newlines=True)

    if merge_process.returncode != 0:
        print('Code coverage merge failed')
        print('  Stdout - {}'.format(merge_process.stdout.decode('utf-8')))
        print('  Stderr - {}'.format(merge_process.stderr.decode('utf-8')))
        sys.exit(1)

    os.remove(manifest_path)
    return profdata_path


def prepare_html_report(host_llvm_cov, profile, coverage_report_dir, binary):
    """
    Function is used to generation of code coverage report.
    """
    print('\n:: Preparing html report for {0}...'.format(binary), end='')
    sys.stdout.flush()
    objects = []
    objects.append(binary)
    index_page = os.path.join(coverage_report_dir, 'index.html')
    if not os.path.isdir(coverage_report_dir):
        os.makedirs(coverage_report_dir)

    cov_command = [host_llvm_cov, 'show'] + objects + [
        '-format',
        'html',
        '-instr-profile',
        profile,
        '-o',
        coverage_report_dir,
        '-show-line-counts-or-regions',
        '-Xdemangler',
        'c++filt',
        '-Xdemangler',
        '-n',
        '-project-title',
        'Concord-bft Apollo Code Coverage'
    ]

    cov_process = subprocess.run(cov_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 universal_newlines=True)
    if cov_process.returncode != 0:
        print('::Code coverage report generation failed')
        print('Stdout - {}'.format(cov_process.stdout.decode('utf-8')))
        print('Stderr - {}'.format(cov_process.stderr.decode('utf-8')))
        sys.exit(1)

    with open(os.path.join(coverage_report_dir, 'summary.txt'), 'wb') as Summary:
        subprocess.check_call([host_llvm_cov, 'report'] + objects
                              + ['-instr-profile', profile], stdout=Summary)

    print('\n:: Merged Code Coverage Reports are in {}'.format(coverage_report_dir))
    print('\n:: Open browser on {}'.format(index_page))
    change_permissions_recursive(coverage_report_dir, 0o777)


def get_commandline_args():
    """
    parse command line
    :return: args structure
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('apollo_build_path',
                        help='Path to the apollo build directory')
    parser.add_argument('--preserve-profiles',
                        help='Do not delete raw profiles',
                        action='store_true')
    return parser.parse_args()


if __name__ == '__main__':
    args = get_commandline_args()

    # Path to an instrumented binary
    apollo_binary_path = os.path.join(
        'build', 'tests', 'simpleKVBC', 'TesterReplica', 'skvbc_replica')

    # Path to the directory containing the raw profiles
    profile_data_dir = os.path.join(args.apollo_build_path, 'codecoverage')

    # path to llvm-profdata and llvm-cov binary
    profdata_exe, cov_exe = get_llvm_tools(profile_data_dir)

    profdata_path = merge_raw_profiles(profdata_exe, profile_data_dir)
    if not os.path.isfile(profdata_path):
        print('\n:: No profdata file is present')
        sys.exit(1)

    # Path to the output directory for html reports
    coverage_report_dir = os.path.join(
        args.apollo_build_path, 'coveragereport')

    prepare_html_report(cov_exe, profdata_path,
                        coverage_report_dir, apollo_binary_path)

    if not args.preserve_profiles:
        # Delete all contents of a directory and ignore errors
        shutil.rmtree(profile_data_dir, ignore_errors=True)
