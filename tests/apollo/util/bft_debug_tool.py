# Concord
#
# Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import os
import psutil
import shutil
import subprocess
from pathlib import Path

class BftDebugTool:
    """
    This class handles all operations that should be done to support debug and analysis tools such as Heaptrack/ASAN/TSAN etc.
    """
    sanitizer_names = ["TSAN", "ASAN", "UBSAN"]

    def __init__(self, num_replicas, builddir, current_test):
        """
        Initialize debug tool name and its output folder, path and prefixes for the current test case
        """
        self.external_tool_procs = {}   # process info for external tools (e.g Valgrind and Heaptrack)
        self.name = None                # None if there is no debug tool, else upper case name of current test debug tool
        self.output_log_path = None
        self.output_log_prefix = None
        output_folder_name = None
        name = None
        # used to count output files per specific replica in the same test case
        self.output_counters = {i: 0 for i in range(num_replicas)}
        environ_to_tool_name = {
            'UBSAN_OPTIONS' : 'UBSAN',
            'TSAN_OPTIONS' : 'TSAN',
            'ASAN_OPTIONS' : 'ASAN',
            'ENABLE_HEAPTRACK' : 'HEAPTRACK'
            # Add new tools here
        }
        enabled_options = [ opt for opt in environ_to_tool_name.keys() if opt in os.environ ]
        assert len(enabled_options) <= 1, f"You should run with at most, a single debug tool! (enabled_options={enabled_options}"
        if len(enabled_options) == 0:
            # not running any debug tool, make it explicit
            self.name = None
            return

        # We have a single tool running
        self.name = environ_to_tool_name[enabled_options[0]]
        name = self.name # for convenience

        #
        #  currently, do not run other tools at the same time, and we must run with graceful shutdown (SIGTERM sent to tester replica)
        #
        # Do not allow running debug tool without graceful shutdown
        assert os.environ.get('GRACEFUL_SHUTDOWN', "").lower() in ["true", "on"], \
            f"{name} should be ran with graceful shutdown only!"
        # Do not allow running debug tool with PERF_REPLICA enabled
        assert not os.environ.get('PERF_REPLICA', ""), \
            f"{name} should be ran without PERF_REPLICA defined!"
        # Do not allow running debug tool with CODECOVERAGE enabled
        assert not os.environ.get('CODECOVERAGE', "").lower() in ["true", "on"], \
            f"{name} should be ran without CODECOVERAGE defined!"

        if name == "HEAPTRACK":
            # make sure we are running with debug image
            if not shutil.which("heaptrack"):
                raise NotImplementedError("Heaptrack is not installed on this Linux machine!\n" \
                    "(If you are running in docker, consider running with debug image, or install heaptrack)")

        output_folder_name = name.lower() + "_logs"
        test_suite_name = os.environ.get('TEST_NAME')
        if os.environ.get('BLOCKCHAIN_VERSION', default="1") == "4" :
            test_suite_name += "_v4"
        self.output_log_path = os.path.join(builddir, output_folder_name, test_suite_name, current_test)
        if name in self.sanitizer_names:
            self.output_log_prefix = name.lower() + ".log"
            # add log path to sanitizer options
            sanitizer_options_env_key = name + "_OPTIONS"
            sanitizer_options = os.environ[sanitizer_options_env_key].partition(':')
            assert len(sanitizer_options) > 0
            sanitizer_options_env_val = ""
            i = 0
            while i < len(sanitizer_options):
                if "log_path=" in sanitizer_options[i]: # remove the older log path
                    i = i + 2
                else:
                    sanitizer_options_env_val += sanitizer_options[i]
                    i = i + 1
            sanitizer_options_env_val += ":log_path=" + os.path.join(self.output_log_path, self.output_log_prefix)
            os.environ[sanitizer_options_env_key] = sanitizer_options_env_val
        # re-create the test output folder (or create if it was never there). Same as log output, we delete everything from old tests
        shutil.rmtree(self.output_log_path, ignore_errors=True)
        Path(self.output_log_path).mkdir(parents=True, exist_ok=True)

    def process_pids_after_replica_started(self, started_proc_info, started_replica_id):
        '''
        Some tools may spawn skvbc_replica as child process. We need to keep track of the process information to be able to signal
        for termination at later stages.
        Returns: the tester replica process info  (according to the debug tool)
        '''
        assert self.name is not None, "This function should be called only if there is an active debug tool!"
        if self.name == "HEAPTRACK":
            # proc holds heaptrack subprocess object.
            # At later stage we would like to signal the actual replica executable to stop, so lets find its pid using psutil
            for child_proc in psutil.Process(started_proc_info.pid).children():
                if child_proc.name() == 'skvbc_replica':
                    ret = psutil.Process(child_proc.pid)
                    self.external_tool_procs[started_replica_id] = started_proc_info   # external_tool_procs holds Heaptrack's process info
                    break
            assert started_replica_id in self.external_tool_procs
            return ret
        else:
            return started_proc_info

    def process_output(self, stopped_replica_id, stoppped_proc, testdir):
        '''
        Copy and/or extract and/or rename the debug tool logs into the correct output output folder
        '''
        output_log_format = "{}_skvbc_replica_pid_{}_repid_{}_#{}.{}" # tool name, process id, replica id, counter, postfix
        if stopped_replica_id not in self.output_counters:
            self.output_counters[stopped_replica_id] = 0    # May happen in reconfiguration
        counter = self.output_counters[stopped_replica_id]
        if self.name == "HEAPTRACK":
            # Heaptrack output is a gz file. We would like to rename the gz file and extract it to a log file with heaptrack_print
            assert (stopped_replica_id in self.external_tool_procs)
            heaptrack_proc = self.external_tool_procs[stopped_replica_id]
            heaptrack_proc.wait()
            heaptrack_src_log_name = "heaptrack.skvbc_replica.{}.gz".format(heaptrack_proc.pid)
            heaptrack_dst_base = output_log_format.format(self.name.lower(), heaptrack_proc.pid, stopped_replica_id, counter, "{}")
            heaptrack_dst_gz_path = os.path.join(self.output_log_path, heaptrack_dst_base.format("gz"))
            heaptrack_dst_log_path = os.path.join(self.output_log_path, heaptrack_dst_base.format("log"))
            shutil.copyfile(os.path.join(
                testdir, heaptrack_src_log_name), os.path.join(self.output_log_path, heaptrack_dst_gz_path))
            with open(heaptrack_dst_log_path, 'w') as logfile:
                subprocess.call(["heaptrack_print","-f",heaptrack_dst_gz_path,"-t","1","-l","1"], stdout=logfile, shell=False)
            del self.external_tool_procs[stopped_replica_id]
        elif self.name in self.sanitizer_names:
            # The default sanitizer output is <bft_network.output_log_prefix>.pid, e.g 'asan.log.170' where 170 is skvbc_replica pid
            # We would like to rename the file, so that it contains the replica id as well, and a file counter per replica id.
            pid = stoppped_proc.pid
            sanitizer_src_file_path = Path(os.path.join(self.output_log_path, "{}.{}".format(self.output_log_prefix, pid)))
            assert sanitizer_src_file_path.is_file(), f"File {sanitizer_src_file_path} must exist!"
            sanitizer_dst_file_path = os.path.join(self.output_log_path, \
                output_log_format.format(self.name.lower(), pid, stopped_replica_id, counter, "log"))
            sanitizer_src_file_path.rename(sanitizer_dst_file_path)
        self.output_counters[stopped_replica_id] += 1

    def set_tool_in_replica_command(self, cmd):
        '''
        Receives a command with replica binary set in index 0.
        Some tools need to modify the replica command structure, for example: Heaptrack assign "heaptrack" as 1st parameter.
        Returns: the actual index of the replica binary (after shifted).
        If not debug tools int this run, do nothing and returns 0.
        '''
        replica_binary_path_index = 0
        if self.name == "HEAPTRACK":
            # heaptrack invokes the tester replica binary so it becomes the 1st argument
            replica_binary_path_index = 1
            cmd.insert(0, "heaptrack")
        return replica_binary_path_index