#!/usr/bin/python3

import argparse
import os
import subprocess
import threading
import io
import sys
import re
import logging
import time

g_logger = logging.getLogger("simpletest")
g_log_formatter = logging.Formatter(
    "%(asctime)-15s %(levelname)-6s %(""message)s")
g_log_file_handler = logging.FileHandler("test_log.txt")
g_log_console_handler = logging.StreamHandler()
g_log_file_handler.setFormatter(g_log_formatter)
g_log_console_handler.setFormatter(g_log_formatter)
g_logger.addHandler(g_log_file_handler)
g_logger.addHandler(g_log_console_handler)

class CLI:
    def __init__(self, num_it, vc_t):
        self.num_of_iterations = num_it
        self.vc_timeout = vc_t

class BFT:
    def __init__(self, name, n, r, f, c, cl, vc):
        self.name = name
        self.num_of_replicas = n
        self.num_of_replicas_to_run = r
        self.num_of_faulties = f
        self.num_of_slow = c
        self.num_of_clients = cl
        self.test_vc = vc
        self.vc_will_fail = False if not vc else n - r >= f


class Environment:
    def __init__(self, rep_exe, cl_exe, binary_dir, log_dir, no_console):
        self.replica_exec = rep_exe
        self.client_exec = cl_exe
        self.binary_dir = binary_dir
        self.log_dir = log_dir
        self.no_console = no_console


class TestResult:
    def __init__(self, exit_code, user_msg):
        self.exit_code = exit_code
        self.user_msg = user_msg


class ProcessOutput:
    def __init__(self, exec_name, exit_code, user_msg, uid):
        self.uid = uid
        self.user_msg = user_msg
        self.exec_name = exec_name
        self.exit_code = exit_code


class TestProcess:
    def __init__(self,
                 friendly_name,
                 executable,
                 params,
                 log_dir,
                 is_waitable,
                 uid,
                 no_console,
                 on_new_line_callback = None):
        self.friendly_name = friendly_name
        # full path to the executable
        self.executable = executable
        self.log_dir = log_dir
        self.params = params
        self.is_waitable = is_waitable
        self.uid = uid
        self.process = None
        self.thread = None
        self.out_file = None
        self.no_console = no_console
        self.on_new_line_callback = on_new_line_callback

    def invoke_on_new_line(self, txt):
        if self.on_new_line_callback:
            self.on_new_line_callback=self.on_new_line_callback(txt, self.uid)

    def collect(self):
        with io.open(self.out_file, "r") as reader:
            log = reader.read()

        m = re.search("(?<=\n).*(?=\n$)", log)
        if m:
            user_msg = m.group(0)
        else:
            user_msg = "No match"

        res = ProcessOutput("{0} {1}".format(self.friendly_name, self.params),
                         self.process.returncode,
                         user_msg,
                         self.uid)
        return res

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        cmd = [self.executable]
        cmd.extend(self.params)

        g_logger.debug("Run command: {}".format(cmd))

        self.out_file = os.path.join(
            self.log_dir, self.friendly_name + "_" + str(self.uid) + "_out.log")

        FNULL = open(os.devnull, 'w')
        with io.open(self.out_file, "wb", 8196) as out_writer, \
        io.open(self.out_file, "br", 8196) as out_b_reader, \
        io.open(self.out_file, "r", 8196) as out_t_reader:
            self.process = subprocess.Popen(cmd,
                                        bufsize=8196,
                                        stdout=out_writer,
                                        stderr=FNULL)
            # output only client messages
            if self.is_waitable:
                while self.process.poll() is None:
                    t = out_t_reader.readline()
                    self.invoke_on_new_line(t)
                    if not self.no_console:
                        sys.stdout.buffer.write(out_b_reader.read())
                #read leftovers
                if not self.no_console:
                    sys.stdout.buffer.write(out_b_reader.read())
                lines = out_t_reader.readlines()
                for line in lines:
                    self.invoke_on_new_line(line)

    def wait(self):
        self.thread.join()

    def stop(self):
        self.process.terminate()
        self.process.communicate()

    def kill(self):
        self.process.kill()
        self.process.communicate()


class TestConfig:
    iterations_done = 0
    leader_killed = False
    iterations_counter_lock = threading.Lock()
    config_run_timer = None
    config_last_update_time = None

    def __init__(self, num_rep, num_cl, faulty, slow, vc_enabled, vc_timeout,
                 num_it, env, config_name, num_of_rep_to_launch, customize_fn):
        self.num_of_replicas = num_rep
        self.num_of_clients = num_cl
        self.num_of_faulty = faulty
        self.num_of_slow = slow
        self.view_change_enable = vc_enabled
        self.view_change_timeout = vc_timeout
        self.num_of_iterations = num_it
        self.env = env
        self.waitables = []
        self.non_waitables = []
        self.process_outputs = []
        self.config_name = config_name
        self.num_of_replicas_to_launch = num_of_rep_to_launch
        self.on_new_line_callback = self.on_new_line_vc if vc_enabled else \
            self.on_new_line

        self.config_timeout = 60000 + (0 if not vc_enabled else vc_timeout * 5)
        self.customize_fn = customize_fn

    @staticmethod
    def reset_globals():
        TestConfig.iterations_done = 0
        TestConfig.leader_killed = False
        TestConfig.config_run_timer = None
        TestConfig.config_last_update_time = None

    def prepare(self):
        exec_dir = os.path.abspath(self.env.binary_dir)
        time_stamp = time.strftime("%d_%m_%Y_%H_%M_%S")
        log_a_path = os.path.abspath(self.env.log_dir)
        self.log_dir = os.path.join(log_a_path, time_stamp)

        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)

        with io.open(os.path.join(
                self.log_dir, self.config_name.replace(",","_")),"w") as w:
            w.write("dummy file to display configuration in the log folder")

        cmd = ["../../../../tools/GenerateConcordKeys",
               "-n", str(self.num_of_replicas),
               "-f", str(self.num_of_faulty),
               "-o", os.path.join(self.log_dir, "private_replica_")]
        subprocess.check_call(cmd)

        for i in range(0, self.num_of_replicas_to_launch):
            params = ["-id", str(i),
                      "-r", str(self.num_of_replicas),
                      "-c", str(self.num_of_clients)]
            if self.view_change_enable:
                params.extend(["-vc", "-vct", str(self.view_change_timeout)])

            tp = TestProcess(self.env.replica_exec,
                             os.path.join(exec_dir, self.env.replica_exec),
                             params,
                             self.log_dir,
                             False,
                             i,
                             self.env.no_console)
            self.non_waitables.append(tp)

        for i in range(0, self.num_of_clients):
            params = ["-i", str(self.num_of_iterations),
                      "-r", str(self.num_of_replicas),
                      "-cl", str(self.num_of_clients),
                      "-id", str(self.num_of_replicas + i),
                      "-f", str(self.num_of_faulty),
                      "-c", str(self.num_of_slow)]

            tp = TestProcess(self.env.client_exec,
                             os.path.join(exec_dir, self.env.client_exec),
                             params,
                             self.log_dir,
                             True,
                             self.num_of_replicas + i,
                             self.env.no_console,
                             self.on_new_line_callback)
            self.waitables.append(tp)

    def print_name(fn):
        def wrapper(self):
            global g_logger
            g_logger.info('Start config "{}"'.format(self.config_name))
            fn(self)
            g_logger.info('End config "{}"'.format(self.config_name))
        return wrapper

    def kill_replica(self, id):
        self.non_waitables[id].kill()
        del self.non_waitables[id]

    def kill_all(self):
        for p in self.waitables + self.non_waitables:
            p.kill()

    # this method is used to determine total number of iterations been proceed
    # so far by clients and to kill primary when needed
    def on_new_line(self, text, instanceId):
        if text is None:
            return
        pattern = re.compile(r"(?<=^Iterations count:\s)\d+")
        TestConfig.iterations_counter_lock.acquire()
        found = False
        for m in re.finditer(pattern, text):
            found = True
            TestConfig.iterations_done += int(m.group(0))
            g_logger.info(f"From {instanceId}, num of iterations done: " +
                          f"{TestConfig.iterations_done}")
        if found:
            TestConfig.config_last_update_time=int(round(time.time()*1000))
        TestConfig.iterations_counter_lock.release()
        return self.on_new_line

    def on_new_line_vc(self, text, instanceId):
        retval = self.on_new_line_vc
        self.on_new_line(text, instanceId)
        TestConfig.iterations_counter_lock.acquire()
        if TestConfig.iterations_done >= self.num_of_iterations * int(
                self.num_of_clients) / 2 and not TestConfig.leader_killed:
            g_logger.info(f"From: {instanceId}, killing replica 0")
            self.kill_replica(0)
            TestConfig.leader_killed = True
            g_logger.info("Killed replica 0")
            retval = self.on_new_line
        TestConfig.iterations_counter_lock.release()
        return retval

    def test_timer_handler(self):
        millis = int(round(time.time() * 1000))
        if TestConfig.config_last_update_time is not None and \
                millis-TestConfig.config_last_update_time > self.config_timeout:
            g_logger.error("Timeout, killing all...")
            self.kill_all()
            self.collect()
            g_logger.error("Timeout, all killed")
        else:
            if TestConfig.config_last_update_time is not None \
            and millis - TestConfig.config_last_update_time > 10000:
                g_logger.debug("No data from clients in last 10 seconds")
            TestConfig.config_timer=threading.Timer(10, self.test_timer_handler)
            TestConfig.config_timer.start()

    @print_name
    def run(self):
        curr_dir = os.getcwd()
        os.chdir(self.log_dir)
        for tp in self.non_waitables + self.waitables:
            tp.start()

        self.test_timer_handler()

        for tp in self.waitables:
            tp.wait()
            TestConfig.config_timer.cancel()
            res = tp.collect()
            self.process_outputs.append(res)
        for tp in self.non_waitables:
            tp.stop()
            res = tp.collect()
            self.process_outputs.append(res)

        os.chdir(curr_dir)

    def collect(self):
        if self.customize_fn:
            self.customize_fn(self)
        return self.process_outputs

    @staticmethod
    def create(name, env, cli, n, r, f, c, cl, vc, customize_fn):
        return TestConfig(n, cl, f, c, vc, cli.vc_timeout,
                          int(cli.num_of_iterations), env,
                          name.replace("+", "_"), r, customize_fn)


def process_output(process_outputs):
    res = ""
    exit_code = 0
    for output in process_outputs:
        res += "{0} is done with exit code {1} and message '{2}'".format(
            output.exec_name,
            output.exit_code,
            output.user_msg)
        res += os.linesep
        exit_code += output.exit_code
    g_logger.debug(f"output: {res}, {exit_code}")
    return TestResult(exit_code, res)


def customize_test_vc_never_ends(test_config):
    for o in test_config.process_outputs:
        o.exit_code = 0
        o.user_message = "Success"


def main():
    parser = argparse.ArgumentParser(
        description="Simple test automation script",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-bd",
                        help="Folder with BFT executables",
                        default="..")
    parser.add_argument("-ld",
                        help="Directory for log files",
                        default=".")
    parser.add_argument("-nc",
                        help="Do not print processes' output to the console",
                        action="store_true")
    parser.add_argument("-l",
                        help="Set log level for this script (not for the " +
                        "executables)",
                        default="INFO")
    parser.add_argument("-i",
                        help="Set number of operations to run",
                        default="2800")
    parser.add_argument("-vct",
                        default=60000,
                        type=int,
                        help="View change timeout (if VC is enabled)")
    parser.add_argument("-bft",
                       default=["n=4,r=4,f=1,c=0,cl=1"],
                       nargs="+",
                       help="List of BFT configuration sets to use. All " +
                       "parameters should be separated by ','. " +
                       "n - total number of " + "replicas, r - number of " +
                       "running replicas, " +
                       "f - max. number of byzantine replicas, " +
                       "c - max. number of slow replicas, " +
                       "cl - number of running clients, " +
                       "testViewChange - if present, viewChange will be " +
                       "triggered after 1/2 of iterations done. " +
                       "Please note, if View change is enabled AND n - r " +
                       "<= f - the system will not progress after primary " +
                       "is killed, and the test will SUCCESS after timeout " +
                       "- this is desired behaviour for this configuration." +
                       "You can provide set " +
                       "of configurations in the form of " +
                       "set1 set2 .... setN - " +
                       "each of them will be ran as separate test")

    args = parser.parse_args()
    env = Environment("server", "client", args.bd, args.ld, args.nc)

    global g_logger
    g_logger.setLevel(args.l)

    print(args.bft)

    configs = []
    for param in args.bft:
        set = param.split(",")
        print(set)
        n=r=f=c=cl=None
        vc=False
        for p_expr in set:
            p = p_expr.split("=")
            if p[0] == "n":
                n = int(p[1])
            elif p[0] == "r":
                r = int(p[1])
            elif p[0] == "f":
                f = int(p[1])
            elif p[0] == "c":
                c = int(p[1])
            elif p[0] == "cl":
                cl = int(p[1])
            elif p[0] == "testViewChange":
                vc = True
            else:
                print(f"Unsupported parameter: {p[0]}")
                return -1
        if n is None or r is None or f is None or c is None or cl is None:
            print("Unable to parse BFT parameters")
            return -1
        configs.append(BFT(param, n, r, f, c, cl, vc))

    cli = CLI(args.i, args.vct)
    for c in configs:
        g_logger.debug(f"Creating test configuratiom {c.name} " +
                       f" with {c.num_of_replicas_to_run} running replicas, " +
                       f"f = {c.num_of_faulties}, c = {c.num_of_slow}")

        c = TestConfig.create(c.name, env, cli, c.num_of_replicas,
              c.num_of_replicas_to_run, c.num_of_faulties,
              c.num_of_slow, c.num_of_clients, c.test_vc,
              customize_test_vc_never_ends if c.vc_will_fail else None)

        TestConfig.reset_globals()
        c.prepare()
        c.run()
        out = c.collect()
        result = process_output(out)
        if result.exit_code != 0:
            print("CONFIGURATION RUN FAIL")
            # THIS IS DONE ON PURPOSE TO SUPPORT AUTOMATIC TESTING
            print("TESTS FAIL")
            return result.exit_code
        else:
            print("CONFIGURATION RUN SUCCESS")

    print("TEST SUCCESS")
    return 0

main()
