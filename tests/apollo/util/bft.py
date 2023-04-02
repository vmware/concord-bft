# Concord
#
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

# Add the pyclient directory to $PYTHONPATH
import hashlib
import pathlib
from logging import setLogRecordFactory
import sys
import signal
import psutil
import os
import os.path
import shutil
import random
import subprocess
from collections import namedtuple, Counter, OrderedDict
import tempfile
from functools import wraps, lru_cache
from datetime import datetime
from functools import partial
import inspect
import time
from pathlib import Path
from re import sub
from typing import Coroutine, Sequence, Callable, Optional, Tuple, Dict, TextIO
import re
import trio
import json

from util.test_base import repeat_test, ApolloTest
from util.consts import CHECKPOINT_SEQUENCES

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "pyclient"))

import util.bft_debug_tool
import bft_config
import bft_client
import bft_metrics_client
import inspect
from enum import Enum
from util import bft_metrics, eliot_logging as log
from util.eliot_logging import log_call, logdir_timestamp, log_message, add_destinations, remove_destination, \
    format_eliot_message
from util import skvbc as kvbc
from util.bft_test_exceptions import AlreadyRunningError, AlreadyStoppedError, KeyExchangeError, CreError
from bft_config import BFTConfig
from threading import Thread, Event
from time import sleep

DB_FILE_PREFIX = "simpleKVBTests_DB_"
DB_SNAPSHOT_PREFIX = DB_FILE_PREFIX + "snapshot_"

TestConfig = namedtuple('TestConfig', [
    'n',
    'f',
    'c',
    'num_clients',
    'key_file_prefix',
    'start_replica_cmd',
    'stop_replica_cmd',
    'num_ro_replicas'
])

# NOTE: When the value is changed, then ensure to change in ReplicaConfig class'
# 'operatorMsgSigningAlgo' value also.
operator_msg_signing_algo = "eddsa" # or "ecdsa"

class ConsensusPathPrevalentResult(Enum):
   OK = 0
   TOO_FEW_REQUESTS_ON_EXPECTED_PATH = 1
   TOO_MANY_REQUESTS_ON_UNEXPECTED_PATH = 2

class ConsensusPathType(Enum):
    SLOW = 'slow'
    FAST_WITH_THRESHOLD = 'fast_with_threshold'
    OPTIMISTIC_FAST = 'optimistic_fast'

PathMetricParams = namedtuple('PathMetricParams', ['metric_name', 'timeout', 'interval'])
PATH_TYPE_TO_METRIC_PARAMS = {
    ConsensusPathType.SLOW: PathMetricParams(metric_name='CommittedSlow', timeout=5., interval=.5),
    ConsensusPathType.FAST_WITH_THRESHOLD: PathMetricParams(metric_name='CommittedFastThreshold',
                                                            timeout=5., interval=2.),
    ConsensusPathType.OPTIMISTIC_FAST: PathMetricParams(metric_name='CommittedFast',
                                                        timeout=5., interval=2.),
}

KEY_FILE_PREFIX = "replica_keys_"
# bft_client.py  implement 2 types of clients currently:
# UdpClient for UDP communication and TcpTlsClient for TCP/TLS communication
BFT_CLIENT_TYPE = bft_client.TcpTlsClient if os.environ.get('BUILD_COMM_TCP_TLS', "").lower() == "true" \
                                          else bft_client.UdpClient

# For better performance, we Would like to keep the next constant as minimal as possible.
# If you need more clients, increase with caution.
# Reserved clients (RESERVED_CLIENTS_QUOTA) are not part of NUM_CLIENTS
RESERVED_CLIENTS_QUOTA = 2
NUM_PARTICIPANTS = 5


def interesting_configs(config_filter=None) -> Sequence[BFTConfig]:
    config_filter = config_filter or (lambda n, f, c: c == 0)
    bft_configs = [BFTConfig(n=6, f=1, c=1),
                   BFTConfig(n=7, f=2, c=0),
                   #BFTConfig(n=4, f=1, c=0),
                   #BFTConfig(n=9, f=2, c=1),
                   #BFTConfig(n=12, f=3, c=1),
                   ]

    selected_bft_configs = list(filter(lambda config: config_filter(config.n, config.f, config.c), bft_configs))
    assert len(selected_bft_configs) > 0, "No eligible BFT configs"
    return selected_bft_configs

def with_trio(async_fn):
    """ Decorator for running a coroutine (async_fn) with trio. """
    @wraps(async_fn)
    def trio_wrapper(*args, **kwargs):
        if "already_in_trio" in kwargs:
            kwargs.pop("already_in_trio")
            return async_fn(*args, **kwargs)
        else:
            return trio.run(async_fn, *args, **kwargs)

    return trio_wrapper

def skip_for_tls(async_fn):
    """ Decorator for skipping the test for TCP/TLS. """
    @wraps(async_fn)
    def wrapper(*args, **kwargs):
        if os.environ.get('BUILD_COMM_TCP_TLS', "").lower() != "true":
            return async_fn(*args, **kwargs)
        else:
            pass

    return wrapper

def with_constant_load(async_fn):
    """
    Runs the decorated async function in parallel with constant load,
    generated by a dedicated (reserved) client. In order to work
    the wrapper assumes a 'bft_network' will be created and passed
    as a keyword argument. The wrapper also creates a 'skvbc' object
    and a 'nursery' which it passes to the wrapped function.
    """
    @wraps(async_fn)
    async def wrapper(*args, **kwargs):
        if "bft_network" in kwargs:
            bft_network = kwargs.pop("bft_network")
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            client = bft_network.new_reserved_client()
            log.log_message(message_type=f"Constant load client id: {client.client_id}")

            async def background_sender(arg1, arg2, *, task_status=trio.TASK_STATUS_IGNORED):
                with trio.CancelScope() as scope:
                    task_status.started(scope)
                    await skvbc.send_indefinite_write_requests(arg1, arg2)

            async with trio.open_nursery() as nursery:
                constant_load = await nursery.start(background_sender, client, 1)  # send a request every second
                await async_fn(*args, **kwargs, bft_network=bft_network, skvbc=skvbc, constant_load=constant_load)
                nursery.cancel_scope.cancel()
    return wrapper

def with_bft_network(start_replica_cmd, selected_configs=None, num_clients=None, num_ro_replicas=0,
                     rotate_keys=False, bft_configs=None, with_cre=False, publish_master_keys=False,
                     num_repeats=int(os.getenv("NUM_REPEATS", 1)),
                     break_on_first_failure=bool(os.getenv('BREAK_ON_FAILURE') == 'TRUE'),
                     break_on_first_success=False, use_unified_certs=False):
    """
    Runs the decorated async function for all selected BFT configs
    start_replica_cmd is a callback which is used to start a replica. It should have the following
    signature:
        def start_replica_cmd(builddir, replica_id)
    or
        def start_replica_cmd(builddir, replica_id, config)
    If you want the bft test network configuration to be passed to your callback you should add
    third parameter named 'config' (the exact name is important!).
    If you don't need this configuration - use two parameters callback with any names you want.
    """
    assert not(break_on_first_failure and break_on_first_success), \
        "Cannot break on first failure and on first success at the same time"
    def decorator(async_fn):
        @wraps(async_fn)
        async def wrapper(*args, **kwargs):
            test_instance = args[0]
            if "bft_network" in kwargs:
                bft_network = kwargs.pop("bft_network")
                bft_network.is_existing = True
                with log.start_task(action_type="Running test with network", test_name=async_fn.__name__):
                    await async_fn(*args, **kwargs, bft_network=bft_network)
            else:
                configs = bft_configs if bft_configs is not None else interesting_configs(selected_configs)
                log.log_message(message_type="Selected configs",
                                configs=[f'{config}_clients={config.clients}' for config in configs])
                for bft_config in configs:
                    test_name = f'{async_fn.__name__}_{bft_config}_clients={bft_config.clients}'
                    config = TestConfig(n=bft_config.n,
                                        f=bft_config.f,
                                        c=bft_config.c,
                                        num_clients=bft_config.clients \
                                            if num_clients is None \
                                            else num_clients,
                                        key_file_prefix=KEY_FILE_PREFIX,
                                        start_replica_cmd=start_replica_cmd,
                                        stop_replica_cmd=None,
                                        num_ro_replicas=num_ro_replicas)
                    subtest_id = None
                    with test_instance.subTest(config=f'{bft_config}',
                                               storage=f"v{os.environ.get('BLOCKCHAIN_VERSION', default='v1')}"):
                        subtest_id = test_instance._subtest.id()
                        ApolloTest.FAILED_CASES[subtest_id] = 'Case aborted by an external signal'
                        async with trio.open_nursery() as background_nursery:
                            @repeat_test(num_repeats, break_on_first_failure, break_on_first_success, test_name)
                            async def test_with_bft_network():
                                with BftTestNetwork.new(
                                        config, background_nursery, with_cre=with_cre,
                                        use_unified_certs=use_unified_certs, case_name=test_name) as bft_network:
                                    seed = test_instance.test_seed
                                    with log.start_task(action_type=f"{async_fn.__name__}",
                                                        bft_config=f'{bft_config}_clients={config.num_clients}',
                                                        seed=seed, repeats=num_repeats,
                                                        repeat_until='first failure' if break_on_first_failure else
                                                        'first success' if break_on_first_success else
                                                        f'{num_repeats} repetitions'):
                                        random.seed(seed)
                                        bft_network.debug_tool = util.bft_debug_tool.BftDebugTool( \
                                            bft_network.config.n + bft_network.config.num_ro_replicas, \
                                            bft_network.builddir, bft_network.current_test)
                                        if rotate_keys:
                                            await bft_network.check_initial_key_exchange(
                                                check_master_key_publication=publish_master_keys)
                                        bft_network.test_start_time = time.time()
                                        await async_fn(*args, **kwargs, bft_network=bft_network)
                            await test_with_bft_network()
                    ApolloTest.FAILED_CASES.pop(subtest_id)
                    test_instance.register_errors()

        return wrapper
    return decorator

MAX_MSG_SIZE = 64*1024 # 64k
REQ_TIMEOUT_MILLI = 5000 * 2
RETRY_TIMEOUT_MILLI = 250
METRICS_TIMEOUT_SEC = 5

# TODO: This is not generic, but is required for use by SimpleKVBC. In the
# future we will likely want to change how we determine the lengths of keys and
# values, make them parameterizable, or generate keys in the protocols rather
# than tester. For now, all keys and values must be 21 bytes.
KV_LEN = 21

class BftTestNetwork:
    """Encapsulates a BFT network instance for testing purposes"""

    def write_message(self, message):
        self._eliot_log_file.write(f"{json.dumps(message)}\n")

    def __enter__(self):
        """context manager method for 'with' statements"""
        if not self._eliot_log_file:
            self._eliot_log_file = (self.current_test_case_path / 'eliot.log').open('a+')
        add_destinations(self.write_message)
        return self

    def __exit__(self, etype, value, tb):
        """context manager method for 'with' statements"""
        if not self.is_existing:
            for client in self.clients.values():
                client.__exit__()
            for client in self.reserved_clients.values():
                client.__exit__()
            if self.with_cre and self.cre_proc is not None:
                if os.environ.get('GRACEFUL_SHUTDOWN', "").lower() in set(["true", "on"]):
                    self.cre_proc.terminate()
                else:
                    self.cre_proc.kill()
                for fd in self.cre_fds:
                    fd.close()
                self.cre_proc.wait()
            self.metrics.__exit__()
            self.stop_all_replicas()
            os.chdir(self.origdir)
            if not os.environ.get('KEEP_TEST_DIR', None):
                dirs_to_remove = (self.testdir, self.certdir, self.txn_signing_keys_base_path)
                log.log_message(message_type=f'Removing test directories', dirs=dirs_to_remove)
                for dir_to_remove in dirs_to_remove:
                    shutil.rmtree(dir_to_remove, ignore_errors=True)
            if self.test_start_time:
                message = f"test duration = {time.time() - self.test_start_time} seconds"
                log.log_message(message_type=message)
                if self.current_test_case_path:
                    with open(os.path.join(self.current_test_case_path, "test_duration.log"), "w+") as log_file:
                        log_file.write(f"{message}\n")
            if self.perf_proc:
                self.perf_proc.wait()

            self.check_error_logs()
            remove_destination(self.write_message)
            if self._eliot_log_file:
                self._eliot_log_file.close()
                self._eliot_log_file = None

    def __init__(self, is_existing, origdir, config, testdir, certdir, builddir, toolsdir,
                 procs, replicas, clients, metrics, client_factory, background_nursery, ro_replicas=[],
                 case_name=""):
        self.is_existing = is_existing
        # An existing deployment might pass some of the folders paths as empty so we skip the next assertion.
        if not is_existing:
            self.assert_dirs_exist([origdir, testdir, certdir, builddir, toolsdir])

        self.origdir = origdir
        self.config = config
        self.testdir = testdir
        self.certdir = certdir
        self.builddir = builddir
        self.toolsdir = toolsdir
        self.procs = procs
        self.subproc_monitors: Dict[int, Tuple[Thread, Event, TextIO]] = dict()
        self.replicas = replicas
        # Make sure that client order is deterministic so that
        # seeded random client choices are deterministic
        self.clients = OrderedDict(clients)
        self.metrics = metrics
        self.reserved_clients = {}
        self.debug_tool = None # As of now, debug tool is set only on is_existing=False runs
        self.reserved_client_ids_in_use = []
        if client_factory:
            self.client_factory = client_factory
        else:
            self.client_factory = partial(self._create_new_client, BFT_CLIENT_TYPE)
        self.open_fds = {}
        self.current_test = case_name
        self.background_nursery = background_nursery
        self.test_start_time = None
        self.perf_proc = None
        self.ro_replicas = ro_replicas
        self.txn_signing_enabled = (os.environ.get('TXN_SIGNING_ENABLED', "").lower() in ["true", "on"])
        self.with_cre = False
        self.use_unified_certs = False
        self.cre_proc = None
        self.cre_fds = None
        self.cre_id = self.config.n + self.config.num_ro_replicas + self.config.num_clients + RESERVED_CLIENTS_QUOTA
        self._logs_dir = Path(self.builddir) / "tests" / "apollo" / "logs" / logdir_timestamp()
        self._eliot_log_file = None
        self._suite_name = os.environ.get('TEST_NAME', None)
        if not self._suite_name:
            now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
            self._suite_name = f"{now}_{self.current_test}"

        if os.environ.get('BLOCKCHAIN_VERSION', default="1").lower() == "4":
            self._suite_name = self._suite_name + "_v4"

        self.current_test_case_path = self._logs_dir / self._suite_name / self.current_test
        self.current_test_case_path.mkdir(parents=True, exist_ok=True)

        # Setup transaction signing parameters
        self.setup_txn_signing()
        self._generate_operator_keys()

    @classmethod
    def new(cls, config, background_nursery, client_factory=None, with_cre=False, use_unified_certs=False,
            case_name=None):
        builddir = os.path.abspath("../../build")
        toolsdir = os.path.join(builddir, "tools")
        testdir = tempfile.mkdtemp(prefix='testdir_')
        certdir = tempfile.mkdtemp(prefix='certdir_')
        cls.assert_dirs_exist([testdir, certdir, builddir, toolsdir])
        bft_network = cls(
            is_existing=False,
            origdir=os.getcwd(),
            config=config,
            testdir=testdir,
            certdir=certdir,
            builddir=builddir,
            toolsdir=toolsdir,
            procs={},
            replicas=[bft_config.Replica(i, "127.0.0.1",
                                         bft_config.bft_msg_port_from_node_id(i),
                                         bft_config.metrics_port_from_node_id(i))
                      for i in range(0, config.n + config.num_ro_replicas)],
            clients={},
            metrics=None,
            client_factory=client_factory,
            background_nursery=background_nursery,
            ro_replicas=[bft_config.Replica(i, "127.0.0.1",
                                            bft_config.bft_msg_port_from_node_id(i),
                                            bft_config.metrics_port_from_node_id(i))
                         for i in range(config.n, config.n + config.num_ro_replicas)],
            case_name=case_name
        )
        bft_network.with_cre = with_cre
        bft_network.use_unified_certs = use_unified_certs
        # Copy logging.properties file
        shutil.copy(os.path.abspath("../simpleKVBC/scripts/logging.properties"), testdir)

        log.log_message(message_type=f"Running test in {bft_network.testdir}", build_dir=builddir, tools_dir=toolsdir,
                        cert_dir=certdir)

        os.chdir(bft_network.testdir)
        bft_network._generate_crypto_keys()
        if bft_network.comm_type() == bft_config.COMM_TYPE_TCP_TLS:
            generate_cre = 0 if bft_network.with_cre is False else 1
            # Generate certificates for all replicas, clients, and reserved clients
            bft_network.generate_tls_certs(
                bft_network.num_total_replicas() + config.num_clients + RESERVED_CLIENTS_QUOTA + generate_cre, use_unified_certs=use_unified_certs)

        bft_network._init_metrics()
        bft_network._create_clients()
        bft_network._create_reserved_clients()

        return bft_network

    @classmethod
    def existing(cls, config, replicas, clients, client_factory=None, background_nursery=None, builddir=None):
        certdir = None
        if not client_factory:
            certdir = tempfile.mkdtemp()
            assert background_nursery is not None, "You must transfer a background nursery which lasts for all test duration!"
        bft_network = cls(
            is_existing=True,
            origdir=None,
            config=config,
            testdir=None,
            certdir=certdir,
            builddir=builddir,
            toolsdir=None,
            procs={r.id: r for r in replicas},
            replicas=replicas,
            clients={i: clients[i] for i in range(len(clients))},
            metrics=None,
            client_factory=client_factory,
            background_nursery = background_nursery
        )

        bft_network._init_metrics()
        bft_network._create_reserved_clients()
        return bft_network

    def start_cre(self):
        """
        Start the cre if it isn't already started.
        Otherwise raise an AlreadyStoppedError.
        """
        with log.start_action(action_type="start_cre"):
            stdout_file = None
            stderr_file = None

            if os.environ.get('KEEP_APOLLO_LOGS', "").lower() in ["true", "on"]:
                test_log = f"{self.current_test_case_path / 'stdout_cre.log' }"

                Path(self.current_test_case_path).mkdir(parents=True, exist_ok=True)

                stdout_file = open(test_log, 'a+')
                stderr_file = open(test_log, 'a+')

                stdout_file.write("############################################\n")
                stdout_file.flush()
                stderr_file.write("############################################\n")
                stderr_file.flush()

                self.cre_fds = (stdout_file, stderr_file)
                cre_exe = os.path.join(self.builddir, "tests", "simpleKVBC", "TesterCRE", "skvbc_cre")
                cre_cmd = [cre_exe,
                           "-i", str(self.cre_id),
                           "-f", str(self.config.f),
                           "-c", str(self.config.c),
                           "-r", str(self.config.n),
                           "-k", self.certdir + "/" + str(self.cre_id),
                           "-t", os.path.join(self.txn_signing_keys_base_path, "transaction_signing_keys",
                                              str(self.principals_to_participant_map[self.cre_id]),
                                              "transaction_signing_priv.pem"),
                           "-o", "1000",
                           "-U", str(int(self.use_unified_certs))]
                digest = self.binary_digest(cre_exe) if Path(cre_exe).exists() else 'Unknown'
                with log.start_action(action_type="start_reconfiguration_client_process", binary=cre_exe,
                                      binary_digest=digest):
                    self.cre_proc = subprocess.Popen(
                        cre_cmd,
                        stdout=stdout_file,
                        stderr=stderr_file,
                        close_fds=True)

    def stop_cre(self):
        with log.start_action(action_type="stop_cre"):
            p = self.cre_proc
            if os.environ.get('GRACEFUL_SHUTDOWN', "").lower() in set(["true", "on"]):
                p.terminate()
            else:
                p.kill()
            for fd in self.cre_fds:
                fd.close()
            p.wait()

    def transfer_db_files(self, source_id: int, dest_ids: Sequence[int]):
        with log.start_action(action_type="transfer db files", source_id=source_id, dests=dest_ids):
            source_db_dir = os.path.join(self.testdir, DB_FILE_PREFIX + str(source_id))
            for r in dest_ids:
                dest_db_dir = os.path.join(self.testdir, DB_FILE_PREFIX + str(r))
                res = subprocess.run(['cp', '-rf', source_db_dir, dest_db_dir])
                log.log_message(message_type=f"copy db files from {source_db_dir} to {dest_db_dir}, result is {res.returncode}")

    async def change_configuration(self, config, generate_tls=False, use_unified_certs=False):
        with log.start_action(action_type="change_configuration"):
            self.config = config
            self.replicas = [bft_config.Replica(i, "127.0.0.1",
                                                bft_config.bft_msg_port_from_node_id(i), bft_config.metrics_port_from_node_id(i))
                             for i in range(0, config.n + config.num_ro_replicas)]

            self._generate_crypto_keys()

            if generate_tls and self.comm_type() == bft_config.COMM_TYPE_TCP_TLS:
                generate_cre = 0 if self.with_cre is False else 1
                # Generate certificates for replicas, clients, and reserved clients
                self.generate_tls_certs(self.num_total_replicas() + config.num_clients + RESERVED_CLIENTS_QUOTA + generate_cre, use_unified_certs=use_unified_certs)

    def restart_clients(self, generate_tx_signing_keys=True, restart_replicas=True):
        with log.start_action(action_type="restart_clients", generate_tx_signing_keys=generate_tx_signing_keys,
                              restart_replicas=restart_replicas):
            # remove all existing clients
            for client in self.clients.values():
                client.__exit__()
            for client in self.reserved_clients.values():
                client.__exit__()
            self.reserved_client_ids_in_use = []
            self.metrics.__exit__()
            self.clients = {}

            if generate_tx_signing_keys:
                # remove existing transaction signing keys and generate again
                shutil.rmtree(self.txn_signing_keys_base_path, ignore_errors=True)
                self.setup_txn_signing()
                if restart_replicas:
                    # Now, we must restart the replicas
                    self.stop_all_replicas()
                    self.start_all_replicas()

            self._init_metrics()
            self._create_clients()
            self._create_reserved_clients()

    def _generate_crypto_keys(self):
        keygen = os.path.join(self.toolsdir, "GenerateConcordKeys")
        args = [keygen, "-n", str(self.config.n), "-f", str(self.config.f)]
        if self.config.num_ro_replicas > 0:
            args.extend(["-r", str(self.config.num_ro_replicas)])
        args.extend(["-o", self.config.key_file_prefix])
        with log.start_action(action_type="Key Generation", cmdline=' '.join(args)):
            subprocess.run(args, check=True)

    def _generate_operator_keys(self):
        if self.builddir is None:
            return
        with open(os.path.join(self.builddir, "operator_pub.pem"), 'w') as f:
            if ("ecdsa" == operator_msg_signing_algo):
                # ECDSA public key.
                f.write("-----BEGIN PUBLIC KEY-----\n"
                        "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAENEMHcbJgnnYxfa1zDlIF7lzp/Ioa"
                        "NfwGuJpAg84an5FdPALwZwBp/m/X3d8kwmfZEytqt2PGMNhHMkovIaRI1A==\n"
                        "-----END PUBLIC KEY-----")
            elif ("eddsa" == operator_msg_signing_algo):
                # EdDSA public key.
                f.write("-----BEGIN PUBLIC KEY-----\n"
                        "MCowBQYDK2VwAyEAq6x6mTckhvzscZmDtRAwgneYpIE3sqkdLdaZ4B5JBbw=\n"
                        "-----END PUBLIC KEY-----")
        with open(os.path.join(self.builddir, "operator_priv.pem"), 'w') as f:
            if ("ecdsa" == operator_msg_signing_algo):
                # ECDSA private key.
                f.write("-----BEGIN EC PRIVATE KEY-----\n"
                        "MHcCAQEEIEWf8ZTkCWbdA9WrMSNGCC7GQxvSXiDlU6dlZAi6JaCboAoGCCqGSM49"
                        "AwEHoUQDQgAENEMHcbJgnnYxfa1zDlIF7lzp/IoaNfwGuJpAg84an5FdPALwZwBp"
                        "/m/X3d8kwmfZEytqt2PGMNhHMkovIaRI1A==\n"
                        "-----END EC PRIVATE KEY-----")
            elif ("eddsa" == operator_msg_signing_algo):
                # EdDSA private key.
                f.write("-----BEGIN PRIVATE KEY-----\n"
                        "MC4CAQAwBQYDK2VwBCIEIIIyaCtHzqPqMdvvcTIp+ZtOGccurc7e8qMPs8+jt0xo\n"
                        "-----END PRIVATE KEY-----")

    def generate_tls_certs(self, num_to_generate, start_index=0, use_unified_certs=False):
        """
        Generate 'num_to_generate' certificates and private keys. The certificates are generated on a given range of
        node IDs folders [start_index, start_index+num_to_generate-1] into an output certificate root folder
        self.certdir.
        Since every node might be a potential client and/or server, each node certificate folder consists of the
        subfolders 'client' and 'server'. Each subfolder holds an X.509 certificate client.cert or server.cert and the
        matching private key pk.pem file (PEM format). All certificates are generated using bash script
        create_tls_certs.sh.
        The output is generated into the test folder to a folder called 'certs'.
        """
        certs_gen_script_path = os.path.join(self.builddir, "tests/simpleTest/scripts/create_tls_certs.sh")
        # If not running TLS, just exit here. keep certs_path to pass it by default to any type of replicas
        # We want to save time in non-TLs runs, avoiding certificate generation
        temp_cert_dir = self.certdir + "/tmp"
        Path(temp_cert_dir).mkdir(parents=True, exist_ok=True)
        args = [certs_gen_script_path, str(num_to_generate), temp_cert_dir, str(start_index), str(int(use_unified_certs))]
        subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
        for c in range(num_to_generate):
            comp_cert_dir = os.path.join(self.certdir, str(c))
            shutil.rmtree(comp_cert_dir, ignore_errors=True)
            shutil.copytree(temp_cert_dir, comp_cert_dir)
        shutil.rmtree(temp_cert_dir, ignore_errors=True)

    def copy_certs_from_server_to_clients(self, src):
        src_cert = self.certdir + "/" + str(src)
        for c in range(self.num_total_replicas(), self.num_total_replicas() + self.config.num_clients + RESERVED_CLIENTS_QUOTA):
            comp_cert_dir = self.certdir + "/" + str(c)
            shutil.rmtree(comp_cert_dir, ignore_errors=True)
            shutil.copytree(src_cert, comp_cert_dir)

    def _create_clients(self):
        start_id = self.config.n + self.config.num_ro_replicas
        last_id = start_id + self.config.num_clients
        log_message(message_type=f"Creating clients", first_client_id=start_id, last_client_id=last_id,
                    communication=str(BFT_CLIENT_TYPE), config_template=self._bft_config('client_id'))
        for client_id in range(start_id, last_id):
            self.clients[client_id] = self.client_factory(client_id)

    def _create_new_client(self, client_class, client_id):
        config = self._bft_config(client_id)
        ro_replicas = [r.id for r in self.ro_replicas]
        return client_class(config, self.replicas, self.background_nursery, ro_replicas=ro_replicas)

    def _create_reserved_clients(self):
        first_id = self.num_total_replicas() + self.config.num_clients
        for reserved_client_id in range(first_id, first_id + RESERVED_CLIENTS_QUOTA):
            self.reserved_clients[reserved_client_id] = self.client_factory(reserved_client_id)

    def new_reserved_client(self):
        if len(self.reserved_client_ids_in_use) == RESERVED_CLIENTS_QUOTA:
            raise NotImplemented("You must increase RESERVED_CLIENTS_QUOTA, see comment above")
        start_id = self.num_total_replicas() + self.config.num_clients
        reserved_client_ids = [ id for id in range(start_id, start_id + RESERVED_CLIENTS_QUOTA) ]
        free_reserved_client_ids = set(reserved_client_ids) - set(self.reserved_client_ids_in_use)
        reserved_client_id = next(iter(free_reserved_client_ids))
        self.reserved_client_ids_in_use.append(reserved_client_id)
        return self.reserved_clients[reserved_client_id]

    def _bft_config(self, client_id):
        return bft_config.Config(client_id,
                                 self.config.f,
                                 self.config.c,
                                 MAX_MSG_SIZE,
                                 REQ_TIMEOUT_MILLI,
                                 RETRY_TIMEOUT_MILLI,
                                 self.certdir + "/" + str(client_id),
                                 self.txn_signing_keys_base_path,
                                 self.principals_to_participant_map,
                                 self.use_unified_certs)

    def _init_metrics(self):
        metric_clients = {}
        for r in self.replicas:
            metric_clients[r.id] = bft_metrics_client.MetricsClient(r)
        self.metrics = bft_metrics.BftMetrics(metric_clients)

    def random_client(self, without=None):
        if without == None:
            without = set()

        return random.choice(list(set(self.clients.values()) - without))

    def random_clients(self, max_clients):
        return set(random.choices(list(self.clients.values()), k=max_clients))

    def get_all_clients(self):
        return self.clients.values()

    def setup_txn_signing(self):
        self.txn_signing_keys_base_path = ""
        self.principals_mapping = ""
        self.principals_to_participant_map = {}
        if self.txn_signing_enabled:
            self.txn_signing_keys_base_path = tempfile.mkdtemp(prefix='txn_signing_keys')
            self.principals_mapping, self.principals_to_participant_map = self.create_principals_mapping()
            self.generate_txn_signing_keys(self.txn_signing_keys_base_path)

    def generate_txn_signing_keys(self, keys_path):
        """ Generates num_participants number of key pairs """
        script_path = "/concord-bft/scripts/linux/create_concord_clients_transaction_signing_keys.sh"
        args = [script_path, "-n", str(self.num_participants), "-o", keys_path]
        subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def create_principals_mapping(self):
        """
        If client principal ids range from 11-20, for example, this method splits them into groups based on
        NUM_PARTICIPANTS and self.num_participants.
        Client ids in each group will be space separated, and each group will be semicolon separated.
        E.g. "11 12;13 14;15 16;17 18;19 20" for 10 client ids divided into 5 participants.
        If there are reserved clients, they are added at the end of the group in round robin manner.
        Thus, if there are 2 reserved client, with ids 21 and 22, the final string would look like:
        "11 12 21;13 14 22;15 16;17 18,19 20".
        This method also returns a principals to participants map, with the key being the principal id
        of the client or reserved client, and the value being the participant it belongs to (numbered 1 onwards).
        If number of clients is not default, but modified from outside, there might be cases where self.num_participants
        will be less than NUM_PARTICIPANTS.
        """
        def split(a, n):
            """ Splits list 'a' into n chunks """
            k, m = divmod(len(a), n)
            return [a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

        start_id = self.config.n + self.config.num_ro_replicas
        client_ids = range(start_id, start_id + self.config.num_clients)
        start_id = self.num_total_replicas() + self.config.num_clients
        reserved_client_ids = range(start_id, start_id + RESERVED_CLIENTS_QUOTA + 1)

        principals = ""
        client_ids = sorted(client_ids)
        reserved_client_ids = sorted(reserved_client_ids)
        combined_clients = client_ids + reserved_client_ids
        combined_clients_set = set(combined_clients)
        assert len(combined_clients_set) == len(combined_clients), "Client Ids and Reserved Client Ids must all be unique ids"
        self.num_participants = min(NUM_PARTICIPANTS, len(client_ids))
        client_ids_chunks = split(client_ids, self.num_participants)
        reserved_client_ids_chunks = split(reserved_client_ids, self.num_participants)
        principals_to_participant_map = {}

        # iterate number of participants
        for i in range(self.num_participants):
            # add client_ids to principals
            for cid in client_ids_chunks[i]:
                principals = principals + str(cid) + " "
                principals_to_participant_map[cid] = i+1
            # add reserved_client_ids to principals
            for rcid in reserved_client_ids_chunks[i]:
                principals = principals + str(rcid) + " "
                principals_to_participant_map[rcid] = i+1
            # remove last space
            if principals[-1] == ' ':
                principals = principals[:-1]
            # add , to separate next set of client_ids
            principals = principals + ";"

        # remove last semicolon
        if principals[-1] == ';':
            principals = principals[:-1]
        return principals, principals_to_participant_map

    def start_replica_cmd(self, replica_id):
        """
        Returns a tuple:
            1) Command line to start replica with the given id
            2) The binary index in the command line of the replica binary. In debug tools run, it might be different than 0.
        If the callback accepts three parameters and one of them is named 'config' - pass the network configuration too.
        Append the SSL certificate path. This is needed only for TLS communication.
        """
        start_replica_fn_args = inspect.getfullargspec(self.config.start_replica_cmd).args
        if "config" in start_replica_fn_args and len(start_replica_fn_args) == 3:
            cmd = self.config.start_replica_cmd(self.builddir, replica_id, self.config)
        else:
            cmd = self.config.start_replica_cmd(self.builddir, replica_id)
        # Potentially, set debug tool name as 1st parameter (usually this will happen for external tools only)
        # if debug tool is set, return the actual replica binary path index in the command
        replica_binary_path_index = 0
        if self.debug_tool:
            replica_binary_path_index = self.debug_tool.set_tool_in_replica_command(cmd)
        if self.certdir:
            cmd.append("-c")
            cmd.append(self.certdir + "/" + str(replica_id))
        if self.txn_signing_enabled:
            cmd.append("-p")
            cmd.append(self.principals_mapping)
            keys_path = os.path.join(self.txn_signing_keys_base_path, "transaction_signing_keys")
            cmd.append("-t")
            cmd.append(keys_path)
        return cmd, cmd[replica_binary_path_index]

    def check_error_logs(self):
        """
        Checking FATAL logs in replica logs and writing in a file.
        If the file exists for any testcase, warning is displayed in CI
        """

        if not self.current_test_case_path:
            # Some tests to not start any replicas/cre, or with KEEP_APOLLO_LOGS=FALSE - in that case there is nothing to check
            log.log_message(message_type="skipping check_error_logs since self.current_test_case_path is None")
            return

        with log.start_action(action_type="replica_log_scanner") as action:
            cmd = ['grep', '-R', 'FATAL', '--exclude=ReplicaErrorLogs.txt']
            file_path =  os.path.join(self.current_test_case_path, "ReplicaErrorLogs.txt")

            with open(file_path, 'w+') as outfile:
                subprocess.run(cmd, stdout=outfile, cwd=self.current_test_case_path)

            if os.path.isfile(file_path):
                if  os.stat(file_path).st_size > 0:
                    action.log(message_type=f'Error in Replica logs')
                else:
                    os.remove(file_path)

    def stop_replica_cmd(self, replica_id):
        """
        Returns command line to stop a replica with the given id
        """
        with log.start_action(action_type="stop_replica_cmd", replica=replica_id):
            return self.config.stop_replica_cmd(replica_id)

    def start_all_replicas(self):
        with log.start_action(action_type="start_all_replicas"):
            all_replicas = self.all_replicas()
            for i in all_replicas:
                try:
                    self.start_replica(i)
                except AlreadyRunningError:
                    if not self.is_existing:
                        raise
            assert len(self.procs) == self.config.n

    def stop_all_replicas(self):
        """ Stop all running replicas"""
        [ self.stop_replica(i) for i in self.get_live_replicas() ]
        assert len(self.procs) == 0

    def start_replicas(self, replicas):
        """
        Start from list "replicas"
        """
        [ self.start_replica(r) for r in replicas ]

    def stop_replicas(self, replicas):
        """
        Start from list "replicas"
        """
        for r in replicas:
            self.stop_replica(r)

    @staticmethod
    def monitor_replica_subproc(subproc: subprocess.Popen, replica_id: int, stop_event: Event, stdout_file):
        while True:
            return_code = subproc.poll()

            if return_code == 0:
                break

            if return_code is not None:
                stop_event.set()
                error_msg = f"ERROR - The subprocess of replica {replica_id} with pid {subproc.pid} has failed, " \
                            f"return code = {return_code}"
                log_message(message_type=f"{error_msg}, aborting test", replica_log=stdout_file.name)
                stdout_file.write(f"####### FATAL ERROR: The process has crashed, subproc return value: {return_code}\n")
                print(error_msg, file=sys.stderr)
                os.kill(os.getpid(), signal.SIGINT)
                break

            if stop_event.isSet():
                break

            sleep(0.2)

    def get_replicas(self, replica_ids):
        return [replica for replica in self.replicas if replica.id in replica_ids]

    def start_replica(self, replica_id):
        """
        Start a replica if it isn't already started.
        Otherwise raise an AlreadyStoppedError.
        """
        stdout_file = None
        stderr_file = None
        keep_logs = os.environ.get('KEEP_APOLLO_LOGS', "").lower() in ["true", "on"]

        if keep_logs:
            suite_name = self._suite_name
            replica_test_log_path = self.current_test_case_path / f"stdout_{replica_id}.log"

            Path(self.current_test_case_path).mkdir(parents=True, exist_ok=True)

            stdout_file = open(replica_test_log_path, 'a+')
            stderr_file = open(replica_test_log_path, 'a+')

            stdout_file.write("############################################\n")
            stdout_file.flush()
            stderr_file.write("############################################\n")
            stderr_file.flush()

            self.open_fds[replica_id] = (stdout_file, stderr_file)

        """
        If the apollo test's CODECOVERAGE flag is set, all raw profile files will be created
        in coverage dir; otherwise, ignore it.
        """
        if os.environ.get('CODECOVERAGE', "").lower() in ["true", "on"] and \
                os.environ.get('GRACEFUL_SHUTDOWN',"").lower() in ["true", "on"]:
            self.coverage_dir = f"{self.builddir}/tests/apollo/codecoverage/{suite_name}/{self.current_test}/"
            Path(self.coverage_dir).mkdir(parents=True, exist_ok=True)
            profraw_file_name = f"coverage_{replica_id}_%9m.profraw"
            profraw_file_path = os.path.join(
                self.coverage_dir, profraw_file_name)
            os.environ['LLVM_PROFILE_FILE'] = profraw_file_path

        if replica_id in self.procs:
            raise AlreadyRunningError(replica_id)

        is_external = self.is_existing and self.config.stop_replica_cmd is not None
        start_cmd, replica_binary_path = self.start_replica_cmd(replica_id)
        digest = self.binary_digest(replica_binary_path) if Path(replica_binary_path).exists() else 'Unknown'
        with log.start_action(action_type="start_replica_process", replica=replica_id, is_external=is_external,
                              binary_path=replica_binary_path, binary_digest=digest, cmd=' '.join(start_cmd),
                              with_monitor=keep_logs):
            my_env = os.environ.copy()
            my_env["RID"] = str(replica_id)
            if is_external:
                proc = subprocess.run(
                    start_cmd,
                    check=True,
                    env=my_env
                )
            else:
                proc = subprocess.Popen(
                    start_cmd,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    close_fds=True,
                    env=my_env
                )

            # If we run with some debug tool, let the module process the triggered proc process, and set the process info
            # according to the returned value. Some debug tools spawn multiple processes.
            if self.debug_tool and self.debug_tool.name:
                self.procs[replica_id] = self.debug_tool.process_pids_after_replica_started(proc, replica_id)
            else:
                self.procs[replica_id] = proc

                if keep_logs:
                    for _, __, other_replica_stdout_file in self.subproc_monitors.values():
                        if not other_replica_stdout_file.closed:
                            other_replica_stdout_file.write(
                                f"################### Apollo starting replica {replica_id}\n")
                            other_replica_stdout_file.flush()
                    stop_event = Event()
                    thread = Thread(target=BftTestNetwork.monitor_replica_subproc,
                                    args=(self.procs[replica_id], replica_id, stop_event, stdout_file))
                    self.subproc_monitors[replica_id] = (thread, stop_event, stdout_file)
                    thread.start()

                    self.verify_matching_replica_client_communication(replica_test_log_path)

        replica_for_perf = os.environ.get('PERF_REPLICA', None)
        if self.test_start_time and replica_for_perf and int(replica_for_perf) == replica_id:
            log.log_message(message_type=f"Profiling replica {replica_id} using perf")
            perf_samples = os.environ.get('PERF_SAMPLES', "1000")
            perf_cmd = ["perf", "record", "-F", perf_samples, "-p", f"{self.procs[replica_id].pid}",
                        "-a", "-g", "-o", f"{self.current_test_case_path}perf.data"]
            self.perf_proc = subprocess.Popen(perf_cmd, close_fds=True)

    @staticmethod
    def verify_matching_replica_client_communication(replica_test_log_path: str, timeout_seconds: float = 5.,
                                                     sleep_seconds=.1):
        """
        Parses the communication protocol of a replica from its log
        and matches it to the python client communication protocol
        @param replica_test_log_path: The path to the replica log
        @param timeout_seconds: The timeout for waiting for the log message to be written
        @param sleep_seconds: The time to sleep between subsequent file reads
        """
        max_read_bytes = 4096
        chunk_bytes = 1024
        communication_str = 'Replica communication protocol='
        replica_to_client_communication = {
            'TlsTcp': bft_client.TcpTlsClient,
            'PlainUdp': bft_client.UdpClient
        }

        log_data = ""
        with open(replica_test_log_path, 'r') as replica_log:
            time_elapsed = 0
            while communication_str not in log_data and \
                    time_elapsed < timeout_seconds and \
                    len(log_data) < max_read_bytes:
                log_data += replica_log.read(chunk_bytes)
                time.sleep(sleep_seconds)
                time_elapsed += sleep_seconds

            # make sure we have read the whole communication phrase
            if "," not in log_data[log_data.find(communication_str) + len(communication_str):]:
                log_data += replica_log.read(chunk_bytes)

        assert communication_str in log_data, \
            f"Communication str not in {max_read_bytes + chunk_bytes} first bytes of the log, " \
            f"time elapsed: {time_elapsed}, read bytes: {len(log_data)}"

        actual_replica_communication = \
            log_data[log_data.find(communication_str) + len(communication_str):].strip().split(",")[0]

        correct_client = replica_to_client_communication[actual_replica_communication]
        assert BFT_CLIENT_TYPE is correct_client, \
            f"Replica communication protocol is {actual_replica_communication} " \
            f"but client is {BFT_CLIENT_TYPE}"

    def replica_id_from_pid(self, pid):
        """ Return an already-started replica id, according to a given pid """
        for rep_id in self.procs.keys():
            if self.procs[rep_id].pid == pid:
                return rep_id
        return None

    def is_replica_id(self, node_id):
        """ Return True if a node_id is a replica id (includes a read-only replica) """
        return node_id < self.num_total_replicas()

    def is_client_id(self, node_id):
        """ Return True if node_id is a client Id - reserved or non-reserved """
        start_id = self.num_total_replicas()
        return start_id <= node_id < (start_id + self.config.num_clients + RESERVED_CLIENTS_QUOTA)

    def is_read_only_replica_id(self, node_id):
        """ Return True if node_id is a read-only replica id """
        return self.config.n <= node_id < (self.config.n + self.config.num_ro_replicas)

    def is_reserved_client_id(self, node_id):
        """ Return True is node_id is a reserved-client Id """
        start_id = self.num_total_replicas() + self.config.num_clients
        return start_id <= node_id < (start_id + RESERVED_CLIENTS_QUOTA)

    def comm_type(self):
        """
        Returns a string representing the communication type.
        Raise NotImplementedError if communication type is not implemented
        """
        if bft_client.TcpTlsClient == BFT_CLIENT_TYPE:
            return bft_config.COMM_TYPE_TCP_TLS
        if bft_client.UdpClient == BFT_CLIENT_TYPE:
            return bft_config.COMM_TYPE_UDP
        raise NotImplementedError(f"{type(self.clients[self.config.n])} is not supported!")

    def num_total_replicas(self):
        return self.config.n + self.config.num_ro_replicas

    def num_total_clients(self):
        return self.config.num_clients + RESERVED_CLIENTS_QUOTA

    def node_id_from_bft_msg_port(self, bft_msg_port):
        assert ((bft_msg_port % 2 == 0) and
                (bft_msg_port < bft_config.START_METRICS_PORT) and
                (bft_msg_port >= bft_config.START_BFT_MSG_PORT))
        return (bft_msg_port - bft_config.START_BFT_MSG_PORT) // 2

    def stop_replica(self, replica_id, force_kill=False):
        """
        Stop a replica if it is running.
        Otherwise raise an AlreadyStoppedError.
        """
        if replica_id in self.subproc_monitors:
            thread, stop_event, stdout_file = self.subproc_monitors[replica_id]
            stop_event.set()
            thread.join(timeout=2)
            for other_id, other_tuple in self.subproc_monitors.items():
                other_file = other_tuple[2]
                if not other_file.closed:
                    other_file.write(f"################### Apollo stopping replica {replica_id}\n")
                    other_file.flush()


        with log.start_action(action_type="stop_replica", replica=replica_id):
            if replica_id not in self.procs.keys():
                raise AlreadyStoppedError(replica_id)

            if self.is_existing and self.config.stop_replica_cmd is not None:
                self._stop_external_replica(replica_id)
            else:
                proc = self.procs[replica_id]
                if force_kill:
                    proc.kill()
                else:
                    if os.environ.get('GRACEFUL_SHUTDOWN', "").lower() in set(["true", "on"]):
                        proc.terminate()
                    else:
                        proc.kill()
                for fd in self.open_fds.get(replica_id, ()):
                    fd.close()
                proc.wait(timeout=30)
                if self.debug_tool:
                    self.debug_tool.process_output(replica_id, proc, self.testdir)
            del self.procs[replica_id]

    def _stop_external_replica(self, replica_id):
        with log.start_action(action_type="_stop_external_replica", replica=replica_id):
            subprocess.run(
                self.stop_replica_cmd(replica_id),
                check=True
            )

    def all_replicas(self, without=None):
        """
        Returns a list of all ACTIVE replica IDs excluding the "without" set, and without RO replicas
        """
        if without == None:
            without = set()
        return list(set(range(0, self.config.n)) - without)

    def all_client_ids(self, without=None, with_reserved_clients=True):
        """
        Returns a list of all client IDs, excluding the "without" set
        """
        if without == None:
            without = set()
        num_total_clients = self.config.num_clients
        num_total_replicas = self.num_total_replicas()
        if with_reserved_clients:
            num_total_clients += RESERVED_CLIENTS_QUOTA
        return list(set(range(num_total_replicas, num_total_replicas + num_total_clients)) - without)

    def random_set_of_replicas(self, size, without=None):
        """ Returns a random list of ACTIVE replica IDs excluding the "without" set, and without any RO replicas """
        if without is None:
            without = set()
        random_replicas = set()
        for _ in range(size):
            exclude_replicas = random_replicas | without
            random_replicas.add(random.choice(self.all_replicas(without=exclude_replicas)))
        return random_replicas

    def get_live_replicas(self):
        """
        Returns the id-s of all live replicas
        """
        return list(self.procs.keys())

    def get_client(self, id):
        if self.is_client_id(id):
            return self.reserved_clients[id] if self.is_reserved_client_id(id) else self.clients[id]
        return None

    async def get_current_primary(self):
        """
        Returns the current primary replica id
        """
        with log.start_action(action_type="get_current_primary") as action:
            current_primary = await self.get_current_view() % self.config.n
            action.add_success_fields(current_primary=current_primary)
            return current_primary

    async def get_current_view(self):
        """
        Returns the current view number
        """
        with log.start_action(action_type="get_current_view") as action:
            matching_view = None
            nb_replicas_in_matching_view = 0

            async def get_view(replica_id):
                def expected(_): return True
                replica_view = await self._wait_for_matching_agreed_view(replica_id, expected)
                replica_views.append(replica_view)

            try:
                with trio.fail_after(seconds=30):
                    while True:
                        replica_views = []
                        async with trio.open_nursery() as nursery:
                            for r in self.get_live_replicas():
                                nursery.start_soon(get_view, r)
                        view = Counter(replica_views).most_common(1)[0]
                        # wait for n-f = 2f+2c+1 replicas to have agreed the expected view
                        if view[1] >= 2 * self.config.f + 2 * self.config.c + 1:
                            matching_view = view[0]
                            nb_replicas_in_matching_view = view[1]
                            break
                        await trio.sleep(0.1)

                    action.log(
                        message_type=f'Matching view #{matching_view} has been agreed among replicas.')

                    action.log(f'View #{matching_view} is active on '
                            f'{nb_replicas_in_matching_view} replicas '
                            f'({nb_replicas_in_matching_view} >= n-f = {self.config.n - self.config.f}).')
                    action.add_success_fields(current_view=matching_view)

                    return matching_view

            except trio.TooSlowError:
                assert False, "Could not agree view among replicas."

    async def get_metric(self, replica_id, bft_network, mtype, mname, component='replica'):
        with trio.fail_after(seconds=30):
            while True:
                with trio.move_on_after(seconds=1):
                    try:
                        key = [component, mtype, mname]
                        value = await bft_network.metrics.get(replica_id, *key)
                    except KeyError:
                        # metrics not yet available, continue looping
                        log.log_message(message_type=f"KeyError! '{mname}' not yet available.")
                        await trio.sleep(0.1)
                    else:
                        return value

    async def wait_for_view_with_threshold(self, view_number: int, threshold: Optional[int] = None,
                                           timeout_seconds: float = 45, sleep_seconds: float = .25, error_msg=None):
        """
        Waits for at least threshold replicas to reach view view_number
        :param view_number: The view number to reach
        :param threshold: The minimal amount of replicas which are expected to reach view view_number,
                          Defaults to 2f + 2c + 1
        :param timeout_seconds: Maximal amount of seconds to wait
        :param sleep_seconds: Time to sleep between samples, in seconds
        :param error_msg: Error to display in case of timeout
        :return: The number of replicas who reached view view_number at the time of the last sample
        """
        threshold = threshold if threshold else 2 * self.config.f + 2 * self.config.c + 1
        replica_in_view_count = 0
        try:
            with trio.fail_after(timeout_seconds):
                while replica_in_view_count < threshold:
                    replica_in_view_count = await self.count_replicas_in_view(view_number)
                    await trio.sleep(sleep_seconds)
        except trio.TooSlowError as e:
            msg = error_msg if error_msg else \
                f"Only {replica_in_view_count} out of {threshold} replicas reached view number {view_number}"
            raise trio.TooSlowError(msg) from e
        return replica_in_view_count

    async def wait_for_view(self, replica_id, expected=None,
                            err_msg="Expected view not reached"):
        """
        Waits for a view that matches the "expected" predicate,
        and returns the corresponding view number.

        If the "expected" predicate is not provided,
        returns the current view number.

        In case of a timeout, fails with the provided err_msg
        """
        with log.start_action(action_type="wait_for_view", replica=replica_id) as action:
            if expected is None:
                expected = lambda _: True

            matching_view = None
            nb_replicas_in_matching_view = 0
            try:
                matching_view = await self._wait_for_matching_agreed_view(replica_id, expected)
                action.log(message_type=f'Matching view #{matching_view} has been agreed among replicas.')

                nb_replicas_in_matching_view = \
                    await self.wait_for_view_with_threshold(view_number=matching_view,
                                                            threshold=2 * self.config.f + 2 * self.config.c + 1)
                action.log(f'View #{matching_view} is active on '
                      f'{nb_replicas_in_matching_view} replicas '
                      f'({nb_replicas_in_matching_view} >= n-f = {self.config.n - self.config.f}).')

                return matching_view
            except trio.TooSlowError:
                assert False, err_msg + \
                              f'(matchingView={matching_view} ' \
                              f'replicasInMatchingView={nb_replicas_in_matching_view})'

    async def _wait_for_matching_agreed_view(self, replica_id, expected):
        """
        Wait for the last agreed view to match the "expected" predicate
        """
        with log.start_action(action_type="_wait_for_matching_agreed_view", replica=replica_id) as action:
            last_agreed_view = None

            async def the_last_agreed_view():
                key = ['replica', 'Gauges', 'lastAgreedView']
                view = await self.retrieve_metric(replica_id, *key)

                if view is not None and expected(view):
                    return view

            last_agreed_view = await self.wait_for(the_last_agreed_view, 45, 1)
            action.add_success_fields(last_agreed_view=last_agreed_view)
            return last_agreed_view

    async def count_replicas_in_view(self, view):
        """
        Count the number of replicas that have activated a given view
        """
        with log.start_action(action_type="count_replicas_in_view", view=view):
            nb_replicas_in_view = 0

            async def _count_if_replica_in_view(r, expected_view):
                """
                A closure that allows concurrent counting of replicas
                that have activated a given view.
                """
                with log.start_action(action_type="count_if_replica_in_view", replica=r):
                    nonlocal nb_replicas_in_view

                    key = ['replica', 'Gauges', 'currentActiveView']

                    with trio.move_on_after(seconds=5):
                        while True:
                            with trio.move_on_after(seconds=1):
                                try:
                                    replica_view = await self.metrics.get(r, *key)
                                    if replica_view == expected_view:
                                        nb_replicas_in_view += 1
                                except KeyError:
                                    # metrics not yet available, continue looping
                                    await trio.sleep(0.25)
                                    continue
                                else:
                                    break

        async with trio.open_nursery() as nursery:
            for r in self.get_live_replicas():
                nursery.start_soon(
                    _count_if_replica_in_view, r, view)
        return nb_replicas_in_view

    async def force_quorum_including_replica(self, replica_id):
        """
        Bring down a sufficient number of replicas (excluding the primary),
        so that the remaining replicas form a quorum that includes replica_id
        """
        with log.start_action(action_type="force_quorum_including_replica", replica=replica_id) as action:
            assert len(self.procs) >= 2 * self.config.f + self.config.c + 1
            primary = await self.get_current_primary()
            self.stop_replicas(self.random_set_of_replicas(
                len(self.procs) - (2 * self.config.f + self.config.c + 1), without={primary, replica_id}))

    async def wait_for_fetching_state(self, replica_id):
        """
        Check metrics on fetching replica to see if the replica is in a
        fetching state

        Returns the current source replica for state transfer.
        """
        with log.start_action(action_type="wait_for_fetching_state", replica=replica_id) as action:
            async def replica_to_be_in_fetching_state():
                has_metric = False
                while not has_metric:
                    try:
                        is_fetching = await self.is_fetching(replica_id)
                        has_metric = True
                    except KeyError:
                        # If a replica was down, the metric server might be up prior to the replica
                        # registering its state transfer related metrics
                        pass
                source_replica_id = await self.source_replica(replica_id)
                if is_fetching:
                    action.add_success_fields(source_replica_id=source_replica_id)
                    return source_replica_id

            return await self.wait_for(replica_to_be_in_fetching_state, 10, .5)

    async def is_fetching(self, replica_id):
        """Return whether the current replica is fetching state"""
        key = ['bc_state_transfer', 'Statuses', 'fetching_state']
        state = await self.metrics.get(replica_id, *key)
        return state != "NotFetching"

    async def source_replica(self, replica_id):
        """Return whether the current replica has a source replica already set"""
        with log.start_action(action_type="source_replica", replica=replica_id):
            key = ['state_transfer_source_selector', 'Gauges', 'current_source_replica']
            source_replica_id = await self.metrics.get(replica_id, *key)
            return source_replica_id

    async def wait_for_state_transfer_to_start(self, replica_ids: Optional[Sequence[int]]=None):
        """
        Retry checking every .5 seconds until state transfer starts at least one
        node. Stop trying, and fail the test after 30 seconds.
        """
        replicas = self.replicas if not replica_ids else filter(lambda rep: rep.id in replica_ids, self.replicas)
        with log.start_action(action_type="wait_for_state_transfer_to_start", replica_ids=[rep.id for rep in replicas]):
            with trio.fail_after(30): # seconds
                async with trio.open_nursery() as nursery:
                    for replica in replicas:
                        nursery.start_soon(self._wait_to_receive_st_msgs,
                                           replica,
                                           nursery.cancel_scope)

    async def wait_for_replicas_to_collect_stable_checkpoint(self, replicas, checkpoint, timeout=30):
        with log.start_action(action_type="wait_for_replicas_to_collect_stable_checkpoint", replicas=replicas,
                              checkpoint=checkpoint) as action:
            with trio.fail_after(seconds=timeout):
                last_stable_seqs = []
                while True:
                    for replica_id in replicas:
                        last_stable = await self.get_metric(replica_id, self, 'Gauges', "lastStableSeqNum")
                        last_stable_seqs.append(last_stable)
                        action.log(message_type="lastStableSeqNum", replica=replica_id, last_stable=last_stable)
                    assert checkpoint >= last_stable / 150, "Probably got wrong checkpoint as input"
                    if sum(x >= 150 * checkpoint for x in last_stable_seqs) == len(replicas):
                        break
                    else:
                        last_stable_seqs.clear()
                        await trio.sleep(seconds=0.1)

    async def _wait_to_receive_st_msgs(self, replica, cancel_scope):
        """
        Check metrics to see if state transfer started. If so cancel the
        concurrent coroutines in the request scope.
        """
        with log.start_action(action_type="_wait_to_receive_st_msgs", replica=replica.id) as action:
            while True:
                with trio.move_on_after(.5): # seconds
                    try:
                        key = ['replica', 'Counters', 'receivedStateTransferMsgs']
                        n = await self.metrics.get(replica.id, *key)
                        if n > 0:
                            action.log(message_type="State transfer has started. Cancelling concurrent coroutines", receivedStateTransferMsgs=n)
                            cancel_scope.cancel()
                    except KeyError:
                        pass # metrics not yet available, continue looping
                    await trio.sleep(0.1)

    async def wait_for_state_transfer_to_stop(self, up_to_date_node: int, stale_node: int, stop_on_stable_seq_num=False, seconds_until_timeout=45 if os.getenv('BUILD_COMM_TCP_TLS') == "OFF" else 30):
        with log.start_action(action_type="wait_for_state_transfer_to_stop", up_to_date_node=up_to_date_node, stale_node=stale_node, stop_on_stable_seq_num=stop_on_stable_seq_num, seconds_until_timeout=seconds_until_timeout):
            with trio.fail_after(seconds_until_timeout):
                # Get the lastExecutedSeqNumber from a started node
                if stop_on_stable_seq_num:
                    key = ['replica', 'Gauges', 'lastStableSeqNum']
                else:
                    key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                expected_seq_num = await self.metrics.get(up_to_date_node, *key)
                with log.start_action(action_type='start_polling', key=key[2], expected_seq_num=expected_seq_num) as action:
                    last_n = -1
                    while True:
                        with trio.move_on_after(.5): # seconds
                            metrics = await self.metrics.get_all(stale_node)
                            try:
                                stale_node_metric = self.metrics.get_local(metrics, *key)
                            except KeyError:
                                # ignore - the metric will eventually become available
                                await trio.sleep(0.1)
                            else:
                                # Debugging
                                if stale_node_metric != last_n:
                                    last_n = stale_node_metric
                                    last_stored_checkpoint = self.metrics.get_local(metrics,
                                        'bc_state_transfer', 'Gauges', 'last_stored_checkpoint')
                                    on_transferring_complete = self.metrics.get_local(metrics,
                                        'bc_state_transfer', 'Counters', 'on_transferring_complete')
                                    action.log(message_type="Not complete yet",
                                               seq_num=stale_node_metric, expected_seq_num=expected_seq_num,
                                               last_stored_checkpoint=last_stored_checkpoint,
                                               on_transferring_complete=on_transferring_complete)

                                # Exit condition - make sure that same checkpoint as live replica is reached
                                if (stale_node_metric // CHECKPOINT_SEQUENCES) == (expected_seq_num // CHECKPOINT_SEQUENCES):
                                    action.add_success_fields(n=stale_node_metric, expected_seq_num=expected_seq_num)
                                    return

                                await trio.sleep(0.5)

    async def wait_for_state_transfer_to_stop_with_RFMD(self, up_to_date_node, stale_node, stop_on_stable_seq_num=False):
        with log.start_action(action_type="wait_for_state_transfer_to_stop_with_RFMD", up_to_date_node=up_to_date_node, stale_node=stale_node, stop_on_stable_seq_num=stop_on_stable_seq_num):
            with trio.fail_after(30): # seconds
                # Get the lastExecutedSeqNumber from a started node
                if stop_on_stable_seq_num:
                    key = ['replica', 'Gauges', 'lastStableSeqNum']
                else:
                    key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                expected_seq_num = await self.metrics.get(up_to_date_node, *key)
                with log.start_action(action_type='start_polling', key=key[2], expected_seq_num=expected_seq_num) as action:
                    last_n = -1
                    while True:
                        with trio.move_on_after(.5): # seconds
                            metrics = await self.metrics.get_all(stale_node)
                            try:
                                n = self.metrics.get_local(metrics, *key)
                                # If seq_num is not moving send the new message to advance it
                                if (n == last_n):
                                    skvbc = kvbc.SimpleKVBCProtocol(self)
                                    client = self.random_client()
                                    # Write a KV pair with a known value
                                    known_key = skvbc.unique_random_key()
                                    known_val = skvbc.random_value()
                                    known_kv = [(known_key, known_val)]
                                    await skvbc.send_write_kv_set(client, known_kv)
                            except KeyError:
                                # ignore - the metric will eventually become available
                                await trio.sleep(0.1)
                            else:
                                # Debugging
                                if n != last_n:
                                    last_n = n
                                    last_stored_checkpoint = self.metrics.get_local(metrics,
                                        'bc_state_transfer', 'Gauges', 'last_stored_checkpoint')
                                    on_transferring_complete = self.metrics.get_local(metrics,
                                        'bc_state_transfer', 'Counters', 'on_transferring_complete')
                                    action.log(message_type="Not complete yet",
                                        seq_num=n, last_stored_checkpoint=last_stored_checkpoint, on_transferring_complete=on_transferring_complete)

                                # Exit condition
                                if n >= expected_seq_num:
                                    action.add_success_fields(n=n, expected_seq_num=expected_seq_num)
                                    return

    async def wait_for_replicas_rvt_root_values_to_be_in_sync(self, replica_ids, timeout=30):
        """
        Wait for the root values of the Range validation trees of all replicas to be in sync within `timeout` seconds.

        Wait for each replica in `replica_ids` to return the current value of the root of its Range validation tree.
        When all of the values are collected, compare them to check if they are all the same.
        If there are discrepancies, sleep for 1 second and try retrieving the values again.
        """
        with log.start_action(action_type="wait_for_replicas_rvt_root_values", replica_ids=replica_ids, timeout=timeout):
            root_values = [None] * len(replica_ids)

            with trio.fail_after(timeout): # seconds
                while True:
                    async with trio.open_nursery() as nursery:
                        for replica_id in replica_ids:
                            nursery.start_soon(self.wait_for_rvt_root_value, replica_id, root_values, timeout)

                    print(root_values)
                    # At this point all replicas' root values are collected
                    if root_values.count(root_values[0]) == len(root_values) and len(root_values[0]) > 0:
                        break
                    else:
                        await trio.sleep(1)

    async def wait_for_rvt_root_value(self, replica_id, root_values, timeout=30):
        """
        Wait for a single replica to return the current value of the root of its Range validation tree.
        Check every .5 seconds and fail after `timeout` seconds.
        """
        with log.start_action(action_type="wait_for_rvt_root_value", replica=replica_id, timeout=timeout) as action:
            async def rvt_root_value_to_be_returned():
                key = ['bc_state_transfer', 'Statuses', 'current_rvb_data_state']
                rvb_data_state = await self.retrieve_metric(replica_id, *key)
                if (rvb_data_state is not None):
                    action.log(f"Replica {replica_id}'s current rvb_data_state is: \"{rvb_data_state}\".")
                    root_values[replica_id] = rvb_data_state
                    return rvb_data_state

        return await self.wait_for(rvt_root_value_to_be_returned, timeout, .5)

    async def wait_for_replicas_to_checkpoint(self, replica_ids: Sequence[int],
                                              expected_checkpoint_num: Callable[[int], bool] = lambda _: True,
                                              timeout=30):
        """
        Wait for every replica in `replicas` to take a checkpoint.
        Check every .5 seconds and give fail after 30 seconds.
        """
        with log.start_action(action_type="wait_for_replicas_to_checkpoint", replica_ids=replica_ids):
            with trio.fail_after(timeout): # seconds
                async with trio.open_nursery() as nursery:
                    for replica_id in replica_ids:
                        nursery.start_soon(self.wait_for_checkpoint, replica_id, expected_checkpoint_num)

    async def wait_for_checkpoint(self, replica_id: int,
                                  expected_checkpoint_num: Callable[[int], bool] = lambda _: True):
        """
        Wait for a single replica to reach the expected_checkpoint_num.
        If none is provided, return the last stored checkpoint.
        """
        with log.start_action(action_type="wait_for_checkpoint", replica=replica_id) as action:
            async def expected_checkpoint_to_be_reached():
                key = ['bc_state_transfer', 'Gauges', 'last_stored_checkpoint']
                last_stored_checkpoint = await self.retrieve_metric(replica_id, *key)
                if last_stored_checkpoint is not None and expected_checkpoint_num(last_stored_checkpoint):
                    action.log(message_type=f'[checkpoint] #{last_stored_checkpoint} reached by replica=#{replica_id}')
                    action.add_success_fields(last_stored_checkpoint=last_stored_checkpoint)
                    return last_stored_checkpoint

        return await self.wait_for(expected_checkpoint_to_be_reached, 30, .5)

    async def wait_for_fast_path_to_be_prevalent(self, run_ops, threshold, replica_id=0, timeout=90):
        await self._wait_for_consensus_path_to_be_prevalent(
            fast=True, run_ops=run_ops, threshold=threshold, replica_id=replica_id)

    async def wait_for_slow_path_to_be_prevalent(self, run_ops, threshold, replica_id=0, timeout=90):
        await self._wait_for_consensus_path_to_be_prevalent(
            fast=False, run_ops=run_ops, threshold=threshold, replica_id=replica_id, timeout=timeout)

    async def _wait_for_consensus_path_to_be_prevalent(self, fast, run_ops, threshold, replica_id=0, timeout=90):
        """
        Waits until at least threshold operations are being executed in the selected path.
          run_ops: lambda that executes skvbc.run_concurrent_ops or creates requests in some other way
          threshold: minimum number of requests that have to be executed in the correct path
        run_ops should produce at least threshold executions
        """
        with log.start_action(action_type="wait_for_%s_path_to_be_prevalent" % ("fast" if fast else "slow")):
            with trio.fail_after(seconds=timeout):
                done = False
                while not done:
                    #initial state to ensure requests are sent
                    res = ConsensusPathPrevalentResult.TOO_MANY_REQUESTS_ON_UNEXPECTED_PATH
                    nb_fast_paths_to_ignore = 0
                    nb_slow_paths_to_ignore = 0
                    with trio.move_on_after(seconds=15):  #retry timeout for not enough right path requests
                        while not done:
                            if res == ConsensusPathPrevalentResult.TOO_MANY_REQUESTS_ON_UNEXPECTED_PATH:
                                # we need to reset the counters
                                log.log_message("run_ops")
                                nb_fast_paths_to_ignore = await self.num_of_fast_path_requests(replica_id)
                                nb_slow_paths_to_ignore = await self.num_of_slow_path_requests(replica_id)
                                if run_ops:
                                    await run_ops()
                            res = await self._consensus_path_prevalent(
                                    fast,
                                    nb_fast_paths_to_ignore + ((threshold - 1) if fast else 0),
                                    nb_slow_paths_to_ignore + ((threshold - 1) if not fast else 0),
                                    replica_id)
                            if res == ConsensusPathPrevalentResult.OK:
                                # the selected path is prevalent - done.
                                log.log_message("done")
                                done = True
                                break
                            if res == ConsensusPathPrevalentResult.TOO_FEW_REQUESTS_ON_EXPECTED_PATH:
                                # continue polling for enough right path requests
                                await trio.sleep(seconds=.5)
                                continue
                            if res == ConsensusPathPrevalentResult.TOO_MANY_REQUESTS_ON_UNEXPECTED_PATH:
                                # wait a bit before resending new requests
                                await trio.sleep(seconds=5)


    async def wait_for_consensus_path(self, path_type: ConsensusPathType,
                                      run_ops: Callable[[], Coroutine],
                                      threshold: int,
                                      replica_id: int = 0,
                                      sleep_seconds: int = 1,
                                      timeout_seconds: int = 60):
        """
        Polls until either threshold operations are being executed in the selected path or the operation times out.
        path_type: The consensus path type to wait for
        run_ops: A coroutine which sends requests to the network
        threshold: The number of messages to be committed in path_type
        sleep_seconds: Time to sleep between polls in seconds
        timeout_seconds: Timeout for waiting in seconds
        """
        initial_count = await self.commit_count_by_path(path_type, replica_id)
        prev_diff = threshold

        await run_ops()

        with log.start_action(action_type=f"wait_for_requests_in_path",
                              path_type=path_type.value, request_count=threshold,
                              initial_count=initial_count) as action, \
                trio.fail_after(seconds=timeout_seconds):
            while True:
                await trio.sleep(seconds=sleep_seconds)
                current_count = await self.commit_count_by_path(path_type, replica_id)
                assert initial_count <= current_count, f"Inconsistent metrics values. initial value: {initial_count}," \
                                                       f"current value: {current_count}"
                counter_diff = current_count - initial_count
                if counter_diff >= threshold:
                    break
                if counter_diff < prev_diff:
                    action.log(message_type=f"info", title='consensus_progress',
                               path_type=path_type.value, remaining=counter_diff)
                prev_diff = counter_diff

    async def wait_for_last_executed_seq_num(self, replica_id=0, expected=0):
        with log.start_action(action_type="wait_for_last_executed_seq_num"):
            async def expected_seq_num_to_be_reached():
                key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                last_executed_seq_num = await self.retrieve_metric(replica_id, *key)

                if last_executed_seq_num is not None and last_executed_seq_num >= expected:
                    return last_executed_seq_num

            return await self.wait_for(expected_seq_num_to_be_reached, 30, .5)

    async def assert_state_transfer_not_started_all_up_nodes(self, up_replica_ids):
        with log.start_action(action_type="assert_state_transfer_not_started_all_up_nodes"):
            with trio.fail_after(METRICS_TIMEOUT_SEC):
                # Check metrics for all started nodes in parallel
                async with trio.open_nursery() as nursery:
                    up_replicas = [self.replicas[i] for i in up_replica_ids]
                    for r in up_replicas:
                        nursery.start_soon(self._assert_state_transfer_not_started,
                                           r)

    async def assert_fast_path_prevalent(self, nb_fast_paths_to_ignore=0, nb_slow_paths_to_ignore=0, replica_id=0):
        res = await self._consensus_path_prevalent(
            fast=True,
            nb_fast_paths_to_ignore=nb_fast_paths_to_ignore,
            nb_slow_paths_to_ignore=nb_slow_paths_to_ignore,
            replica_id=replica_id)
        assert res == ConsensusPathPrevalentResult.OK, \
            f"Fast path is not prevalent in replica {replica_id}: \n" \
            f"nb_fast_paths_to_ignore: {nb_fast_paths_to_ignore}, nb_slow_paths_to_ignore: {nb_slow_paths_to_ignore}\n"

    async def assert_slow_path_prevalent(self, nb_fast_paths_to_ignore=0, nb_slow_paths_to_ignore=0, replica_id=0):
        res = await self._consensus_path_prevalent(
            fast=False,
            nb_fast_paths_to_ignore=nb_fast_paths_to_ignore,
            nb_slow_paths_to_ignore=nb_slow_paths_to_ignore,
            replica_id=replica_id)
        assert res == ConsensusPathPrevalentResult.OK, \
            f"Slow path is not prevalent in replica {replica_id}: \n" \
            f"nb_fast_paths_to_ignore: {nb_fast_paths_to_ignore}, nb_slow_paths_to_ignore: {nb_slow_paths_to_ignore}\n"

    async def _consensus_path_prevalent(
            self, fast, nb_fast_paths_to_ignore=0, nb_slow_paths_to_ignore=0, replica_id=0):
        """
        Asserts all executed requests after the ignored number have been processed on the fast or slow path
        depending on the fast parameter value being True/False
        """
        with log.start_action(action_type="assert_%s_path_prevalent" % ("fast" if fast else "slow")):
            total_nb_fast_paths = await self.num_of_fast_path_requests(replica_id)
            total_nb_slow_paths = await self.num_of_slow_path_requests(replica_id)

            log.log_message("assert on %s path | slow %d/%d fast %d/%d" % ("fast" if fast else "slow",
                total_nb_slow_paths, nb_slow_paths_to_ignore, total_nb_fast_paths, nb_fast_paths_to_ignore))

            if fast:
                if total_nb_slow_paths > nb_slow_paths_to_ignore:
                    return ConsensusPathPrevalentResult.TOO_MANY_REQUESTS_ON_UNEXPECTED_PATH  # we can't have any slow
                if total_nb_fast_paths <= nb_fast_paths_to_ignore:
                    return ConsensusPathPrevalentResult.TOO_FEW_REQUESTS_ON_EXPECTED_PATH  # we don't have enough fast

                return ConsensusPathPrevalentResult.OK
            else:
                if total_nb_fast_paths > nb_fast_paths_to_ignore:
                    return ConsensusPathPrevalentResult.TOO_MANY_REQUESTS_ON_UNEXPECTED_PATH  # we can't have any fast
                if total_nb_slow_paths <= nb_slow_paths_to_ignore:
                    return ConsensusPathPrevalentResult.TOO_FEW_REQUESTS_ON_EXPECTED_PATH  # we don't have enough slow

                return ConsensusPathPrevalentResult.OK

    async def _assert_state_transfer_not_started(self, replica):
        key = ['replica', 'Counters', 'receivedStateTransferMsgs']
        n = await self.metrics.get(replica.id, *key)
        assert n == 0

    async def wait_for(self, task, timeout, interval):
        """
        Wait for the given async task function to return a value. Give up
        waiting for the task's completion after interval (seconds) and retry
        until timeout (seconds) expires. Raise trio.TooSlowError when timeout expires.

        Important:
         * The given task function must be async, otherwise a TypeError exception will be thrown.
         * Retries may occur more frequently than interval if the task
           returns None before interval expires. This only matters in that it
           uses more CPU.
        """
        assert inspect.iscoroutinefunction(task)
        with trio.fail_after(timeout):
            while True:
                with trio.move_on_after(interval):
                    result = await task()
                    if result is not None:
                        return result
                    else:
                        await trio.sleep(0.1)


    async def retrieve_metric(self, replica_id, component_name, type_, key):
        try:
            return await self.metrics.get(replica_id, component_name, type_, key)
        except KeyError:
            return None

    async def commit_count(self, replica_id: int = 0):
        """
        Returns the number of committed requests
        """
        with log.start_action(action_type="commit_count"):
            async def request_count():
                metric_key = ['replica', 'Counters', 'total_committed_seqNum']
                return await self.retrieve_metric(replica_id, *metric_key)

        return await self.wait_for(request_count, timeout=5., interval=.5)

    async def commit_count_by_path(self, path_type: ConsensusPathType, replica_id: int = 0):
        """
        Returns the total number of executed sequence numbers on path_type
        """
        metric_params = PATH_TYPE_TO_METRIC_PARAMS[path_type]
        with log.start_action(action_type="commit_count", path_type=path_type.value, replica_id=replica_id):
            async def request_count():
                metric_key = ['replica', 'Counters', metric_params.metric_name]
                return await self.retrieve_metric(replica_id, *metric_key)

        return await self.wait_for(request_count, timeout=metric_params.timeout, interval=metric_params.interval)

    async def num_of_fast_path_requests(self, replica_id=0):
        """
        Returns the total number of requests processed on the fast commit path
        """
        with log.start_action(action_type="num_of_fast_path_requests", replica_id=replica_id):
            async def the_number_of_fast_path_requests():
                metric_key = ['replica', 'Counters', 'totalFastPathRequests']
                nb_fast_path = await self.retrieve_metric(replica_id, *metric_key)
                return nb_fast_path

        return await self.wait_for(the_number_of_fast_path_requests, 5, 2)

    async def num_of_slow_path_requests(self, replica_id=0):
        """
        Returns the total number of requests processed on the slow commit path
        """
        with log.start_action(action_type="num_of_slow_path_requests", replica_id=replica_id):
            async def the_number_of_slow_path_requests():
                metric_key = ['replica', 'Counters', 'totalSlowPathRequests']
                nb_slow_path = await self.retrieve_metric(replica_id, *metric_key)
                return nb_slow_path

        return await self.wait_for(the_number_of_slow_path_requests, 5, .5)

    async def last_db_checkpoint_block_id(self, replica_id=0):
        """
        Returns the block ID of the last RocksDB checkpoint
        """
        with log.start_action(action_type="last_db_checkpoint_block_id", replica_id=replica_id):
            async def the_last_db_checkpoint_block_id():
                metric_key = ['rocksdbCheckpoint', 'Gauges', 'lastDbCheckpointBlockId']
                snapshot_id = await self.retrieve_metric(replica_id, *metric_key)
                return snapshot_id

        return await self.wait_for(the_last_db_checkpoint_block_id, 5, .5)

    async def check_initial_master_key_publication(self, replicas):
        prev_num_executions = 0
        with log.start_action(action_type="check_initial_master_key_publication"), trio.fail_after(seconds=120):
            succ = False
            while succ is False:
                succ = True
                for r in replicas:
                    with trio.move_on_after(seconds=1):
                        try:
                            current_num_executions = int(await self.get_metric(r, self, "Counters", "executions", "reconfiguration_dispatcher"))
                            if current_num_executions != prev_num_executions:
                                log.log_message(message_type="Reconfig execution progress", current_num_executions=current_num_executions, prev_num_executions=prev_num_executions)
                                prev_num_executions = current_num_executions
                            if current_num_executions < len(replicas):
                                succ = False
                                break
                        except trio.TooSlowError:
                            succ = False
                            break
                await trio.sleep(1)

    async def check_initial_key_exchange(self, stop_replicas=True, full_key_exchange=False, replicas_to_start=[], check_master_key_publication=False):
        """
        Performs initial key exchange, starts all replicas, validate the exchange and stops all replicas.
        The stop is done in order for a test who uses this functionality, to proceed without imposing n up replicas.
        """
        required_exchanges = self.config.n - 1 if full_key_exchange else 2 * self.config.f + self.config.c

        with log.start_action(action_type="check_initial_key_exchange", required_exchanges=required_exchanges):
            replicas_to_start = [r for r in range(self.config.n)] if replicas_to_start == [] else replicas_to_start
            self.start_replicas(replicas_to_start)
            num_of_exchanged_replicas = 0
            with trio.fail_after(seconds=120):
                for replica_id in replicas_to_start:
                    while True:
                        with trio.move_on_after(seconds=1):
                            try:
                                sent_key_exchange_on_start_status = await self.metrics.get(replica_id, *["KeyExchangeManager", "Statuses", "sent_key_exchange_on_start"])
                                sent_key_exchange_counter = await self.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "sent_key_exchange"])
                                self_key_exchange_counter = await self.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                                public_key_exchange_for_peer_counter = await self.metrics.get(replica_id, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                                if sent_key_exchange_on_start_status == 'False' or \
                                    sent_key_exchange_counter < 1 or \
                                    self_key_exchange_counter < 1 or \
                                    public_key_exchange_for_peer_counter < required_exchanges :
                                    await trio.sleep(0.1)
                                    continue
                            except trio.TooSlowError:
                                print(f"Replica {replica_id} was not able to exchange keys on start")
                                raise KeyExchangeError
                            else:
                                assert sent_key_exchange_on_start_status == 'True'
                                assert sent_key_exchange_counter >= 1
                                assert self_key_exchange_counter >= 1
                                assert public_key_exchange_for_peer_counter >= required_exchanges
                                num_of_exchanged_replicas += 1
                                log.log_message(message_type=f"Replica {replica_id} exchanged its key", num_of_exchanged_replicas=num_of_exchanged_replicas, required_exchanges=required_exchanges)
                                break
                    if num_of_exchanged_replicas >= len(replicas_to_start):
                        break
            with trio.fail_after(seconds=5):
                lastExecutedKey = ['replica', 'Gauges', 'lastExecutedSeqNum']
                rep = random.choice(replicas_to_start)
                lastExecutedVal = await self.metrics.get(rep, *lastExecutedKey)
            if check_master_key_publication:
                await self.check_initial_master_key_publication(replicas_to_start)
            if stop_replicas:
                self.stop_replicas(replicas_to_start)

            return lastExecutedVal

    async def assert_successful_pre_executions_count(self, replica_id, num_requests):
        try:
            pre_proc_req = 0
            total_pre_exec_requests_executed = 0
            with trio.fail_after(10):
                while pre_proc_req < num_requests or \
                        total_pre_exec_requests_executed < num_requests:
                    key1 = ["preProcessor", "Counters", "preProcReqCompleted"]
                    pre_proc_req = await self.metrics.get(replica_id, *key1) - self.initial_preexec_sent
                    key2 = ["replica", "Counters", "totalPreExecRequestsExecuted"]
                    total_pre_exec_requests_executed = await self.metrics.get(replica_id, *key2) - self.initial_preexec_executed
                    await trio.sleep(0.1)
        except trio.TooSlowError:
            assert False, "Preprocessor requests " + \
                      f'(expected={num_requests} ' \
                      f'executed={total_pre_exec_requests_executed} ' \
                      f'preexecsent={pre_proc_req})'
        except AttributeError:
            assert False, "Preprocessor counter not initialized"

    async def init_preexec_count(self, replica_id=0):
        # read initial preexecutions count, so we can adjust expectations for individual tests
        key1 = ["preProcessor", "Counters", "preProcReqCompleted"]
        self.initial_preexec_sent = await self.metrics.get(replica_id, *key1)
        key2 = ["replica", "Counters", "totalPreExecRequestsExecuted"]
        self.initial_preexec_executed = await self.metrics.get(replica_id, *key2)

    async def wait_for_replica_to_ask_for_view_change(self, replica_id, previous_asks_to_leave_view_msg_count = 0):
        """
        Wait for a single replica to send a ReplicaAsksToLeaveViewMsg.
        """
        with log.start_action(action_type="wait_for_replica_asks_to_leave_view_msg", replica=replica_id,
                              previous_asks_to_leave_view_msg_count=previous_asks_to_leave_view_msg_count) as action:

            async def the_replica_to_ask_for_view_change():
                key = ['replica', 'Gauges', 'sentReplicaAsksToLeaveViewMsg']
                replica_asks_to_leave_view_msg_count = await self.retrieve_metric(replica_id, *key)

                if replica_asks_to_leave_view_msg_count is not None and replica_asks_to_leave_view_msg_count > previous_asks_to_leave_view_msg_count:
                                action.add_success_fields(
                                    replica_asks_to_leave_view_msg_count=replica_asks_to_leave_view_msg_count)
                                return replica_asks_to_leave_view_msg_count

        return await self.wait_for(the_replica_to_ask_for_view_change, 60, .5)

    async def wait_for_replicas_to_reach_view(self, replicas_ids, expected_view):
        """
        Wait for all replicas to reach the expected view.
        """
        with log.start_action(action_type="wait_for_replicas_to_reach_view", replicas_ids=replicas_ids, expected_view=expected_view) as action:
            with trio.fail_after(seconds=30):
                async with trio.open_nursery() as nursery:
                    for replica_id in replicas_ids:
                        nursery.start_soon(self.wait_for_replica_to_reach_view, replica_id, expected_view)

    async def wait_for_replicas_to_reach_at_least_view(self, replicas_ids, expected_view, timeout=30):
        """
        Wait for all replicas to reach the expected view.
        """
        with log.start_action(action_type="wait_for_replicas_to_reach_view", replicas_ids=replicas_ids, expected_view=expected_view) as action:
            with trio.fail_after(seconds=timeout):
                async with trio.open_nursery() as nursery:
                    for replica_id in replicas_ids:
                        nursery.start_soon(self.wait_for_replica_to_reach_at_least_view, replica_id, expected_view)

    async def wait_for_replica_to_reach_view(self, replica_id, expected_view):
        """
        Wait for a single replica to reach the expected view.
        """
        with log.start_action(action_type="wait_for_replica_to_reach_view", replica=replica_id,
                              expected_view=expected_view) as action:
            async def expected_view_to_be_reached():
                key = ['replica', 'Gauges', 'view']
                replica_view = await self.retrieve_metric(replica_id, *key)

                if replica_view is not None and replica_view == expected_view:
                    return replica_view

        return await self.wait_for(expected_view_to_be_reached, 30, .5)

    async def wait_for_replica_to_reach_at_least_view(self, replica_id, expected_view):
        """
        Wait for a single replica to reach the expected view.
        """
        with log.start_action(action_type="wait_for_replica_to_reach_at_least_view", replica=replica_id,
                              expected_view=expected_view) as action:
            async def expected_view_to_be_reached():
                key = ['replica', 'Gauges', 'view']
                replica_view = await self.retrieve_metric(replica_id, *key)

                if replica_view is not None and replica_view >= expected_view:
                    return replica_view


    @staticmethod
    @lru_cache(maxsize=None)
    def binary_digest(binary_path: str):
        buf_size = 4096
        sha1 = hashlib.sha1()

        with open(binary_path, 'rb') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()

    def reset_metadata(self, replica_id):
        with log.start_action(action_type="reset_metadata", replica_id=replica_id) as action:
            db_editor_path = os.path.join(self.builddir, "kvbc", "tools", "db_editor", "kv_blockchain_db_editor")
            db_dir = os.path.join(self.testdir, DB_FILE_PREFIX + str(replica_id))
            reset_md_cmd = [db_editor_path,
                            db_dir,
                            "resetMetadata",
                            str(replica_id),
                            str(self.config.f),
                            str(self.config.c),
                            "37",  # number of principles, defined in setup.cpp
                            "10"]  # max client batch size, defined in ReplicaConfig.hpp
            out_file_name = "reset_md_out." + str(replica_id) + ".log"
            err_file_name = "reset_md_err." + str(replica_id) + ".log"
            with open(out_file_name, 'w+') as stdout_file, open(err_file_name, 'w+') as stderr_file:
                res = subprocess.run(reset_md_cmd, stdout=stdout_file, stderr=stderr_file)
                assert res.returncode == 0

    def remove_metadata(self, replica_id):
        with log.start_action(action_type="remove_metadata", replica_id=replica_id) as action:
            db_editor_path = os.path.join(self.builddir, "kvbc", "tools", "db_editor", "kv_blockchain_db_editor")
            db_dir = os.path.join(self.testdir, DB_FILE_PREFIX + str(replica_id))
            remove_md_cmd = [db_editor_path,
                            db_dir,
                            "removeMetadata",
                            str(replica_id),
                            str(self.config.f),
                            str(self.config.c)]
            out_file_name = "remove_md_out." + str(replica_id) + ".log"
            err_file_name = "remove_md_err." + str(replica_id) + ".log"
            with open(out_file_name, 'w+') as stdout_file, open(err_file_name, 'w+') as stderr_file:
                res = subprocess.run(remove_md_cmd, stdout=stdout_file, stderr=stderr_file)
                assert res.returncode == 0


    async def wait_for_stable_checkpoint(self, replicas, stable_seqnum):
        with trio.fail_after(seconds=30), log.start_action(action_type="wait_for_stable_checkpoint",
                                                           replicas=replicas, stable_seqnum=stable_seqnum):
            all_in_checkpoint = False
            while all_in_checkpoint is False:
                all_in_checkpoint = True
                for r in replicas:
                    lastStable = await self.get_metric(r, self, "Gauges", "lastStableSeqNum")
                    if lastStable != stable_seqnum:
                        all_in_checkpoint = False
                        break
                await trio.sleep(0.5)

    async def wait_for_created_db_snapshots_metric(self, replicas, expected_num_of_created_snapshots):
        with trio.fail_after(seconds=30):
            while True:
                found_mismatch = False
                for replica_id in replicas:
                    num_of_created_snapshots = await self.get_metric(replica_id, self,
                                                                            "Counters", "numOfDbCheckpointsCreated",
                                                                            component="rocksdbCheckpoint")
                    if num_of_created_snapshots != expected_num_of_created_snapshots:
                        found_mismatch = True
                        break
                if found_mismatch:
                    await trio.sleep(0.5)
                else:
                    break

    def db_snapshot_exists(self, replica_id, snapshot_id=None):
        snapshot_db_dir = os.path.join(
            self.testdir, DB_SNAPSHOT_PREFIX + str(replica_id))
        if snapshot_id is not None:
            snapshot_db_dir = os.path.join(snapshot_db_dir, str(snapshot_id))
        if not os.path.exists(snapshot_db_dir):
            return False

        # Make sure that checkpoint folder is not empty.
        size = 0
        for element in os.scandir(snapshot_db_dir):
            size += os.path.getsize(element)
        return (size > 0)

    async def wait_for_db_snapshot(self, replica_id, snapshot_id=None):
        with trio.fail_after(seconds=30), log.start_action(action_type="wait_for_db_snapshot", replica_id=replica_id,
                                                           snapshot_id=snapshot_id):
            while True:
                if self.db_snapshot_exists(replica_id, snapshot_id) == True:
                    break
                await trio.sleep(0.5)

    def verify_db_snapshot_is_available(self, replica_id, snapshot_id, isPresent=True):
        with log.start_action(action_type="verify snapshot db files"):
            snapshot_db_dir = os.path.join(
                self.testdir, DB_SNAPSHOT_PREFIX + str(replica_id) + "/" + str(snapshot_id))
            if isPresent == True:
                assert self.db_snapshot_exists(replica_id, snapshot_id) == True
            else:
                assert os.path.exists(snapshot_db_dir) == False

    def restore_form_older_db_snapshot(self, snapshot_id, src_replica, dest_replicas, prefix):
        with log.start_action(action_type="restore with older snapshot"):
            snapshot_db_dir = os.path.join(
                self.testdir, DB_SNAPSHOT_PREFIX + str(src_replica) + "/" + str(snapshot_id))
            temp_dir = os.path.join(tempfile.gettempdir(),
                                    prefix + str(src_replica) + "/" + str(snapshot_id))
            ret = shutil.copytree(snapshot_db_dir, temp_dir)
            for r in dest_replicas:
                dest_db_dir = os.path.join(
                    self.testdir, DB_FILE_PREFIX + str(r))
                if os.path.exists(dest_db_dir):
                    shutil.rmtree(dest_db_dir)
                ret = shutil.copytree(temp_dir, dest_db_dir)
                log.log_message(
                    message_type=f"copy db files from {snapshot_db_dir} to {dest_db_dir}, result is {ret}")
            shutil.rmtree(temp_dir)

    @staticmethod
    def assert_dirs_exist(dirs):
        for dir in map(os.path.abspath, dirs):
            assert os.path.isdir(dir), f"{dir} must exist!"