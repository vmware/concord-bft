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

import unittest
import trio
import os
import random
import subprocess
import tempfile
import shutil
import time

from util import bft
from util import skvbc as kvbc
from util.skvbc import SimpleKVBCProtocol
from util.skvbc_history_tracker import verify_linearizability
from math import inf

from util.bft import KEY_FILE_PREFIX, with_trio, with_bft_network

def start_replica_cmd(builddir, replica_id, config):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout and with RocksDB
    persistence enabled (-p).

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    ro_params = [ "--s3-config-file",
                    os.path.join(builddir, "tests", "simpleKVBC", "scripts", "test_s3_config.txt")
                ]
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    ret = [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-p",
            "-t", os.environ.get('STORAGE_TYPE')
            ]
    if replica_id >= config.n and replica_id < config.n + config.num_ro_replicas and os.environ["USE_S3_OBJECT_STORE"]:
        ret.extend(ro_params)
        
    return ret

class SkvbcReadOnlyReplicaTest(unittest.TestCase):
    @classmethod
    def _start_s3_server(cls):
        print("Starting server")
        server_env = os.environ.copy()
        server_env["MINIO_ACCESS_KEY"] = "concordbft"
        server_env["MINIO_SECRET_KEY"] = "concordbft"

        minio_server_fname = os.environ.get("CONCORD_BFT_MINIO_BINARY_PATH")
        if minio_server_fname is None:
            shutil.rmtree(cls.work_dir)
            raise RuntimeError("Please set path to minio binary to CONCORD_BFT_MINIO_BINARY_PATH env variable")

        cls.minio_server_proc = subprocess.Popen([minio_server_fname, "server", cls.minio_server_data_dir], 
                                                    env = server_env, 
                                                    close_fds=True)

    @classmethod
    def setUpClass(cls):
        if not os.environ["USE_S3_OBJECT_STORE"]:
            return

        # We need a temp dir for data and binaries - this is cls.dest_dir
        # self.dest_dir will contain data dir for minio buckets and the minio binary
        # if there are any directories inside data dir - they become buckets
        cls.work_dir = "/tmp/concord_bft_minio_datadir_" + next(tempfile._get_candidate_names())
        cls.minio_server_data_dir = os.path.join(cls.work_dir, "data")
        os.makedirs(os.path.join(cls.work_dir, "data", "blockchain"))     # create all dirs in one call

        print(f"Working in {cls.work_dir}")

        # Start server
        cls._start_s3_server()
        
        print("Initialisation complete")

    @classmethod
    def tearDownClass(cls):
        if not os.environ["USE_S3_OBJECT_STORE"]:
            return

        # First stop the server gracefully
        cls.minio_server_proc.terminate()
        cls.minio_server_proc.wait()

        # Delete workdir dir
        shutil.rmtree(cls.work_dir)

    @classmethod
    def _stop_s3_server(cls):
        cls.minio_server_proc.kill()
        cls.minio_server_proc.wait()

    @classmethod
    def _stop_s3_for_X_secs(cls, x):
        cls._stop_s3_server()
        time.sleep(x)
        cls._start_s3_server()

    @classmethod
    def _start_s3_after_X_secs(cls, x):
        time.sleep(x)
        cls._start_s3_server()

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd, num_ro_replicas=1)
    @verify_linearizability
    async def test_ro_replica_start_with_delay(self, bft_network, tracker):
        """
        Start up N of N regular replicas.
        Send client commands until checkpoint 1 is reached.
        Start read-only replica.
        Wait for State Transfer in ReadOnlyReplica to complete.
        """
        bft_network.start_all_replicas()
        # TODO replace the below function with the library function:
        # await tracker.skvbc.tracked_fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), checkpoint_num=1)
        with trio.fail_after(seconds=60):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.send_indefinite_tracked_ops)
                while True:
                    with trio.move_on_after(seconds=.5):
                        try:
                            key = ['replica', 'Gauges', 'lastStableSeqNum']
                            replica_id = 0
                            lastStableSeqNum = await bft_network.metrics.get(replica_id, *key)
                        except KeyError:
                            continue
                        else:
                            if lastStableSeqNum >= 150:
                                #enough requests
                                print("Consensus: lastStableSeqNum:" + str(lastStableSeqNum))
                                nursery.cancel_scope.cancel()
        # start the read-only replica
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)
        with trio.fail_after(seconds=60):
            while True:
                with trio.move_on_after(seconds=.5):
                    try:
                        key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                        lastExecutedSeqNum = await bft_network.metrics.get(ro_replica_id, *key)
                    except KeyError:
                        continue
                    else:
                        # success!
                        if lastExecutedSeqNum >= 150:
                            print("Replica " + str(ro_replica_id) + ": lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                            break

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd, num_ro_replicas=1)
    @verify_linearizability
    async def test_ro_replica_start_simultaneously (self, bft_network, tracker):
        """
        Start up N of N regular replicas.
        Start read-only replica.
        Send client commands.
        Wait for State Transfer in ReadOnlyReplica to complete.
        """
        bft_network.start_all_replicas()
        # start the read-only replica
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)
        # TODO replace the below function with the library function:
        # await tracker.skvbc.tracked_fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), checkpoint_num=1)     
        with trio.fail_after(seconds=60):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.run_concurrent_ops, 900, 1)
                while True:
                    with trio.move_on_after(seconds=.5):
                        try:
                            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                            lastExecutedSeqNum = await bft_network.metrics.get(ro_replica_id, *key)
                        except KeyError:
                            continue
                        else:
                            # success!
                            if lastExecutedSeqNum >= 50:
                                print("Replica" + str(ro_replica_id) + " : lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                                nursery.cancel_scope.cancel()

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd, num_ro_replicas=1)
    @verify_linearizability
    async def test_ro_replica_with_s3_failures(self, bft_network, tracker):
        bft_network.start_all_replicas()
        self.__class__._stop_s3_server()
        
        # start the read-only replica while the s3 service is down
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)
        # TODO replace the below function with the library function:
        # await tracker.skvbc.tracked_fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), checkpoint_num=1)     
        with trio.fail_after(seconds=60):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(tracker.run_concurrent_ops, 900, 1)
                
                # restore the s3 service in 15 secs
                self.__class__._start_s3_after_X_secs(15)
                
                # let it work for a while
                time.sleep(5)

                # and kill it for some time
                self.__class__._stop_s3_for_X_secs(5)

                # the ro replica should be able to survive these failures
                while True:
                    with trio.move_on_after(seconds=.5):
                        try:
                            key = ['replica', 'Gauges', 'lastExecutedSeqNum']
                            lastExecutedSeqNum = await bft_network.metrics.get(ro_replica_id, *key)
                        except KeyError:
                            continue
                        else:
                            # success!
                            if lastExecutedSeqNum >= 50:
                                print("Replica" + str(ro_replica_id) + " : lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                                nursery.cancel_scope.cancel()