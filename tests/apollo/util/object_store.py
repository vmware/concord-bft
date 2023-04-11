# Concord
#
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.
import os
import random
import string
import subprocess
import tempfile
import shutil
import time
from util import eliot_logging as log
from functools import wraps
from util.bft import KEY_FILE_PREFIX

MINIO_DATA_DIR="/tmp/concord_bft_minio_datadir"
def start_replica_cmd_prefix(builddir, replica_id, config):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    The replica is started with a short view change timeout.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    path_to_s3_config = os.path.join(builddir, "test_s3_config_prefix.txt")
    if replica_id >= config.n and replica_id < config.n + config.num_ro_replicas:
        bucket = "blockchain-" + ''.join(random.choice('0123456789abcdefghijklmnopqrstuvwxyz') for i in range(6))
        with open(path_to_s3_config, "w") as f:
            f.write("# test configuration for S3-compatible storage\n"
                    "s3-bucket-name:" + bucket + "\n"
                    "s3-access-key: concordbft\n"
                    "s3-protocol: HTTP\n"
                    "s3-url: 127.0.0.1:9000\n"
                    "s3-secret-key: concordbft\n"
                    "s3-path-prefix: concord")
        os.makedirs(os.path.join(MINIO_DATA_DIR, "data", bucket))     # create new bucket for this run

    ro_params = [ "--s3-config-file",
                  path_to_s3_config
                  ]

    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    ret = [path,
           "-k", KEY_FILE_PREFIX,
           "-i", str(replica_id),
           "-s", statusTimerMilli,
           "-V", os.getenv('BLOCK_CHAIN_VERSION', default="1"),
           "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties")
           ]
    if replica_id < config.n:
        ret += ["--key-exchange-on-start", "--publish-master-key-on-startup"]
    if replica_id >= config.n and replica_id < config.n + config.num_ro_replicas:
        ret.extend(ro_params)

    return ret


class ObjectStore:

    def __init__(self):
        log.log_message(message_type="Running in S3 mode.")

        # We need a temp dir for data and binaries - this is self.dest_dir
        # self.dest_dir will contain data dir for minio buckets and the minio binary
        # if there are any directories inside data dir - they become buckets
        self.work_dir = MINIO_DATA_DIR

        random_end_str = ''.join(random.choice(string.ascii_letters) for i in range(20))
        self.minio_server_data_dir = os.path.join(self.work_dir, "data_",random_end_str)
        os.makedirs(os.path.join(self.minio_server_data_dir))
        log.log_message(message_type=f"Working in {self.work_dir}")
        self.start_s3_server()
        log.log_message(message_type="Initialisation complete")

    def start_s3_server(self):
        log.log_message(message_type="Starting server")
        server_env = os.environ.copy()
        server_env["MINIO_ACCESS_KEY"] = "concordbft"
        server_env["MINIO_SECRET_KEY"] = "concordbft"
        server_env["CI"] = "on"

        self.minio_server_proc = subprocess.Popen(["minio", "server", self.minio_server_data_dir],
                                                 env = server_env,
                                                 close_fds=True)


    def __del__(self):
        # First stop the server gracefully
        self.minio_server_proc.kill()
        self.minio_server_proc.wait()

        # Delete workdir dir
        shutil.rmtree(self.work_dir)

    def stop_s3_server(self):
        self.minio_server_proc.kill()
        self.minio_server_proc.wait()

    def stop_s3_for_X_secs(self, x):
        self.stop_s3_server()
        time.sleep(x)
        self.start_s3_server()

    def start_s3_after_X_secs(self, x):
        time.sleep(x)
        self.start_s3_server()
