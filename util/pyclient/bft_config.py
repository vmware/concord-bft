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

# This code requires python 3.5 or later
from collections import namedtuple

Config = namedtuple('Config', ['id', 'f', 'c', 'max_msg_size', 'req_timeout_milli',
                               'retry_timeout_milli', "certs_path", "txn_signing_keys_path",
                               "principals_to_participant_map"])

Replica = namedtuple('Replica', ['id', 'ip', 'port', 'metrics_port'])

START_BFT_MSG_PORT = 3710
START_METRICS_PORT = 4710


def bft_msg_port_from_node_id(id):
    return START_BFT_MSG_PORT + 2 * id


def metrics_port_from_node_id(id):
    return START_METRICS_PORT + 2 * id


COMM_TYPE_TCP_TLS = "tcp_tls"
COMM_TYPE_UDP = "udp"


class BFTConfig:

    def __init__(self, n: int, f: int = 1, c: int = 0, clients=10):
        self.n : int = n
        self.f : int = f
        self.c : int = c
        self.clients : int = clients
        assert self.n >= 3 * self.f + 2 * self.c + 1, \
            f"Invariant breached. Expected: n >= 3f + 2c + 1"

    def __repr__(self):
        return f'n={self.n}_f={self.f}_c={self.c}'
