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
import subprocess
import time, sys, os
from re import split
from abc import ABC, abstractmethod
from itertools import combinations
from collections import namedtuple
from functools import partial
sys.path.append(os.path.abspath("../../util/pyclient"))
import bft_config

class NetworkPartitioningAdversary(ABC):
    """Represents an adversary capable of inflicting network partitioning"""

    BFT_NETWORK_PARTITIONING_RULE_CHAIN = "bft-network-partitioning"

    def __init__(self, bft_network):
        self.bft_network = bft_network
        self.connections = {}
        self.all_client_ids = bft_network.all_client_ids()
        self.comm_type = self.bft_network.comm_type()
        assert self.comm_type == bft_config.COMM_TYPE_TCP_TLS or self.comm_type == bft_config.COMM_TYPE_UDP

    def __enter__(self):
        """context manager method for 'with' statements"""
        self._init_bft_network_rule_chain()
        if self.comm_type == bft_config.COMM_TYPE_TCP_TLS:
            # Build port connectivity matrix, to map source/dest node IDs into matching ports.
            self._build_port_connectivity_matrix()
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        self._remove_bft_network_rule_chain()

    @abstractmethod
    def interfere(self):
        """ This is where the actual malicious behavior is defined """
        pass

    def _init_bft_network_rule_chain(self):
        subprocess.run(
            ["iptables", "-N", self.BFT_NETWORK_PARTITIONING_RULE_CHAIN],
            check=True)
        subprocess.run(
            ["iptables", "-A", "INPUT",
             "-s", "localhost", "-d", "localhost",
             "-j", self.BFT_NETWORK_PARTITIONING_RULE_CHAIN],
            check=True)

    def _remove_bft_network_rule_chain(self):
        """ Flush all customize rules and chains, accept anything """
        subprocess.run(["iptables", "-F"], check=True)
        subprocess.run(["iptables", "-X"], check=True)

    def _port_to_node_id(self, port, other_port, pid=None):
        """
        Used only in TCP communication.
        Resolve to which node belongs a given port, when given the peer connected port other_port and
        optionally process ID of the process which openned that port.
        """
        node_id = None
        if port < other_port:
            # Known port - TCP server port
            node_id = self.bft_network.node_id_from_bft_msg_port(port)
        elif pid:
            # Unknown port and pid is given - TCP client port. The node id belongs to a client node or a replica with 
            # process Id pid. Look in the replica procid table, maybe it is there (if it is a replica).
            node_id = self.bft_network.replica_id_from_pid(pid)
        else:
            # Unknown port and pid not given - try to get the process ID which openned the port using shell command 
            # fuser.
            fuser_output = subprocess.run(['fuser', str(port) + '/tcp'], check=True,  universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc_id = int(fuser_output.stdout.lstrip())
            node_id = self.bft_network.replica_id_from_pid(proc_id)

        # None of the previous methods worked - this is a client node unknown port, search in all clients
        # It must be found there
        if node_id is None:
            for id in self.all_client_ids:
                client = self.bft_network.get_client(id)
                if port in client.get_connections_port_list():
                    node_id = id
                    break
        assert node_id is not None
        return node_id

    def _build_port_connectivity_matrix(self, time_to_wait_sec=5.0, time_between_retries=0.25):
        """
        Used only in TCP communication.
        Build a connectivity matrix self.connections, a dictionary which maps a pair src_node_id, dest_node_id
        into a matching source_port and dest_port.
        Having this data structure allows to translate "block communication between node id X and node it Y" very easily
        onto the underlaying ports.
        Since module cannot know if all replicas started or no, it retries the discovery in a loop, with
        time_between_retries seconds between retries. It fails after time_to_wait_sec seconds (This should never happen)
        """
        assert isinstance(time_to_wait_sec, float) and isinstance(time_between_retries, float)
        wait_until_time = time.time() + time_to_wait_sec
        server_msg_ports = [bft_config.bft_msg_port_from_node_id(i) for i in self.bft_network.all_replicas()]
        all_connections_established = False
        num_replicas = self.bft_network.config.n
        # Rules to calculate num_expected_connections:
        # 1) Each client/replica connect to all other replicas with lower ID. Hence, replica 0 does not connect to
        # anyone.
        # 2) Each active client connects to all other replicas.
        # 3) Clients do not connect to each other.
        # 4) Each connection is bi-directional (hence 2 lines in output.
        # 5) Each connection is uni-directional for our purpose.
        # So between replicas we get 2 * (0 + 1 + 2... + N-1) = N * (N -1) connections, where N is the number of
        # replicas and for the clients <-> replicas we get 2 * N * num_clients connections
        num_expected_connections = num_replicas * (num_replicas - 1) + (len(self.all_client_ids) * num_replicas * 2)
        while time.time() < wait_until_time:
            try:
                # Use 'netstat -tpun' and grep on server known ports, to get all established TCP connection details
                netstat_output = subprocess.run(['netstat', '-tpun'],
                                                check=True, universal_newlines=True,
                                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # stringify the grep command regex
                grep_format = "127.0.0.1:%d .* ESTABLISHED "
                grep_str = ""
                grep_reg_or = "\\|"
                for i in range(len(server_msg_ports)):
                    grep_str += (grep_format % server_msg_ports[i]) + grep_reg_or
                grep_str = grep_str[:len(grep_str) - len(grep_reg_or)]
                grep_output = subprocess.run(['grep', grep_str],
                                             check=True, universal_newlines=True, input=netstat_output.stdout,
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                netstat_lines = grep_output.stdout.splitlines()
                if len(netstat_lines) == num_expected_connections:
                    # Success, all expected data is parsed, break.
                    all_connections_established = True
                    break
                else:
                    # Failure, connections are not yet fully established, need to retry
                    time.sleep(time_between_retries)
                    continue
            except subprocess.CalledProcessError:
                # Failure, connections are not yet fully established, need to retry
                time.sleep(time_between_retries)
                continue

        assert all_connections_established, "Failed to build port connectivity matrix using netstat!"

        # Here we pars the data we got from grep_output
        for line in netstat_lines:
            tokens = list(filter(None, split(' |:|/', line)))
            src_port = int(tokens[4])
            dst_port = int(tokens[6])
            src_proc_id = int(tokens[8])
            assert (src_port in server_msg_ports) ^ (dst_port in server_msg_ports)

            src_node_id = self._port_to_node_id(src_port, dst_port, src_proc_id)
            dst_node_id = self._port_to_node_id(dst_port, src_port)

            self.connections[(src_node_id, dst_node_id)] = (src_port, dst_port)
        assert len(self.connections) == num_expected_connections
        # Uncomment to print all connections (for debug)
        # from pprint import pprint; pprint(self.connections)

    def _drop_packets_between(self, src_node_id, dst_node_id, drop_rate_percentage=100):
        """ Drop all packets send between src_node_id and dst_node_id with probability drop_rate_percentage """
        if self.comm_type == bft_config.COMM_TYPE_TCP_TLS:
            # Get source and destination ports from already initialized self.connections
            (src_port, dst_port) = self.connections[(src_node_id, dst_node_id)]
        else:
            # Source and destination ports are well-known ports
            src_port = bft_config.bft_msg_port_from_node_id(src_node_id)
            dst_port = bft_config.bft_msg_port_from_node_id(dst_node_id)
            pass

        self._drop_packets_between_ports(src_port, dst_port, drop_rate_percentage)

    def _drop_packets_between_ports(self, src_port, dst_port, drop_rate_percentage=100):
        assert 0 <= drop_rate_percentage <= 100
        drop_rate = drop_rate_percentage / 100
        subprocess.run(
            ["iptables", "-A", self.BFT_NETWORK_PARTITIONING_RULE_CHAIN,
             "-p", "tcp" if self.comm_type == bft_config.COMM_TYPE_TCP_TLS else "udp",
             "--sport", str(src_port), "--dport", str(dst_port),
             "-m", "statistic", "--mode", "random",
             "--probability", str(drop_rate),
             "-j", "DROP"],
            check=True
        )

class PassiveAdversary(NetworkPartitioningAdversary):
    """ Adversary does nothing = synchronous network """

    def interfere(self):
        pass

class PrimaryIsolatingAdversary(NetworkPartitioningAdversary):
    """ Adversary that intercepts and drops all outgoing packets from the current primary """

    async def interfere(self):
        primary = await self.bft_network.get_current_primary()

        non_primary_replicas = self.bft_network.all_replicas(without={primary})
        for replica in non_primary_replicas:

            self._drop_packets_between(primary, replica)

class PacketDroppingAdversary(NetworkPartitioningAdversary):
    """ Adversary that drops random packets between all replicas """

    def __init__(self, bft_network, drop_rate_percentage=50):
        self.drop_rate_percentage = drop_rate_percentage
        super(PacketDroppingAdversary, self).__init__(bft_network)

    def interfere(self):
        # drop some packets between every two replicas
        for connection in combinations(self.bft_network.all_replicas(), 2):
            self._drop_packets_between(connection[0], connection[1], self.drop_rate_percentage)

class ReplicaSubsetIsolatingAdversary(NetworkPartitioningAdversary):
    """
    Adversary that isolates a sub-set of replicas,
    both from other replicas, as well as from the clients.
    """

    def __init__(self, bft_network, replicas_to_isolate):
        assert len(replicas_to_isolate) < bft_network.config.n
        self.replicas_to_isolate = replicas_to_isolate
        super(ReplicaSubsetIsolatingAdversary, self).__init__(bft_network)

    def interfere(self):
        other_replicas = set(self.bft_network.all_replicas()) - set(self.replicas_to_isolate)
        for ir in self.replicas_to_isolate:
            for r in other_replicas:
                self._drop_packets_between(ir, r)
                self._drop_packets_between(r, ir)

        for ir in self.replicas_to_isolate:
            for client_id in self.all_client_ids:
                self._drop_packets_between(client_id, ir)
                self._drop_packets_between(ir, client_id)

class ReplicaSubsetOneWayIsolatingAdversary(NetworkPartitioningAdversary):
    """
    Adversary that isolates all messages except client requests
    to a subset of replicas. The Replicas in the subset will be able 
    to send msgs to other replicas in the network, but anything addressed
    to them by other replicas will be dropped.
    """

    def __init__(self, bft_network, replicas_to_isolate):
        assert len(replicas_to_isolate) < bft_network.config.n
        self.replicas_to_isolate = replicas_to_isolate
        super(ReplicaSubsetOneWayIsolatingAdversary, self).__init__(bft_network)

    def interfere(self):
        for ir in self.replicas_to_isolate:
            for r in self.bft_network.all_replicas():
                if ir != r:
                    self._drop_packets_between(ir, r)

class ReplicaOneWayTwoSubsetsIsolatingAdversary(NetworkPartitioningAdversary):
    """
    Adversary that isolates all messages except client requests
    to a subset of replicas (blocked receivers) from another subset (blocked senders).
    The Replicas in the blocked receivers subset will be able to send msgs to all other
    replicas in the network, but anything addressed to them by the blocked senders set
    of replicas will be dropped.
    """

    def __init__(self, bft_network, blocked_receivers, blocked_senders):
        self._rules = []
        self.config = bft_network.config
        self.add_rule(blocked_receivers, blocked_senders)
        super(ReplicaOneWayTwoSubsetsIsolatingAdversary, self).__init__(bft_network)

    def add_rule(self, blocked_receivers, blocked_senders):
        assert len(blocked_receivers) < self.config.n
        assert len(blocked_senders) < self.config.n
        self._rules.append((blocked_receivers, blocked_senders))

    def interfere(self):
        for blocked_receivers, blocked_senders in self._rules:
            for sender in blocked_senders:
                for receiver in blocked_receivers:
                    assert sender != receiver
                    self._drop_packets_between(sender, receiver)

class ReplicaSubsetTwoWayIsolatingAdversary(NetworkPartitioningAdversary):
    """
    Adversary that isolates all messages except client requests
    to a subset of replicas. The Replicas in the subset will not
    be able to send or receive messages from other replicas.
    """

    def __init__(self, bft_network, replicas_to_isolate):
        assert len(replicas_to_isolate) < bft_network.config.n
        self.replicas_to_isolate = replicas_to_isolate
        super(ReplicaSubsetTwoWayIsolatingAdversary, self).__init__(bft_network)

    def interfere(self, drop_rate_percentage=100):
        for ir in self.replicas_to_isolate:
            for r in self.bft_network.all_replicas():
                if ir != r:
                    self._drop_packets_between(ir, r, drop_rate_percentage)
                    self._drop_packets_between(r, ir, drop_rate_percentage)

