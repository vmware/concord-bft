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
from abc import ABC, abstractmethod
from itertools import combinations


class NetworkPartitioningAdversary(ABC):
    """Represents an adversary capable of inflicting network partitioning"""

    BFT_NETWORK_PARTITIONING_RULE_CHAIN = "bft-network-partitioning"

    def __init__(self, bft_network):
        self.bft_network = bft_network

    def __enter__(self):
        """context manager method for 'with' statements"""
        self._init_bft_network_rule_chain()

        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        self._remove_bft_network_rule_chain()

    @abstractmethod
    def interfere(self):
        """ This is where the actual malicious behavior is defined """
        pass

    @classmethod
    def _init_bft_network_rule_chain(cls):
        subprocess.run(
            ["iptables", "-N", cls.BFT_NETWORK_PARTITIONING_RULE_CHAIN],
            check=True)
        subprocess.run(
            ["iptables", "-A", "INPUT",
             "-s", "localhost", "-d", "localhost",
             "-j", cls.BFT_NETWORK_PARTITIONING_RULE_CHAIN],
            check=True)

    @classmethod
    def _remove_bft_network_rule_chain(cls):
        subprocess.run(
            ["iptables", "-D", "INPUT",
             "-s", "localhost", "-d", "localhost",
             "-j", cls.BFT_NETWORK_PARTITIONING_RULE_CHAIN],
            check=True)
        subprocess.run(
            ["iptables", "-F", cls.BFT_NETWORK_PARTITIONING_RULE_CHAIN], check=True)
        subprocess.run(
            ["iptables", "-X", cls.BFT_NETWORK_PARTITIONING_RULE_CHAIN], check=True)

    @classmethod
    def _drop_packets_between(
            cls, source_port, dest_port, drop_rate_percentage=100):
        assert 0 <= drop_rate_percentage <= 100
        drop_rate = drop_rate_percentage / 100
        subprocess.run(
            ["iptables", "-A", cls.BFT_NETWORK_PARTITIONING_RULE_CHAIN,
             "-p", "udp",
             "--sport", str(source_port), "--dport", str(dest_port),
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
        primary_port = self.bft_network.replicas[primary].port

        non_primary_replicas = self.bft_network.all_replicas(without={primary})

        for replica in non_primary_replicas:
            replica_port = self.bft_network.replicas[replica].port

            self._drop_packets_between(primary_port, replica_port)


class PacketDroppingAdversary(NetworkPartitioningAdversary):
    """ Adversary that drops random packets between all replicas """

    def __init__(self, bft_network, drop_rate_percentage=50):
        self.drop_rate_percentage = drop_rate_percentage
        super(PacketDroppingAdversary, self).__init__(bft_network)

    def interfere(self):
        # drop some packets between every two replicas
        for connection in combinations(self.bft_network.all_replicas(), 2):
            source_port = self.bft_network.replicas[connection[0]].port
            dest_port = self.bft_network.replicas[connection[1]].port

            self._drop_packets_between(
                source_port, dest_port, self.drop_rate_percentage
            )


class ReplicaSubsetIsolatingAdversary(NetworkPartitioningAdversary):
    """
    Adversary that isolates a sub-set of replicas,
    both from other replicas, as well as from the clients.
    """

    def __init__(self, bft_network, replicas_to_isolate):
        assert len(replicas_to_isolate) < len(bft_network.all_replicas())
        self.replicas_to_isolate = replicas_to_isolate
        super(ReplicaSubsetIsolatingAdversary, self).__init__(bft_network)

    def interfere(self):
        other_replicas = set(self.bft_network.all_replicas()) - set(self.replicas_to_isolate)
        for ir in self.replicas_to_isolate:
            for r in other_replicas:
                isolated_replica_port = self.bft_network.replicas[ir].port
                other_replica_port = self.bft_network.replicas[r].port
                self._drop_packets_between(isolated_replica_port, other_replica_port)
                self._drop_packets_between(other_replica_port, isolated_replica_port)

        clients = self.bft_network.clients.values()
        for ir in self.replicas_to_isolate:
            for c in clients:
                isolated_replica_port = self.bft_network.replicas[ir].port
                self._drop_packets_between(c.port, isolated_replica_port)
                self._drop_packets_between(isolated_replica_port, c.port)


class ReplicaSubsetOneWayIsolatingAdversary(NetworkPartitioningAdversary):
    """
    Adversary that isolates all messages except client requests
    to a subset of replicas. The Replicas in the subset will be able 
    to send msgs to other replicas in the network, but anything addressed
    to them by other replicas will be dropped.
    """

    def __init__(self, bft_network, replicas_to_isolate):
        assert len(replicas_to_isolate) < len(bft_network.all_replicas())
        self.replicas_to_isolate = replicas_to_isolate
        super(ReplicaSubsetOneWayIsolatingAdversary, self).__init__(bft_network)

    def interfere(self):
        for ir in self.replicas_to_isolate:
            for r in self.bft_network.all_replicas():
                isolated_replica_port = self.bft_network.replicas[ir].port
                other_replica_port = self.bft_network.replicas[r].port
                if ir != r:
                    self._drop_packets_between(other_replica_port, isolated_replica_port)

