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

import os.path

import trio
import sys

sys.path.append(os.path.abspath("../../build/tests/apollo/util/"))
import concord_msgs as cmf_msgs

sys.path.append(os.path.abspath("../../util/pyclient"))

import bft_client
from ecdsa import SigningKey
from ecdsa.util import sigencode_der
import hashlib
class Operator:
    def __init__(self, config, client, priv_key_dir):
        self.config = config
        self.client = client
        with open(priv_key_dir + "/operator_priv.pem") as f:
            self.private_key = SigningKey.from_pem(f.read(), hashlib.sha256)

    def _sign_reconf_msg(self, msg):
        return self.private_key.sign_deterministic(msg.serialize())

    def _construct_reconfiguration_wedge_command(self):
            wedge_cmd = cmf_msgs.WedgeCommand()
            wedge_cmd.sender = 1000
            wedge_cmd.noop = False
            reconf_msg = cmf_msgs.ReconfigurationRequest()
            reconf_msg.command = wedge_cmd
            reconf_msg.additional_data = bytes(0)
            reconf_msg.signature = bytes(0)
            reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
            return reconf_msg

    def _construct_reconfiguration_latest_prunebale_block_command(self):
        lpab_cmd = cmf_msgs.LatestPrunableBlockRequest()
        lpab_cmd.sender = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = lpab_cmd
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_wedge_status(self, fullWedge=True):
        wedge_status_cmd = cmf_msgs.WedgeStatusRequest()
        wedge_status_cmd.sender = 1000
        wedge_status_cmd.fullWedge = fullWedge
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = wedge_status_cmd
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_unwedge_status(self):
        unwedge_status_cmd = cmf_msgs.UnwedgeStatusRequest()
        unwedge_status_cmd.sender = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = unwedge_status_cmd
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_unwedge_command(self, signatures):
        unwedge_cmd = cmf_msgs.UnwedgeCommand()
        unwedge_cmd.sender = 1000
        unwedge_cmd.noop = False
        unwedge_cmd.signatures = signatures
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = unwedge_cmd
        reconf_msg.additional_data = bytes(0)
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_prune_request(self, latest_pruneble_blocks):
        prune_cmd = cmf_msgs.PruneRequest()
        prune_cmd.sender = 1000
        prune_cmd.latest_prunable_block = latest_pruneble_blocks
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = prune_cmd
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_prune_status_request(self):
        prune_status_cmd = cmf_msgs.PruneStatusRequest()
        prune_status_cmd.sender = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = prune_status_cmd
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_keMsg_command(self, target_replicas = []):
        ke_command = cmf_msgs.KeyExchangeCommand()
        ke_command.sender_id = 1000
        ke_command.target_replicas = target_replicas
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = ke_command
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_addRemove_command(self, new_config):
        addRemove_command = cmf_msgs.AddRemoveCommand()
        addRemove_command.reconfiguration = new_config
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = addRemove_command
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg
    
    def _construct_reconfiguration_addRemoveWithWedge_command(self, new_config, bft=True, restart=True):
        addRemove_command = cmf_msgs.AddRemoveWithWedgeCommand()
        addRemove_command.config_descriptor = new_config
        addRemove_command.token = "dummy"
        addRemove_command.bft = bft
        addRemove_command.restart = restart
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = addRemove_command
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_addRemoveStatus_command(self):
        addRemoveStatus_command = cmf_msgs.AddRemoveStatus()
        addRemoveStatus_command.sender_id = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = addRemoveStatus_command
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_addRemoveWithWedgeStatus_command(self):
        addRemoveStatus_command = cmf_msgs.AddRemoveWithWedgeStatus()
        addRemoveStatus_command.sender_id = 1000
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = addRemoveStatus_command
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_clientKe_command(self, target_clients = []):
        cke_command = cmf_msgs.ClientKeyExchangeCommand()
        cke_command.target_clients = target_clients
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.command = cke_command
        reconf_msg.additional_data = bytes()
        reconf_msg.signature = bytes(0)
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg
    
    def get_rsi_replies(self):
        return self.client.get_rsi_replies()
    
    async def wedge(self):
        reconf_msg = self._construct_reconfiguration_wedge_command()
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)

    async def wedge_status(self, quorum=None, fullWedge=True):
        if quorum is None:
            quorum = bft_client.MofNQuorum.All(self.client.config, [r for r in range(self.config.n)])
        msg = self._construct_reconfiguration_wedge_status(fullWedge)
        return await self.client.read(msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)

    async def unwedge(self):
        await self.can_unwedge()
        signatures = []
        for r in self.client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            signatures.append(
                (res.response.replica_id, bytes(res.response.signature)))

        msg = self._construct_reconfiguration_unwedge_command(signatures)
        return await self.client.read(msg.serialize(), reconfiguration=True)

    async def can_unwedge(self, quorum=None, fullWedge=True):
        if quorum is None:
            quorum = bft_client.MofNQuorum.All(
                self.client.config, [r for r in range(self.config.n)])
        msg = self._construct_reconfiguration_unwedge_status()
        return await self.client.read(msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)

    async def latest_pruneable_block(self):
        reconf_msg = self._construct_reconfiguration_latest_prunebale_block_command()
        return await self.client.read(reconf_msg.serialize(),
                          m_of_n_quorum=bft_client.MofNQuorum.All(self.client.config,
                                                                  [r for r in range(self.client.get_total_num_replicas())]), reconfiguration=True, include_ro=True)

    async def prune(self, latest_pruneable_blocks):
        reconf_msg = self._construct_reconfiguration_prune_request(latest_pruneable_blocks)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)

    async def prune_status(self):
        reconf_msg = self._construct_reconfiguration_prune_status_request()
        # Status is not supported by read only replicas, thus, we poll only the committers
        return await self.client.read(reconf_msg.serialize(),
                          m_of_n_quorum=bft_client.MofNQuorum.All(self.client.config, [r for r in range(
                              self.config.n)]), reconfiguration=True)

    async def key_exchange(self, target_replicas):
        reconf_msg = self._construct_reconfiguration_keMsg_command(target_replicas)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)
    
    async def client_key_exchange(self, target_clients):
        reconf_msg = self._construct_reconfiguration_clientKe_command(target_clients)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)
    
    async def add_remove(self, new_config):
        reconf_msg = self._construct_reconfiguration_addRemove_command(new_config)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)
    
    async def add_remove_with_wedge(self, new_config, bft=True, restart=True):
        reconf_msg = self._construct_reconfiguration_addRemoveWithWedge_command(new_config, bft, restart)

        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)

    async def add_remove_status(self):
        reconf_msg = self._construct_reconfiguration_addRemoveStatus_command()
        return await self.client.read(reconf_msg.serialize(), 
                                m_of_n_quorum=bft_client.MofNQuorum.All(self.client.config, [r for r in range(
                                self.config.n)]), reconfiguration=True)
    
    async def add_remove_with_wedge_status(self, quorum=None):
        if quorum is None:
            quorum = bft_client.MofNQuorum.All(self.client.config, [r for r in range(self.config.n)])
        reconf_msg = self._construct_reconfiguration_addRemoveWithWedgeStatus_command()
        return await self.client.read(reconf_msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)
