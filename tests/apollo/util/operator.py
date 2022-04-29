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
from ecdsa import SECP256k1
import hashlib
from Crypto.PublicKey import RSA

import util.eliot_logging as log


class Operator:
    def __init__(self, config, client, priv_key_dir):
        self.config = config
        self.client = client
        with open(priv_key_dir + "/operator_priv.pem") as f:
            self.private_key = SigningKey.from_pem(f.read(), hashlib.sha256)

    def _sign_reconf_msg(self, msg):
        return self.private_key.sign_deterministic(msg.serialize())


    def  _construct_basic_reconfiguration_request(self, command):
        reconf_msg = cmf_msgs.ReconfigurationRequest()
        reconf_msg.additional_data = bytes(0)
        reconf_msg.sender = 1000
        reconf_msg.signature = bytes(0)
        reconf_msg.command = command
        reconf_msg.signature = self._sign_reconf_msg(reconf_msg)
        return reconf_msg

    def _construct_reconfiguration_wedge_command(self):
            wedge_cmd = cmf_msgs.WedgeCommand()
            wedge_cmd.sender = 1000
            wedge_cmd.noop = False
            return self._construct_basic_reconfiguration_request(wedge_cmd)

    def _construct_reconfiguration_latest_prunebale_block_command(self):
        lpab_cmd = cmf_msgs.LatestPrunableBlockRequest()
        lpab_cmd.sender = 1000
        return self._construct_basic_reconfiguration_request(lpab_cmd)

    def _construct_reconfiguration_wedge_status(self, fullWedge=True):
        wedge_status_cmd = cmf_msgs.WedgeStatusRequest()
        wedge_status_cmd.sender = 1000
        wedge_status_cmd.fullWedge = fullWedge
        return self._construct_basic_reconfiguration_request(wedge_status_cmd)

    def _construct_reconfiguration_unwedge_status(self, bft):
        unwedge_status_cmd = cmf_msgs.UnwedgeStatusRequest()
        unwedge_status_cmd.sender = 1000
        unwedge_status_cmd.bft_support = bft
        return self._construct_basic_reconfiguration_request(unwedge_status_cmd)

    def _construct_reconfiguration_unwedge_command(self, unwedges, bft, restart):
        unwedge_cmd = cmf_msgs.UnwedgeCommand()
        unwedge_cmd.sender = 1000
        unwedge_cmd.bft_support = bft
        unwedge_cmd.restart = restart
        unwedge_cmd.unwedges = unwedges
        return self._construct_basic_reconfiguration_request(unwedge_cmd)

    def _construct_reconfiguration_prune_request(self, latest_pruneble_blocks, tick_period_seconds=1, batch_blocks_num=600):
        prune_cmd = cmf_msgs.PruneRequest()
        prune_cmd.sender = 1000
        prune_cmd.latest_prunable_block = latest_pruneble_blocks
        prune_cmd.tick_period_seconds = tick_period_seconds
        prune_cmd.batch_blocks_num = batch_blocks_num
        return self._construct_basic_reconfiguration_request(prune_cmd)

    def _construct_reconfiguration_prune_status_request(self):
        prune_status_cmd = cmf_msgs.PruneStatusRequest()
        prune_status_cmd.sender = 1000
        return self._construct_basic_reconfiguration_request(prune_status_cmd)

    def _construct_reconfiguration_keMsg_command(self, target_replicas = [], tls=False):
        ke_command = cmf_msgs.KeyExchangeCommand()
        ke_command.sender_id = 1000
        ke_command.target_replicas = target_replicas
        ke_command.tls = tls
        return self._construct_basic_reconfiguration_request(ke_command)

    def _construct_reconfiguration_addRemove_command(self, new_config):
        addRemove_command = cmf_msgs.AddRemoveCommand()
        addRemove_command.reconfiguration = new_config
        return self._construct_basic_reconfiguration_request(addRemove_command)

    def _construct_install_command(self, version, bft=True):
        command = cmf_msgs.InstallCommand()
        command.version = version
        command.bft_support = bft
        return self._construct_basic_reconfiguration_request(command)

    def _construct_reconfiguration_addRemoveWithWedge_command(self, new_config, token, bft=True, restart=True):
        addRemove_command = cmf_msgs.AddRemoveWithWedgeCommand()
        addRemove_command.config_descriptor = new_config
        addRemove_command.token = token
        addRemove_command.bft_support = bft
        addRemove_command.restart = restart
        return self._construct_basic_reconfiguration_request(addRemove_command)

    def _construct_reconfiguration_addRemoveStatus_command(self):
        addRemoveStatus_command = cmf_msgs.AddRemoveStatus()
        addRemoveStatus_command.sender_id = 1000
        return self._construct_basic_reconfiguration_request(addRemoveStatus_command)

    def _construct_reconfiguration_addRemoveWithWedgeStatus_command(self):
        addRemoveStatus_command = cmf_msgs.AddRemoveWithWedgeStatus()
        addRemoveStatus_command.sender_id = 1000
        return self._construct_basic_reconfiguration_request(addRemoveStatus_command)

    def _construct_reconfiguration_clientKe_command(self, target_clients = [], tls = False):
        cke_command = cmf_msgs.ClientKeyExchangeCommand()
        cke_command.target_clients = target_clients
        cke_command.tls = tls
        return self._construct_basic_reconfiguration_request(cke_command)

    def _construct_reconfiguration_clientsAddRemove_command(self, config_desc, tokens):
        car_command = cmf_msgs.ClientsAddRemoveCommand()
        car_command.config_descriptor = config_desc
        car_command.token = tokens
        car_command.restart = False
        return self._construct_basic_reconfiguration_request(car_command)

    def _construct_reconfiguration_clientsAddRemoveStatus_command(self):
        cars_command = cmf_msgs.ClientsAddRemoveStatusCommand()
        cars_command.sender_id = 1000
        return self._construct_basic_reconfiguration_request(cars_command)

    def _construct_reconfiguration_clientsKeyExchangeStatus_command(self, tls=False):
        ckes_command = cmf_msgs.ClientKeyExchangeStatus()
        ckes_command.sender_id = 1000
        ckes_command.tls = tls
        return self._construct_basic_reconfiguration_request(ckes_command)

    def _construct_reconfiguration_restart_command(self, bft, restart, data):
        restart_command = cmf_msgs.RestartCommand()
        restart_command.bft_support = bft
        restart_command.restart = restart
        restart_command.data = data
        return self._construct_basic_reconfiguration_request(restart_command)
    def _construct_reconfiguration_clientsRestart_command(self, data=""):
        client_restart_command = cmf_msgs.ClientsRestartCommand()
        client_restart_command.sender_id = 1000
        client_restart_command.data = data
        return self._construct_basic_reconfiguration_request(client_restart_command)

    def _construct_clients_clientRestart_status_command(self):
        client_restart_status = cmf_msgs.ClientsRestartStatus()
        client_restart_status.sender_id = 1000
        return self._construct_basic_reconfiguration_request(client_restart_status)

    def _construct_reconfiguration_get_dbcheckpoint_info_request(self):
        cpinfo_command = cmf_msgs.GetDbCheckpointInfoRequest()
        cpinfo_command.sender_id = 1000
        return self._construct_basic_reconfiguration_request(cpinfo_command)

    def _construct_reconfiguration_create_dbcheckpoint_command(self):
        cp_command = cmf_msgs.CreateDbCheckpointCommand()
        cp_command.sender_id = 1000
        return self._construct_basic_reconfiguration_request(cp_command)

    def _construct_reconfiguration_signed_public_state_hash_req(self, snapshot_id):
        req = cmf_msgs.SignedPublicStateHashRequest()
        req.snapshot_id = snapshot_id
        req.participant_id = 'apollo_test_participant_id'
        return self._construct_basic_reconfiguration_request(req)

    def _construct_reconfiguration_state_snapshot_read_as_of_req(self, snapshot_id, keys):
        req = cmf_msgs.StateSnapshotReadAsOfRequest()
        req.snapshot_id = snapshot_id
        req.participant_id = 'apollo_test_participant_id'
        req.keys = keys
        return self._construct_basic_reconfiguration_request(req)

    def _construct_reconfiguration_state_snapshot_req(self):
        req = cmf_msgs.StateSnapshotRequest()
        req.checkpoint_kv_count = 0
        req.participant_id = 'apollo_test_participant_id'
        return self._construct_basic_reconfiguration_request(req)

    def _construct_db_size_req(self):
        req = cmf_msgs.DbSizeReadRequest()
        req.sender_id = 1000
        return self._construct_basic_reconfiguration_request(req)

    def get_rsi_replies(self):
        return self.client.get_rsi_replies()

    async def wedge(self):
        seq_num = self.client.req_seq_num.next()
        with log.start_action(action_type="wedge", client=self.client.client_id, seq_num=seq_num):
            reconf_msg = self._construct_reconfiguration_wedge_command()
            return await self.client.write(reconf_msg.serialize(), reconfiguration=True, seq_num=seq_num)

    async def wedge_status(self, quorum=None, fullWedge=True):
        if quorum is None:
            quorum = bft_client.MofNQuorum.All(self.client.config, [r for r in range(self.config.n)])
        msg = self._construct_reconfiguration_wedge_status(fullWedge)
        return await self.client.read(msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)

    async def unwedge(self, bft=False, restart=False, quorum=None):
        if bft is True:
            quorum = bft_client.MofNQuorum.LinearizableQuorum(self.client.config, [r for r in range(self.config.n)])
        await self.can_unwedge(quorum=quorum, bft=bft)
        unwedges = []
        for r in self.client.get_rsi_replies().values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            unwedges.append((res.response.replica_id, res.response))
        if quorum is not None and quorum.required < self.config.n:
            bft = True
        msg = self._construct_reconfiguration_unwedge_command(unwedges, bft, restart)
        return await self.client.read(msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)

    async def can_unwedge(self, quorum=None, bft=False):
        if quorum is None:
            quorum = bft_client.MofNQuorum.All(
                self.client.config, [r for r in range(self.config.n)])
        msg = self._construct_reconfiguration_unwedge_status(bft)
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

    async def key_exchange(self, target_replicas, tls=False):
        reconf_msg = self._construct_reconfiguration_keMsg_command(target_replicas, tls)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)
    
    async def client_key_exchange_command(self, target_clients, tls=False):
        reconf_msg = self._construct_reconfiguration_clientKe_command(target_clients, tls)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)

    async def clients_addRemove_command(self, config_desc):
        token = []
        for cl in range(self.config.num_clients):
            token.append((cl, 'Token' + str(cl)))
        reconf_msg = self._construct_reconfiguration_clientsAddRemove_command(config_desc, token)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)

    async def clients_addRemoveStatus_command(self):
        reconf_msg = self._construct_reconfiguration_clientsAddRemoveStatus_command()
        return await self.client.read(reconf_msg.serialize(), reconfiguration=True)

    async def clients_clientKeyExchangeStatus_command(self, tls=False):
        reconf_msg = self._construct_reconfiguration_clientsKeyExchangeStatus_command(tls)
        return await self.client.read(reconf_msg.serialize(), reconfiguration=True)

    async def clients_clientRestart_command(self):
        reconf_msg = self._construct_reconfiguration_clientsRestart_command('ClientRestart')
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)
    
    async def clients_clientRestart_status(self):
        reconf_msg = self._construct_clients_clientRestart_status_command()
        return await self.client.read(reconf_msg.serialize(), reconfiguration=True)
    
    async def install_cmd(self, version, bft=True):
        install_msg = self._construct_install_command(version, bft)
        return await self.client.write(install_msg.serialize(), reconfiguration=True)

    async def add_remove(self, new_config):
        reconf_msg = self._construct_reconfiguration_addRemove_command(new_config)
        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)
    
    async def add_remove_with_wedge(self, new_config, bft=True, restart=True):
        token = []
        for r in range(self.config.n):
            token.append((r, 'Token' + str(r)))   
        reconf_msg = self._construct_reconfiguration_addRemoveWithWedge_command(new_config, token, bft, restart)

        return await self.client.write(reconf_msg.serialize(), reconfiguration=True)

    async def restart(self, data, bft=True, restart=True):
        with log.start_action(action_type="restart", bft=bft, restart=restart):
            reconf_msg = self._construct_reconfiguration_restart_command(bft, restart, data)
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

    async def get_dbcheckpoint_info_request(self, bft=False):
        if bft is True:
            quorum = bft_client.MofNQuorum.LinearizableQuorum(self.client.config, [r for r in range(self.config.n)])
        else:
            quorum = bft_client.MofNQuorum.All(self.client.config, [r for r in range(self.config.n)])
        cpinfo_msg = self._construct_reconfiguration_get_dbcheckpoint_info_request()
        return await self.client.read(cpinfo_msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)
    
    async def create_dbcheckpoint_cmd(self):
        cp = self._construct_reconfiguration_create_dbcheckpoint_command()
        return await self.client.write(cp.serialize(), reconfiguration=True)

    async def state_snapshot_req(self):
        req = self._construct_reconfiguration_state_snapshot_req()
        return await self.client.write(req.serialize(), reconfiguration=True)

    async def signed_public_state_hash_req(self, snapshot_id):
        req = self._construct_reconfiguration_signed_public_state_hash_req(snapshot_id)
        return await self.client.read(req.serialize(), m_of_n_quorum=bft_client.MofNQuorum.All(self.client.config,
                                        [r for r in range(self.config.n)]), reconfiguration=True)

    async def state_snapshot_read_as_of_req(self, snapshot_id, keys):
        req = self._construct_reconfiguration_state_snapshot_read_as_of_req(snapshot_id, keys)
        return await self.client.read(req.serialize(), m_of_n_quorum=bft_client.MofNQuorum.All(self.client.config,
                                        [r for r in range(self.config.n)]), reconfiguration=True)
                              
    async def get_db_size(self):
        quorum = bft_client.MofNQuorum.All(self.client.config, [r for r in range(self.config.n)])
        msg = self._construct_db_size_req()
        return await self.client.read(msg.serialize(), m_of_n_quorum=quorum, reconfiguration=True)
        
        