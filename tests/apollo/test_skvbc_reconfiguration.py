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
import unittest
import time
from shutil import copy2
from typing import Set, Optional, Callable

import trio
import difflib
import random

from util.test_base import ApolloTest, parameterize
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX, TestConfig
from util import operator
from util.object_store import ObjectStore, start_replica_cmd_prefix
import sys
from util import eliot_logging as log
import concord_msgs as cmf_msgs
from util import bft_network_partitioning as net

sys.path.append(os.path.abspath("../../util/pyclient"))

import bft_client
from bft_config import BFTConfig

def start_replica_cmd_with_object_store(builddir, replica_id, config):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    ret = start_replica_cmd_prefix(builddir, replica_id, config)
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        time_service_enabled = "1"
    else :
        time_service_enabled = "0"
    
    batch_size = "1"
    ret.extend(["-f", time_service_enabled, "-b", "2", "-q", batch_size, "-o", builddir + "/operator_pub.pem"])
    return ret

def start_replica_cmd_with_object_store_and_ke(builddir, replica_id, config):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    ret = start_replica_cmd_prefix(builddir, replica_id, config)
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        time_service_enabled = "1"
    else :
        time_service_enabled = "0"
        
    batch_size = "1"
    ret.extend(["-f", time_service_enabled, "-b", "2", "-q", batch_size, "-e", str(True), "-o", builddir + "/operator_pub.pem", "--publish-master-key-on-startup"])
    return ret

def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        time_service_enabled = "1"
    else :
        time_service_enabled = "0"
        
    batch_size = "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-o", builddir + "/operator_pub.pem",
            "--publish-master-key-on-startup"]


def start_replica_cmd_with_key_exchange(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    if os.environ.get('TIME_SERVICE_ENABLED', default="FALSE").lower() == "true" :
        time_service_enabled = "1"
    else :
        time_service_enabled = "0"
    
    batch_size = "1"
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-l", os.path.join(builddir, "tests", "simpleKVBC", "scripts", "logging.properties"),
            "-f", time_service_enabled,
            "-b", "2",
            "-q", batch_size,
            "-e", str(True),
            "-o", builddir + "/operator_pub.pem",
            "--publish-master-key-on-startup"]

class SkvbcReconfigurationTest(ApolloTest):
    @classmethod
    def setUpClass(cls):
        cls.object_store = ObjectStore()

    @classmethod
    def tearDownClass(cls):
        pass

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_clients_add_remove_cmd(self, bft_network):
        """
            sends a clientsAddRemoveCommand and test the the status for the CRE client is correct
        """
        bft_network.start_all_replicas()
        bft_network.start_cre()

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()

        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        config_desc = "123456789"
        rep = await op.clients_addRemove_command(config_desc)
        rep = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        assert rep.success is True

        with trio.fail_after(60):
            succ = False
            while not succ:
                rep = await op.clients_addRemoveStatus_command()
                rsi_rep = client.get_rsi_replies()
                data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
                if not data.success:
                    continue
                succ = True
                for r in rsi_rep.values():
                    res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                    if len(res[0].response.clients_status) == 0:
                        succ = False
                    for k,v in res[0].response.clients_status:
                        assert(k ==  bft_network.cre_id)
                        assert(v == config_desc)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_client_key_exchange_command(self, bft_network):
        """
            Operator sends client key exchange command for all the clients
        """
        with log.start_action(action_type="test_client_key_exchange_command"):
            bft_network.start_all_replicas()
            bft_network.start_cre()
            await bft_network.check_initial_master_key_publication(bft_network.all_replicas())
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            await self.run_client_key_exchange_cycle(bft_network)
            pub_key_a, ts_a = await self.get_last_client_keys_data(bft_network, bft_network.cre_id, tls=False)
            assert pub_key_a is not None
            assert ts_a is not None

            await self.run_client_key_exchange_cycle(bft_network, pub_key_a, ts_a)
            pub_key_b, ts_b = await self.get_last_client_keys_data(bft_network, bft_network.cre_id, tls=False)
            assert pub_key_b is not None
            assert ts_b is not None

            assert pub_key_a != pub_key_b
            assert ts_a < ts_b
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)

            for i in range(100):
                await skvbc.send_write_kv_set()

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_client_key_exchange_command_with_st(self, bft_network):
        """
            Operator sends client key exchange command for all the clients
        """
        with log.start_action(action_type="test_client_key_exchange_command_with_st"):
            crashed_replicas = {6}
            live_replicas = bft_network.all_replicas(without=crashed_replicas)
            bft_network.start_replicas(live_replicas)
            bft_network.start_cre()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            for i in range(100):
                await skvbc.send_write_kv_set()

            await self.run_client_key_exchange_cycle(bft_network)
            pub_key_a, ts_a = await self.get_last_client_keys_data(bft_network, bft_network.cre_id, tls=False)
            assert pub_key_a is not None
            assert ts_a is not None

            for i in range(350):
                await skvbc.send_write_kv_set()

            bft_network.start_replicas(crashed_replicas)
            await bft_network.wait_for_state_transfer_to_start()
            for r in crashed_replicas:
                await bft_network.wait_for_state_transfer_to_stop(0,
            r,
            stop_on_stable_seq_num=False)

    async def run_client_key_exchange_cycle(self, bft_network, prev_data="", prev_ts=0):
        key_before_exchange = None
        priv_key_path = os.path.join(bft_network.txn_signing_keys_base_path, "transaction_signing_keys", str(bft_network.principals_to_participant_map[bft_network.cre_id]), "transaction_signing_priv.pem")
        with open(priv_key_path) as orig_key:
            key_before_exchange = orig_key.read()
        await self.run_client_ke_command(bft_network, False, prev_data, prev_ts)
        with trio.fail_after(30):
            succ = False
            while not succ:
                await trio.sleep(1)
                succ = True
                new_key_text = None
                with open(priv_key_path) as new_key:
                    new_key_text = new_key.read()
                diff = difflib.unified_diff(key_before_exchange, new_key_text, fromfile="old", tofile="new", lineterm='')
                lines = sum(1 for l in diff)
                if lines == 0:
                    succ = False
        bft_network.stop_cre()
        bft_network.start_cre()
        bft_network.restart_clients(generate_tx_signing_keys=False, restart_replicas=False)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_tls_exchange_client_replica(self, bft_network):
        """
            Operator sends client key exchange command for cre client
        """
        with log.start_action(action_type="test_tls_exchange_client_replica"):
            bft_network.start_all_replicas()
            bft_network.start_cre()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            for i in range(100):
                await skvbc.send_write_kv_set()
            await self.run_client_tls_key_exchange_cycle(bft_network)
            await self.run_client_tls_key_exchange_cycle(bft_network)

            for i in range(100):
                await skvbc.send_write_kv_set()

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_tls_exchange_client_replica_with_st(self, bft_network):
        """
            Operator sends client key exchange command for cre client
        """
        with log.start_action(action_type="test_tls_exchange_client_replica_with_st"):
            bft_network.start_all_replicas()
            bft_network.start_cre()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            for i in range(100):
                await skvbc.send_write_kv_set()

            await skvbc.multiple_validate_last_exec_seq_num_for_all_replicas(30);

            initial_prim = 0
            next_primary = 1
            bft_network.stop_replica(next_primary)
            next_prime_cert = self.collect_client_certificates(bft_network, [next_primary])
            await self.run_client_tls_key_exchange_cycle(bft_network, list(bft_network.all_replicas(without={next_primary})) + [bft_network.cre_id])
            for i in range(500):
                await skvbc.send_write_kv_set()
            current_view = await bft_network.wait_for_view(0)
            # Let the next primary complete state transfer
            bft_network.start_replica(next_primary)
            await bft_network.wait_for_state_transfer_to_start()
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              next_primary,
                                                              stop_on_stable_seq_num=False)
            new_cert = self.collect_client_certificates(bft_network, [next_primary])
            diff = difflib.unified_diff(next_prime_cert[next_primary], new_cert[next_primary], fromfile="old", tofile="new", lineterm='')
            lines = sum(1 for l in diff)
            assert lines > 0
            # Now, stop the primary and wait for VC to happen
            bft_network.stop_replica(initial_prim)
            async with trio.open_nursery() as nursery:
                nursery.start_soon(skvbc.send_indefinite_ops, 1)
                with trio.fail_after(60):
                    while current_view != 1:
                        current_view = await bft_network.wait_for_view(1, expected=lambda v: v == 1)
                nursery.cancel_scope.cancel()

            # Now lets run another Client TLS key exchange and make sure the new primary is able
            # to communicate with the client after its state transfer
            await self.run_client_tls_key_exchange_cycle(bft_network, list(bft_network.all_replicas(without={initial_prim})) + [bft_network.cre_id])

            for i in range(100):
                await skvbc.send_write_kv_set()

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_client_restart_command(self, bft_network):
        """
            Operator sends client restart command for all the clients
        """
        with log.start_action(action_type="test_client_restart_command"):
            bft_network.start_all_replicas()
            bft_network.start_cre()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            for i in range(100):
                await skvbc.send_write_kv_set()
            ts = await self.get_last_client_restart_status(bft_network, bft_network.cre_id)
            assert ts == None
            client = bft_network.random_client()   
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            await op.clients_clientRestart_command()
            with trio.fail_after(30):
                while ts is None:
                    await trio.sleep(1)
                    ts = await self.get_last_client_restart_status(bft_network, bft_network.cre_id)
            assert ts != None
            

    async def run_client_ke_command(self, bft_network, tls, prev_data="", prev_ts=0):
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.client_key_exchange_command([bft_network.cre_id], tls=tls)
        rep = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        assert rep.success is True

    def collect_client_certificates(self, bft_network, targets):
        certs = {}
        for t in targets:
            client_cert_path = os.path.join(bft_network.certdir, str(t), str(bft_network.cre_id))
            if bft_network.use_unified_certs:
                client_cert_path = os.path.join(client_cert_path, "node.cert")
            else:
                client_cert_path = os.path.join(client_cert_path, "client", "client.cert")
            with open(client_cert_path) as orig_cert:
                orig_cert_text = orig_cert.read()
                certs[t] = orig_cert_text
        return certs

    async def run_client_tls_key_exchange_cycle(self, bft_network, effected = None, certs=None):
        effected = list(bft_network.all_replicas()) + [bft_network.cre_id] if effected is None else effected
        certs_in_effected = {}
        if certs is None:
            certs_in_effected = self.collect_client_certificates(bft_network, effected)
        else:
            certs_in_effected = certs
        await self.run_client_ke_command(bft_network, True)
        with trio.fail_after(60):
            client_cert_path = os.path.join(bft_network.certdir, str(bft_network.cre_id), str(bft_network.cre_id))
            if bft_network.use_unified_certs:
                client_cert_path = os.path.join(client_cert_path, "node.cert")
            else:
                client_cert_path = os.path.join(client_cert_path, "client", "client.cert")
            succ = False
            while not succ:
                succ = True
                await trio.sleep(1)
                with open(client_cert_path) as orig_cert:
                    orig_cert_text = orig_cert.read()
                    diff = difflib.unified_diff(certs_in_effected[bft_network.cre_id], orig_cert_text, fromfile=client_cert_path, tofile=client_cert_path, lineterm='')
                    lines = sum(1 for l in diff)
                    if lines == 0:
                        succ = False

        with trio.fail_after(60):
            succ = False
            while not succ:
                await trio.sleep(1)
                succ = True
                new_certs = self.collect_client_certificates(bft_network, effected)
                for p in effected:
                    diff = difflib.unified_diff(certs_in_effected[p], new_certs[p], fromfile="old", tofile="new", lineterm='')
                    lines = sum(1 for l in diff)
                    if lines == 0:
                        succ = False
                        break


    async def get_last_client_keys_data(self, bft_network, client_id, tls):
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.clients_clientKeyExchangeStatus_command(tls=tls)
        rsi_rep = client.get_rsi_replies()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        if not data.success:
            return None, None
        keys = None
        ts = None
        for r in rsi_rep.values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            if len(res[0].response.clients_data) == 0:
                return None, None
            for k,v in res[0].response.clients_data:
                if k != client_id:
                    continue
                if keys is None:
                    keys = v
                if keys != v:
                    return None, None # Not all live replicas have managed to complete the procedure yet
            for k,v in res[0].response.timestamps:
                if k != client_id:
                    continue
                if ts is None:
                    ts = v
                if ts != v:
                    return None, None # Not all live replicas have managed to complete the procedure yet
        return keys, ts

    async def get_last_client_restart_status(self, bft_network, client_id):
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)
        rep = await op.clients_clientRestart_status()
        rsi_rep = client.get_rsi_replies()
        data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        if not data.success:
            return None, None
        ts = None
        for r in rsi_rep.values():
            res = cmf_msgs.ReconfigurationResponse.deserialize(r)
            for k,v in res[0].response.timestamps:
                if k != client_id:
                    continue
                if ts is None:
                    ts = v
                if ts != v:
                    return None
        return ts

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_tls_exchange_replicas_replicas_and_replicas_client(self, bft_network):
        """
            Test that tls key exchange where primary is included and make sure CRE is also able to get the update
        """
        with log.start_action(action_type="test_tls_exchange_replicas_replicas_and_replicas_client"):
            bft_network.start_all_replicas()
            bft_network.start_cre()
            await bft_network.check_initial_master_key_publication(bft_network.all_replicas())
            await self.wait_until_cre_gets_replicas_master_keys(bft_network, bft_network.all_replicas())
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            initial_prim = 0
            for i in range(100):
                await skvbc.send_write_kv_set()
            fast_paths = {}
            for r in bft_network.all_replicas():
                nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
                fast_paths[r] = nb_fast_path
            exchanged_replicas = bft_network.all_replicas()
            await self.run_replica_tls_key_exchange_cycle(bft_network, exchanged_replicas, list(bft_network.all_replicas()) + [bft_network.cre_id])
            # Manually copy the new certs from the last replica of the replicas to clients and restart clients
            bft_network.copy_certs_from_server_to_clients(6)
            bft_network.restart_clients(False, False)
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            for i in range(100):
                await skvbc.send_write_kv_set()
            for r in bft_network.all_replicas():
                nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
                self.assertGreater(nb_fast_path, fast_paths[r])

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, with_cre=True, publish_master_keys=True)
    async def test_tls_exchange_replicas_replicas_and_replicas_clients_with_st(self, bft_network):
        """
            Operator sends client key exchange command to all replicas
        """
        with log.start_action(action_type="test_tls_exchange_replicas_replicas_and_replicas_client_with_st"):
            bft_network.start_all_replicas()
            bft_network.start_cre()
            await self.wait_until_cre_gets_replicas_master_keys(bft_network, bft_network.all_replicas())
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            initial_prim = 0
            crashed_replica = list(bft_network.random_set_of_replicas(1, {initial_prim}))
            exchanged_replicas = list(bft_network.all_replicas(without={crashed_replica[0]}))
            for i in range(100):
                await skvbc.send_write_kv_set()
            fast_paths = {}
            for r in bft_network.all_replicas():
                nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
                fast_paths[r] = nb_fast_path

            bft_network.stop_replicas(crashed_replica)

            live_replicas =  bft_network.all_replicas(without=set(crashed_replica))
            await self.run_replica_tls_key_exchange_cycle(bft_network, exchanged_replicas, live_replicas + [bft_network.cre_id])
            # Manually copy the new certs from one of the replicas to clients and restart clients
            max_live_replica = max(live_replicas)
            bft_network.copy_certs_from_server_to_clients(max_live_replica)
            bft_network.restart_clients(False, False)
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            for i in range(700):
                await skvbc.send_write_kv_set()

            bft_network.start_replicas(crashed_replica)
            await bft_network.wait_for_state_transfer_to_start()
            for r in crashed_replica:
                await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)

            for i in range(500):
                await skvbc.send_write_kv_set()
            for r in bft_network.all_replicas():
                nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
                self.assertGreater(nb_fast_path, fast_paths[r])

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store_and_ke, num_ro_replicas=1, rotate_keys=True,
                      selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_tls_exchange_replicas_replicas_with_ror(self, bft_network):
        """
            Run TLS key exchange and make sure a ror is able to get updates
        """
        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(301): # Produce 301 new blocks
            await skvbc.send_write_kv_set()
        # Now, lets switch keys to all replicas
        exchanged_replicas = bft_network.all_replicas()
        await self.run_replica_tls_key_exchange_cycle(bft_network, exchanged_replicas, affected_replicas=[r for r in range(bft_network.config.n + 1)])
        bft_network.copy_certs_from_server_to_clients(7)
        bft_network.restart_clients(False, False)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        # Make sure that the read only replica is able to complete another state transfer
        for i in range(500): # Produce 500 new blocks
            await skvbc.send_write_kv_set()
        await self._wait_for_st(bft_network, ro_replica_id, 600)



    async def run_replica_tls_key_exchange_cycle(self, bft_network, replicas, affected_replicas=[]):
        reps_data = {}
        if len(affected_replicas) == 0:
            affected_replicas = bft_network.all_replicas()
        for rep in affected_replicas:
            reps_data[rep] = {}
            for r in replicas:
                val = []
                cert_path = os.path.join(bft_network.certdir + "/" + str(rep), str(r))
                if bft_network.use_unified_certs:
                    cert_path = os.path.join(cert_path, "node.cert")
                else:
                    cert_path = os.path.join(cert_path, "server", "server.cert")

                with open(cert_path) as orig_key:
                    cert_text = orig_key.readlines()
                    val += [cert_text]
                cert_path = os.path.join(bft_network.certdir + "/" + str(rep), str(r))
                if bft_network.use_unified_certs:
                    cert_path = os.path.join(cert_path, "node.cert")
                else:
                    cert_path = os.path.join(cert_path, "client", "client.cert")
                with open(cert_path) as orig_key:
                    cert_text = orig_key.readlines()
                    val += [cert_text]
                reps_data[rep][r] = val
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client, bft_network.builddir)

        try:
            await op.key_exchange(replicas, tls=True)
        except trio.TooSlowError:
            # As we rotate the TLS keys immediately, the call may return with timeout
            pass
        with trio.fail_after(60):
            succ = False
            while not succ:
                await trio.sleep(1)
                succ = True
                for rep in affected_replicas:
                    for r in replicas:
                        cert_path = os.path.join(bft_network.certdir + "/" + str(rep), str(r))
                        if bft_network.use_unified_certs:
                            cert_path = os.path.join(cert_path, "node.cert")
                        else:
                            cert_path = os.path.join(cert_path, "server", "server.cert")
                        with open(cert_path) as orig_key:
                            cert_text = orig_key.readlines()
                            diff = difflib.unified_diff(reps_data[rep][r][0], cert_text, fromfile="new", tofile="old", lineterm='')
                            lines = sum(1 for l in diff)
                            if lines > 0:
                                continue
                        cert_path = os.path.join(bft_network.certdir + "/" + str(rep), str(r))
                        if bft_network.use_unified_certs:
                            cert_path = os.path.join(cert_path, "node.cert")
                        else:
                            cert_path = os.path.join(cert_path, "client", "client.cert")
                        with open(cert_path) as orig_key:
                            cert_text = orig_key.readlines()
                            diff = difflib.unified_diff(reps_data[rep][r][1], cert_text, fromfile="new", tofile="old", lineterm='')
                            lines = sum(1 for l in diff)
                            if lines > 0:
                                continue
                        succ = False
                        break

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_key_exchange(self, bft_network):
        """
            No initial key rotation
            Operator sends key exchange command to replica 0
            New keys for replica 0 should get effective at checkpoint 2, i.e. seqnum 300
        """
        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
         
        await self.send_and_check_key_exchange(target_replica=0, bft_network=bft_network, client=client)

        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=3, 
                                                 verify_checkpoint_persistency=False)

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_key_exchange, 
                      selected_configs=lambda n, f, c: n == 7,
                      rotate_keys=True, publish_master_keys=True)
    async def test_key_exchange_with_restart(self, bft_network):
        """
            - With initial key rotation (keys get effective at checkpoint 2)
            - Reach checkpoint 2 since key cannot be generated twice within a 2 checkpoints window
            - Operator sends key exchange command to replica 1 + validate execution
              (New keys for replica 1 should get effective at checkpoint 4, i.e. seqnum 600)
            - Reach checkpoint 4
            - Stop replica 1
            - Client sends 50 requests
            - Start replica 1
            - Reach checkpoint 6 and validate replica 1 is back on track
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        await self.send_and_check_key_exchange(target_replica=1, bft_network=bft_network, client=client)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        bft_network.stop_replica(1)
        
        for i in range(50):
            await skvbc.send_write_kv_set()
        key, val = await skvbc.send_write_kv_set()
        await skvbc.assert_kv_write_executed(key, val)
        
        bft_network.start_replica(1)
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(without={1}), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_key_exchange, 
                      bft_configs=[BFTConfig(n=4, f=1, c=0)],
                      rotate_keys=True, publish_master_keys=True)
    async def test_key_exchange_with_file_backup(self, bft_network):
        """
            - With initial key rotation (keys get effective at checkpoint 2)
            - Reach checkpoint 2 since key cannot be generated twice within a 2 checkpoints window
            - Send key exchange command to replica 1 + validate execution
              (New keys for replica 1 should get effective at checkpoint 4, i.e. seqnum 600)
            - Backup the generated private key file at replica 1.
            - Reach checkpoint 4
            - Send key exchange command to replica 1 + validate execution again
            - Stop replica 1
            - Restore backed up file
            - Start replica 1
              (Replica's 1 published public key now doesn't match its private key. If there's no assertion, replica 1
              will send invalid sugnatires and will be malicious)
            - Reach checkpoint 6 and validate all replicas except replica 1 are back on track.
        """

        bft_network.start_all_replicas()
        client = bft_network.random_client()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        await self.send_and_check_key_exchange(target_replica=1, bft_network=bft_network, client=client)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False)
        
        #backup gen-sec.1 file
        copy2(bft_network.testdir + "/gen-sec.1" , bft_network.testdir + "/gen-sec.1.bak")
        await self.send_and_check_key_exchange(target_replica=1, bft_network=bft_network, client=client)
        bft_network.stop_replica(1)
        # restore gen-sec.1 file
        copy2(bft_network.testdir + "/gen-sec.1.bak" , bft_network.testdir + "/gen-sec.1")
        bft_network.start_replica(1)
        
        await skvbc.fill_and_wait_for_checkpoint(initial_nodes=bft_network.all_replicas(without={1}), 
                                                 num_of_checkpoints_to_add=2, 
                                                 verify_checkpoint_persistency=False,
                                                 assert_state_transfer_not_started=False)
        
    async def send_and_check_key_exchange(self, target_replica, bft_network, client):
        with log.start_action(action_type="send_and_check_key_exchange",
                              target_replica=target_replica):
            sent_key_exchange_counter_before = 0
            self_key_exchange_counter_before = 0
            with trio.fail_after(seconds=15):
                try:
                    sent_key_exchange_counter_before = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "sent_key_exchange"])
                    self_key_exchange_counter_before = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                    # public_key_exchange_for_peer_counter_before = await bft_network.metrics.get(0, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                except:
                    log.log_message(message_type=f"Replica {target_replica} was unable to query KeyExchangeMetrics, assuming zero")
                    
            log.log_message(f"sending key exchange command to replica {target_replica}")
            op = operator.Operator(bft_network.config, client, bft_network.builddir)
            await op.key_exchange([target_replica])
            
            with trio.fail_after(seconds=15):
                while True:
                    with trio.move_on_after(seconds=2):
                        try:
                            sent_key_exchange_counter = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "sent_key_exchange"])
                            self_key_exchange_counter = await bft_network.metrics.get(target_replica, *["KeyExchangeManager", "Counters", "self_key_exchange"])
                            #public_key_exchange_for_peer_counter = await bft_network.metrics.get(0, *["KeyExchangeManager", "Counters", "public_key_exchange_for_peer"])
                            if  sent_key_exchange_counter == sent_key_exchange_counter_before or \
                                self_key_exchange_counter == self_key_exchange_counter_before:
                                continue
                        except (trio.TooSlowError, AssertionError) as e:
                            log.log_message(message_type=f"{e}: Replica {target_replica} was unable to query KeyExchangeMetrics")
                            raise KeyExchangeError
                        else:
                            assert sent_key_exchange_counter >= sent_key_exchange_counter_before + 1
                            #assert public_key_exchange_for_peer_counter ==  public_key_exchange_for_peer_counter_before + 1
                            break

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_wedge_command(self, bft_network):
        """
             Sends a wedge command and checks that the system stops processing new requests.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a wedge command
             2. The client verifies that the system reached a super stable checkpoint.
             3. The client tries to initiate a new write bft command and fails
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = min(bft_network.get_all_clients(), key=lambda client: client.client_id)
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.wedge()
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_unwedge_command(self, bft_network):
        """
             Sends a wedge command and checks that the system stops processing new requests.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a wedge command
             2. The client verifies that the system reached a super stable checkpoint.
             3. The client tries to initiate a new write bft command and fails
             4. The client sends an unwedge command
             5. The client sends a write request and then reads the written value
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(
            bft_network.config, client,  bft_network.builddir)
        await op.wedge()
        await self.verify_replicas_are_in_wedged_checkpoint(bft_network, checkpoint_before, range(bft_network.config.n))
        await self.verify_last_executed_seq_num(bft_network, checkpoint_before)
        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)
        await op.unwedge()
        await self.validate_start_on_unwedge(bft_network,skvbc,fullWedge=True)
        # To make sure that revocery doesn't remove the new epoch block, lets manually restart the replicas
        bft_network.stop_all_replicas()
        bft_network.start_all_replicas()
        protocol = kvbc.SimpleKVBCProtocol(bft_network)

        key = protocol.random_key()
        value = protocol.random_value()
        kv_pair = [(key, value)]
        await client.write(protocol.write_req([], kv_pair, 0))

        read_result = await client.read(protocol.read_req([key]))
        value_read = (protocol.parse_reply(read_result))[key]
        self.assertEqual(value, value_read, "A BFT Client failed to read a key-value pair from a "
                         "SimpleKVBC cluster matching the key-value pair it wrote "
                         "immediately prior to the read.")
        for r in bft_network.all_replicas():
            epoch = await bft_network.get_metric(r, bft_network, "Gauges", "epoch_number", component="epoch_manager")
            self.assertEqual(epoch, 1)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_wedge_command_with_state_transfer(self, bft_network):
        """
            This test checks that even a replica that received the super stable checkpoint via the state transfer mechanism
            is able to stop at the super stable checkpoint.
            The test does the following:
            1. Start all replicas but 1
            2. A client sends a wedge command
            3. Validate that all started replicas reached to the next next checkpoint
            4. Start the late replica
            5. Validate that the late replica completed the state transfer
            6. Validate that all replicas stopped at the super stable checkpoint and that new commands are not being processed
        """
        initial_prim = 0
        late_replicas = bft_network.random_set_of_replicas(1, {initial_prim})
        on_time_replicas = bft_network.all_replicas(without=late_replicas)
        bft_network.start_replicas(on_time_replicas)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.wait_for_liveness()


        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.wedge()
        await self.validate_stop_on_wedge_point(bft_network=bft_network, skvbc=skvbc, fullWedge=False)
        await self.try_to_unwedge(bft_network=bft_network, bft=True, restart=False)
        bft_network.start_replicas(late_replicas)
        await bft_network.wait_for_state_transfer_to_start()
        for r in late_replicas:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=True)
        for i in range(100):
            await skvbc.send_write_kv_set()

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_wedge_command_with_state_transfer_with_write(self, bft_network):
        """
            This test checks that a replica that received client writes via the state transfer mechanism (after wedge and unwedge commands)
            is able to take fast path on subsequent client writes.
            The test does the following:
            1. Start all replicas but 1
            2. A client sends a wedge command
            3. Validate that all started replicas reached to the next next checkpoint
            4. Perform client writes
            5. Start the late replica
            6. Validate that the late replica completed the state transfer
            7. Validate that subsequent writes are completed using fast paths
        """
        initial_primary_replica = 0
        late_replica = random.choice(bft_network.all_replicas(without={initial_primary_replica}))
        on_time_replicas = bft_network.all_replicas(without={late_replica})
        bft_network.start_replicas(on_time_replicas)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.wait_for_liveness()

        initial_fast_path_count = await bft_network.get_metric(initial_primary_replica, bft_network, "Counters", "totalFastPaths")
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.wedge()
        await self.validate_stop_on_wedge_point(bft_network=bft_network, skvbc=skvbc, fullWedge=False)
        await self.try_to_unwedge(bft_network=bft_network, bft=True, restart=False)

        for _ in range(300):
            await skvbc.send_write_kv_set()

        bft_network.start_replica(late_replica)
        await bft_network.wait_for_state_transfer_to_stop(initial_primary_replica,
                                                          late_replica,
                                                          stop_on_stable_seq_num=False)
        for _ in range(300):
            await skvbc.send_write_kv_set()

        final_fast_path_count = await bft_network.get_metric(initial_primary_replica, bft_network, "Counters", "totalFastPaths")
        self.assertGreater(final_fast_path_count, initial_fast_path_count)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_unwedge_command_with_state_transfer(self, bft_network):
        """
            This test checks that a replica that is stopped after wedge manages to catch up with the unwedged ones with ST
            The test does the following:
            1. Start all replicas 
            2. A client sends a wedge command
            3. Validate that all started replicas reached to the next next checkpoint
            4. Stop one replica
            5. The client sends a unwedge command
            6. Start the late replica
            7. Validate that the late replica completed the state transfer
        """
        initial_prim = 0
        on_time_replicas = bft_network.all_replicas()

        bft_network.start_replicas(on_time_replicas)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.wait_for_liveness()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        with log.start_action(action_type="send_wedge_cmd",
                              checkpoint_before=checkpoint_before):
            op = operator.Operator(
                bft_network.config, client,  bft_network.builddir)
            await op.wedge()

        await self.validate_stop_on_wedge_point(bft_network,skvbc)

        replicas_to_stop = bft_network.random_set_of_replicas(1, {
                                                              initial_prim})
        bft_network.stop_replicas(replicas_to_stop)

        with log.start_action(action_type="send_unwedge_cmd",
                              checkpoint_before=checkpoint_before,
                              stopped_replicas=list(replicas_to_stop)):
            op = operator.Operator(
                bft_network.config, client,  bft_network.builddir)
            await op.unwedge(bft=True)

        await self.validate_start_on_unwedge(bft_network,skvbc,fullWedge=False)

        # Make sure the system is able to make progress
        for i in range(500):
            await skvbc.send_write_kv_set()

        bft_network.start_replicas(replicas_to_stop)

        await bft_network.wait_for_state_transfer_to_start()
        for r in replicas_to_stop:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        await skvbc.wait_for_liveness()


    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_unwedge_command_with_f_failures(self, bft_network):
        """
            This test checks that unwedge is executed with f failures
            The test does the following:
            1. Start all replicas
            2. A client sends a wedge command
            3. Validate that all started replicas have reached the wedge point
            4. Stop f of the replicas
            5. Send unwedge command
            6. Verify that system is able to make progress
        """
        initial_prim = 0
        replicas_to_stop = bft_network.random_set_of_replicas(
            bft_network.config.f, {initial_prim})
        on_time_replicas = bft_network.all_replicas(without=replicas_to_stop)
        bft_network.start_replicas(bft_network.all_replicas())

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await skvbc.wait_for_liveness()

        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)

        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)
        with log.start_action(action_type="send_wedge_cmd",
                              checkpoint_before=checkpoint_before,
                              stopped_replicas=list(replicas_to_stop)):
            op = operator.Operator(
                bft_network.config, client,  bft_network.builddir)
            await op.wedge()

        await self.validate_stop_on_wedge_point(bft_network,skvbc, fullWedge= False)

        bft_network.stop_replicas(replicas_to_stop)

        with log.start_action(action_type="send_unwedge_cmd",
                              checkpoint_before=checkpoint_before,
                              stopped_replicas=list(replicas_to_stop)):
            op = operator.Operator(
                bft_network.config, client,  bft_network.builddir)
            await op.unwedge(bft=True)

        await self.validate_start_on_unwedge(bft_network,skvbc,fullWedge=False)

        # Make sure the system is able to make progress
        for i in range(100):
            await skvbc.send_write_kv_set()

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_wedge_status_command(self, bft_network):
        """
             Tests that the wedge command returns correct status after wedge and unwedge
             The test does the following:
             1. A client sends a wedge command
             2. The client sends a wedge_status command and verifies that all replicas are reported as wedged
             3. The client sends an unwedge command
             4. The client sends a wedge_status command and verifies that all replicas are reported as unwedged
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        # We increase the default request timeout because we need to have around 300 consensuses which occasionally may take more than 5 seconds
        client.config._replace(req_timeout_milli=10000)

        op = operator.Operator(
            bft_network.config, client,  bft_network.builddir)
        await op.wedge()

        await self.validate_stop_on_wedge_point(bft_network,skvbc, fullWedge= False)

        await self.validate_stop_on_super_stable_checkpoint(bft_network, skvbc)

        op = operator.Operator(
            bft_network.config, client,  bft_network.builddir)
        await op.unwedge()

        await self.validate_start_on_unwedge(bft_network,skvbc,fullWedge=False)


    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_get_latest_pruneable_block(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        # Create 100 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.send_write_kv_set()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the minimal latest pruneable block among all replicas
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.latest_pruneable_block()

        rsi_rep = client.get_rsi_replies()
        min_prunebale_block = 1000
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            if lpab.response.block_id < min_prunebale_block:
                min_prunebale_block = lpab.response.block_id

        # Create another 100 blocks
        k, v = await skvbc.send_write_kv_set()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the new minimal latest pruneable block
        await op.latest_pruneable_block()

        rsi_rep = client.get_rsi_replies()
        min_prunebale_block_b = 1000
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            if lpab.response.block_id < min_prunebale_block_b:
                min_prunebale_block_b = lpab.response.block_id
        assert min_prunebale_block < min_prunebale_block_b

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_pruning_command(self, bft_network):
        with log.start_action(action_type="test_pruning_command"):
            bft_network.start_all_replicas()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            client = bft_network.random_client()

            # Create 100 blocks in total, including the genesis block we have 101 blocks
            k, v = await skvbc.send_write_kv_set()
            for i in range(99):
                v = skvbc.random_value()
                await client.write(skvbc.write_req([], [(k, v)], 0))

            # Get the minimal latest pruneable block among all replicas
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            await op.latest_pruneable_block()

            latest_pruneable_blocks = []
            rsi_rep = client.get_rsi_replies()
            for r in rsi_rep.values():
                lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
                latest_pruneable_blocks += [lpab.response]

            rep = await op.prune(latest_pruneable_blocks)
            data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
            pruned_block = int(data.additional_data.decode('utf-8'))
            log.log_message(message_type=f"pruned_block {pruned_block}")
            assert pruned_block <= 97

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_pruning_command_with_failures(self, bft_network):
        with log.start_action(action_type="test_pruning_command_with_faliures"):
            bft_network.start_all_replicas()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            client = bft_network.random_client()

            # Create 100 blocks in total, including the genesis block we have 101 blocks
            k, v = await skvbc.send_write_kv_set()
            for i in range(99):
                v = skvbc.random_value()
                await client.write(skvbc.write_req([], [(k, v)], 0))

            # Get the minimal latest pruneable block among all replicas
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            await op.latest_pruneable_block()

            latest_pruneable_blocks = []
            rsi_rep = client.get_rsi_replies()
            for r in rsi_rep.values():
                lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
                latest_pruneable_blocks += [lpab.response]

            # Now, crash one of the non-primary replicas
            crashed_replica = 3
            bft_network.stop_replica(crashed_replica)
            
            rep = await op.prune(latest_pruneable_blocks)
            data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
            pruned_block = int(data.additional_data.decode('utf-8'))
            log.log_message(message_type=f"pruned_block {pruned_block}")
            assert pruned_block <= 97

            # creates 1000 new blocks
            for i in range(1000):
                v = skvbc.random_value()
                await client.write(skvbc.write_req([], [(k, v)], 0))

            # Now, restart the crashed replica and wait for it to finish state transfer.
            # The total number of seq numbers is 1101 (1000 + 100 writes + 1 prune request).
            # This translates to 7 checkpoints and, therefore, 7 * 150 = 1050.
            bft_network.start_replica(crashed_replica)
            await self._wait_for_st(bft_network, crashed_replica, 1050)

            # We expect the late replica to catch up with the state and to perform pruning
            with trio.fail_after(seconds=30):
                while True:
                    num_replies = 0
                    await op.prune_status()
                    rsi_rep = client.get_rsi_replies()
                    for r in rsi_rep.values():
                        status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
                        last_prune_blockid = status.response.last_pruned_block
                        log.log_message(message_type=f"last_prune_blockid {last_prune_blockid}, status.response.sender {status.response.sender}")
                        if status.response.in_progress is False and last_prune_blockid <= 97 and last_prune_blockid > 0:
                            num_replies += 1
                    if num_replies == bft_network.config.n:
                        break
            # Validate the crashed replica has managed to updates its metrics
            with trio.fail_after(60):
                while True:
                    with trio.move_on_after(seconds=.5):
                        try:
                            key = ['kv_blockchain_deletes', 'Counters', 'numOfVersionedKeysDeleted']
                            deletes = await bft_network.metrics.get(crashed_replica, *key)

                            key = ['kv_blockchain_deletes', 'Counters', 'numOfImmutableKeysDeleted']
                            deletes += await bft_network.metrics.get(crashed_replica, *key)

                            key = ['kv_blockchain_deletes', 'Counters', 'numOfMerkleKeysDeleted']
                            deletes += await bft_network.metrics.get(crashed_replica, *key)

                        except KeyError:#might be using the v4 storage
                            try:
                                key = ['v4_blockchain', 'Gauges', 'numOfBlocksDeleted']
                                deletes = await bft_network.metrics.get(crashed_replica, *key)
                                log.log_message(message_type=f"pruning_merics {key}, count {deletes}")
                            except KeyError:
                                continue
                            else:
                                # success!
                                if deletes >= 0:
                                    break
                        else:
                            # success!
                            if deletes >= 0:
                                break
            # Now, crash the same replica again.
            crashed_replica = 3
            bft_network.stop_replica(crashed_replica)

            # Execute 1000 writes.
            for i in range(1000):
              v = skvbc.random_value()
              await client.write(skvbc.write_req([], [(k, v)], 0))

            # Make sure ST completes again. Wait for 2100 = 14 checkpoints * 150 seq numbers.
            bft_network.start_replica(crashed_replica)
            await self._wait_for_st(bft_network, crashed_replica, 2100)
    
    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_pruning_status_command(self, bft_network):

        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.prune_status()

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block == 0

        # Create 100 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.send_write_kv_set()
        for i in range(99):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Get the minimal latest pruneable block among all replicas
        await op.latest_pruneable_block()

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        await op.prune(latest_pruneable_blocks)

        # Verify the system is able to get new write requests (which means that pruning has done)
        with trio.fail_after(30):
            await skvbc.send_write_kv_set()

        await op.prune_status()

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block <= 97

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store, num_ro_replicas=1, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_pruning_with_ro_replica(self, bft_network):

        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)

        # Create more than 150 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.send_write_kv_set()
        for i in range(200):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Wait for the read only replica to catch with the state
        await self._wait_for_st(bft_network, ro_replica_id, 150)

        # Get the minimal latest pruneable block among all replicas
        await op.latest_pruneable_block()

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        await op.prune(latest_pruneable_blocks)

        # Verify the system is able to get new write requests (which means that pruning has done)
        with trio.fail_after(30):
            await skvbc.send_write_kv_set()

        await op.prune_status()

        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.in_progress is False
            assert status.response.last_pruned_block == 150


    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store, num_ro_replicas=1, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_pruning_with_ro_replica_failure(self, bft_network):

        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)

        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()

        op = operator.Operator(bft_network.config, client,  bft_network.builddir)

        # Create more than 150 blocks in total, including the genesis block we have 101 blocks
        k, v = await skvbc.send_write_kv_set()
        for i in range(200):
            v = skvbc.random_value()
            await client.write(skvbc.write_req([], [(k, v)], 0))

        # Wait for the read only replica to catch with the state
        await self._wait_for_st(bft_network, ro_replica_id, 150)

        # Get the minimal latest pruneable block among all replicas
        await op.latest_pruneable_block()

        latest_pruneable_blocks = []
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            lpab = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            latest_pruneable_blocks += [lpab.response]

        # Remove the read only latest pruneable block from the list
        for m in latest_pruneable_blocks:
            if m.replica >= bft_network.config.n:
                latest_pruneable_blocks.remove(m)

        assert len(latest_pruneable_blocks) == bft_network.config.n

        # Now, issue a prune request. we expect to receive an error as the read only latest prunebale block is missing
        rep = await op.prune(latest_pruneable_blocks)
        rep = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
        assert rep.success is False

    async def send_restart_with_params(self, bft_network: 'BftTestNetwork', bft: bool, restart: bool,
                                       faulty_replica_ids: Optional[Set[int]] = None,
                                       post_restart: Optional[Callable] = None,
                                       pre_restart_epoch_id: int = 0,
                                       kv_count: int = 10):
        """
        Send a restart command and verify that replicas have stopped and removed their metadata in two cases
        @param bft_network:
        @param bft: flag for restart command
        @param restart: flag for restart command
        @param faulty_replica_ids:
        @param post_restart: A coroutine which is called after the restart operation is received by the network
        @param pre_restart_epoch_id: The epoch id before the restart operation
        @param kv_count: kvs to send for liveness checks
        """

        if faulty_replica_ids is None:
            faulty_replica_ids = set()

        live_replicas = bft_network.all_replicas(without=faulty_replica_ids)
        bft_network.start_replicas(live_replicas)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        await self.validate_epoch_number(bft_network, pre_restart_epoch_id, live_replicas)
        await skvbc.send_n_kvs_sequentially(kv_count)
        op = operator.Operator(bft_network.config, bft_network.random_client(), bft_network.builddir)
        await op.restart(f"test_restart", bft=bft, restart=restart)
        if post_restart:
            await post_restart(skvbc)
        await self.validate_epoch_number(bft_network, pre_restart_epoch_id + 1, live_replicas)
        await skvbc.send_n_kvs_sequentially(kv_count)

    @with_trio
    @parameterize(bft=[True, False])
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_restart_no_restart_flag(self, bft_network, bft):
        async def post_restart(skvbc):
            await self.validate_stop_on_wedge_point(skvbc.bft_network, skvbc, fullWedge=True)
            bft_network.stop_all_replicas()
            bft_network.start_all_replicas()

        await self.send_restart_with_params(bft_network, bft=bft, restart=False, post_restart=post_restart)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_restart_no_bft_with_restart_flag(self, bft_network):
        await self.send_restart_with_params(bft_network, bft=False, restart=True)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_restart_with_bft_with_restart_flag(self, bft_network):
        bft_config = bft_network.config
        assert bft_config.f >= 2, f"Insufficient fault tolerance factory {bft_config.f}"
        replica_count = bft_config.n
        # For simplicity, we crash the last two replicas
        crashed_replicas = set(range(replica_count - 2, replica_count))
        await self.send_restart_with_params(bft_network, bft=True, restart=True, faulty_replica_ids=crashed_replicas)

    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True, publish_master_keys=True)
    async def test_remove_nodes(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a remove command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Load  a new configuration to the bft network
             4. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
             In addition, we verify the correct epoch number on each epoch change
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        key, val = await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        stopped_replicas = {4, 5, 6}
        conf = TestConfig(n=4,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_key_exchange,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        await op.add_remove_with_wedge(test_config, bft=False)
        await self.validate_epoch_number(bft_network, 1, bft_network.all_replicas())
        bft_network.stop_replicas(stopped_replicas) # to have clean logs
        bft_network.restart_clients()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True, publish_master_keys=True)
    async def test_remove_nodes_with_f_failures(self, bft_network):
        """
        In this test we show how a system operator can remove nodes (and thus reduce the cluster) from 7 nodes cluster
        to 4 nodes cluster even when f nodes are not responding
        For that the operator performs the following steps:
        1. Stop 2 nodes (f=2)
        2. Send a remove_node command - this command also wedges the system
        3. Verify that all live nodes have stopped
        4. Load  a new configuration to the bft network
        5. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
        In addition, we verify the correct epoch number on each epoch change
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        for i in range(100):
            await skvbc.send_write_kv_set()
        # choose two replicas to crash and crash them
        crashed_replicas = {5, 6} # For simplicity, we crash the last two replicas
        bft_network.stop_replicas(crashed_replicas)
        live_replicas = bft_network.all_replicas(without=crashed_replicas)

        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        conf = TestConfig(n=4,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_key_exchange,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        await op.add_remove_with_wedge(test_config, bft=True, restart=True)
        await self.validate_epoch_number(bft_network, 1, bft_network.all_replicas())
        bft_network.stop_replica(4, force_kill=True) # to have clean logs
        bft_network.restart_clients()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_remove_nodes_with_unwedge(self, bft_network):
        """
            Sends a addRemove command and checks that new configuration is written to blockchain.
            Note that in this test we assume no failures and synchronized network.
            The test does the following:
            1. A client sends a remove command which will also wedge the system on next next checkpoint
            2. Validate that all replicas have stopped
            3. Load  a new configuration to the bft network
            4. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
            In addition, we verify the correct epoch number on each epoch change
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        key, val = await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_4_f_1_c_0'
        stopped_replicas = {4, 5, 6}
        conf = TestConfig(n=4,
                          f=1,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_key_exchange,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf)
        await op.add_remove_with_wedge(test_config, bft=False, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        await op.unwedge(restart=True)
        bft_network.stop_replicas(stopped_replicas) # to have clean logs
        bft_network.restart_clients()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
        await self.validate_epoch_number(bft_network, 1, bft_network.all_replicas())

    @with_trio
    @with_bft_network(start_replica_cmd, bft_configs=[BFTConfig(n=4, f=1, c=0)], publish_master_keys=True)
    async def test_add_nodes(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a add node command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Load a new configuration to the bft network
             4. Copy the db files to the new replicas
             5. Rerun the cluster with only new configuration and make sure they succeed to perform transactions in fast path
             In addition, we verify the correct epoch number on each epoch change
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_n_7_f_2_c_0'
        await op.add_remove_with_wedge(test_config, bft=False, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        new_replicas = {4, 5, 6}
        source = list(bft_network.random_set_of_replicas(1)).pop()
        bft_network.stop_all_replicas()
        bft_network.transfer_db_files(source, new_replicas)
        conf = TestConfig(n=7,
                          f=2,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd,
                          stop_replica_cmd=None,
                          num_ro_replicas=0)
        await bft_network.change_configuration(conf, generate_tls=True)
        bft_network.restart_clients()
        await self.validate_epoch_number(bft_network, 1, bft_network.all_replicas())
        for i in range(100):
            await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)

    @with_trio
    @with_bft_network(start_replica_cmd, bft_configs=[BFTConfig(n=4, f=1, c=0)], publish_master_keys=True)
    async def test_add_nodes_with_failures(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             We add nodes to 4 nodes cluster in phases to make it a 7 node cluster even when f nodes are not responding
             The test does the following:
             1. Stop one node and send a add node command which will also wedge the system on next next checkpoint
             2. Verify that all live nodes have stopped
             3. Load a new configuration to the bft network
             4. Add node is done in phases, (n=4,f=1,c=0)->(n=6,f=1,c=0)->(n=7,f=2,c=0)
                Note: For new replicas to catch up with exiting replicas through ST, existing replicas must
                      move the checkpoint window, that means for n=7 configuration, there must be 5 non-faulty
                      replicas to move the checkpoint window, hence new replicas are added in two phases
             5. Rerun the cluster with only new configuration and make sure they succeed to perform transactions in fast path
             In addition, we verify the correct epoch number on each epoch change
         """
        with log.start_action(action_type="test_add_nodes_with_failures"):
            initial_prim = 0
            crashed_replica = bft_network.random_set_of_replicas(1, {initial_prim})
            log.log_message(message_type=f"crashed replica is {crashed_replica}")
            bft_network.start_all_replicas()
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            await bft_network.check_initial_master_key_publication(bft_network.all_replicas())
            for i in range(100):
                await skvbc.send_write_kv_set()
            # This is a loop to make sure that all replicas are up before interfering them
            with trio.fail_after(30):
                nb_fast_path = await bft_network.get_metric(initial_prim, bft_network, "Counters", "totalFastPaths")
                while nb_fast_path <= 0:
                    for i in range(100):
                        await skvbc.send_write_kv_set()
                    nb_fast_path = await bft_network.get_metric(initial_prim, bft_network, "Counters", "totalFastPaths")
            with net.ReplicaSubsetIsolatingAdversary(bft_network, crashed_replica) as adversary:
                adversary.interfere()

                for i in range(601):
                    await skvbc.send_write_kv_set()
                client = bft_network.random_client()
                client.config._replace(req_timeout_milli=10000)
                op = operator.Operator(bft_network.config, client,  bft_network.builddir)
                test_config = 'new_configuration_n_7_f_2_c_0'
                await op.add_remove_with_wedge(test_config, bft=True, restart=False)
            new_replicas = {4, 5, 6}
            replicas_for_st = {6}
            source = list(bft_network.random_set_of_replicas(1, without=crashed_replica)).pop()
            await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=False)
            await bft_network.wait_for_state_transfer_to_start()

            # Lets wait until the late replica gets the new configuration
            with trio.fail_after(30):
                for r in crashed_replica:
                    while True:
                        await trio.sleep(1)
                        configurations_file = os.path.join(bft_network.testdir, "configurations." + str(r))
                        if not os.path.isfile(configurations_file):
                            continue
                        with open(configurations_file, "r") as file:
                            done = False
                            for l in file.readlines():
                                if test_config in l:
                                    done = True
                            if done:
                                break

            bft_network.stop_all_replicas()
            bft_network.transfer_db_files(source, new_replicas - replicas_for_st)

            # Change the cluster's configuration
            conf = TestConfig(n=7,
                              f=2,
                              c=0,
                              num_clients=10,
                              key_file_prefix=KEY_FILE_PREFIX,
                              start_replica_cmd=start_replica_cmd_with_key_exchange,
                              stop_replica_cmd=None,
                              num_ro_replicas=0)
            await bft_network.change_configuration(conf, generate_tls=True)
            bft_network.restart_clients(generate_tx_signing_keys=True, restart_replicas=False)

            live_replicas = bft_network.all_replicas(without=replicas_for_st.union(crashed_replica))
            await bft_network.check_initial_key_exchange(stop_replicas=False, full_key_exchange=False, replicas_to_start=live_replicas)
            for i in range(601):
                await skvbc.send_write_kv_set()
            bft_network.start_replicas(crashed_replica)
            await self.validate_initial_key_exchange(bft_network, crashed_replica, metrics_id="self_key_exchange", expected=0)
            for r in crashed_replica:
                await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                                  r,
                                                                  stop_on_stable_seq_num=False)
            live_replicas = bft_network.all_replicas(without=replicas_for_st)
            await self.validate_epoch_number(bft_network, 1, live_replicas)
            await self.validate_initial_key_exchange(bft_network, crashed_replica, metrics_id="self_key_exchange", expected=1)
            bft_network.start_replicas(replicas_for_st)
            await self.validate_initial_key_exchange(bft_network, replicas_for_st, metrics_id="self_key_exchange", expected=0)
            for i in range(601):
                await skvbc.send_write_kv_set()

            await bft_network.wait_for_state_transfer_to_start()
            for r in replicas_for_st:
                await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                                  r,
                                                                  stop_on_stable_seq_num=False)

            await self.validate_epoch_number(bft_network, 1, bft_network.all_replicas())
            await self.validate_initial_key_exchange(bft_network, replicas_for_st, metrics_id="self_key_exchange", expected=1)
            for i in range(300):
                await skvbc.send_write_kv_set()
            for r in bft_network.all_replicas():
                nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
                self.assertGreater(nb_fast_path, 0)
            await self.validate_initial_key_exchange(bft_network, bft_network.all_replicas(), metrics_id="self_key_exchange", expected=bft_network.config.n)



    @with_trio
    @with_bft_network(start_replica_cmd, selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_addRemoveStatusError(self, bft_network):
        """
             Sends a addRemoveStatus command without adding new configuration 
             and checks that replicas respond with valid error message.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a addRemoveStatus command
             2. The client verifies status returns a valid error message
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        checkpoint_before = await bft_network.wait_for_checkpoint(replica_id=0)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.add_remove_status()
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.error_msg == 'key_not_found'
            assert status.success is False

    @with_trio
    @with_bft_network(start_replica_cmd=start_replica_cmd_with_object_store_and_ke, num_ro_replicas=1, rotate_keys=True,
                      selected_configs=lambda n, f, c: n == 7, publish_master_keys=True)
    async def test_reconfiguration_with_ror(self, bft_network):
        """
             Sends a addRemove command and checks that new configuration is written to blockchain.
             Note that in this test we assume no failures and synchronized network.
             The test does the following:
             1. A client sends a reconfiguration command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Wait for read only replica to done with state transfer
             3. Load  a new configuration to the bft network
             4. Rerun the cluster with only 4 nodes and make sure they succeed to perform transactions in fast path
             5. Make sure the read only replica is able to catch up with the new state
         """
        bft_network.start_all_replicas()
        ro_replica_id = bft_network.config.n
        bft_network.start_replica(ro_replica_id)
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100): # Produce 149 new blocks
            await skvbc.send_write_kv_set()
        key, val = await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        test_config = 'new_configuration_1'
        await op.add_remove_with_wedge(test_config, bft=False, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        blockIdreplica = await bft_network.get_metric(0, bft_network, "Gauges",
                                            "reconfiguration_cmd_blockid", component="reconfiguration_cmd_blockid")
        await self._wait_for_st(bft_network, ro_replica_id, 300)
        blockIdRor = await bft_network.get_metric(ro_replica_id, bft_network, "Gauges",
                                            "last_known_block", component="client_reconfiguration_engine")
        self.assertEqual(blockIdRor, blockIdreplica)

        bft_network.stop_all_replicas()
        # We now expect the replicas to start with a fresh new configuration
        # Metadata is erased on replicas startup
        conf = TestConfig(n=7,
                          f=2,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_object_store_and_ke,
                          stop_replica_cmd=None,
                          num_ro_replicas=1)
        await bft_network.change_configuration(conf)
        ro_replica_id = bft_network.config.n
        await bft_network.check_initial_key_exchange(stop_replicas=False)
        bft_network.start_replica(ro_replica_id)
        for r in bft_network.all_replicas():
            last_stable_checkpoint = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
            self.assertEqual(last_stable_checkpoint, 0)
        await self.validate_state_consistency(skvbc, key, val)
        for i in range(150):
            await skvbc.send_write_kv_set()
        key, val = await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
        await self._wait_for_st(bft_network, ro_replica_id, 150)
        blockIdRor = await bft_network.get_metric(ro_replica_id, bft_network, "Gauges",
                                            "reconfiguration_cmd_blockid", component="reconfiguration_cmd_blockid")
        #validate the reconfiguration blockId is same as before loading this new config
        self.assertEqual(blockIdRor, blockIdreplica)
        bft_network.stop_replica(ro_replica_id)
        test_config = 'new_configuration_2'
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.add_remove_with_wedge(test_config, bft=False, restart=False)
        await self.validate_stop_on_wedge_point(bft_network, skvbc, fullWedge=True)
        blockIdreplica = await bft_network.get_metric(0, bft_network, "Gauges",
                                            "reconfiguration_cmd_blockid", component="reconfiguration_cmd_blockid")
        bft_network.stop_all_replicas()
        conf = TestConfig(n=7,
                          f=2,
                          c=0,
                          num_clients=10,
                          key_file_prefix=KEY_FILE_PREFIX,
                          start_replica_cmd=start_replica_cmd_with_object_store_and_ke,
                          stop_replica_cmd=None,
                          num_ro_replicas=1)
        await bft_network.change_configuration(conf)
        ro_replica_id = bft_network.config.n
        await bft_network.check_initial_key_exchange(stop_replicas=False)
        for r in bft_network.all_replicas():
            last_stable_checkpoint = await bft_network.get_metric(r, bft_network, "Gauges", "lastStableSeqNum")
            self.assertEqual(last_stable_checkpoint, 0)
        await self.validate_state_consistency(skvbc, key, val)
        for i in range(150):
            await skvbc.send_write_kv_set()
        for r in bft_network.all_replicas():
            nb_fast_path = await bft_network.get_metric(r, bft_network, "Counters", "totalFastPaths")
            self.assertGreater(nb_fast_path, 0)
        bft_network.start_replica(ro_replica_id)
        await self._wait_for_st(bft_network, ro_replica_id, 150)
        blockIdRor = await bft_network.get_metric(ro_replica_id, bft_network, "Gauges",
                                            "last_known_block", component="client_reconfiguration_engine")
        #validate the blockId is same as the 2nd reconfiguration command block
        self.assertEqual(blockIdRor, blockIdreplica)

    @with_trio
    @with_bft_network(start_replica_cmd_with_key_exchange, selected_configs=lambda n, f, c: n == 7, rotate_keys=True, publish_master_keys=True)
    async def test_install_command(self, bft_network):
        """
             Sends a install command and checks that new version is written to blockchain.
             The test does the following:
             1. A client sends a install command which will also wedge the system on next next checkpoint
             2. Validate that all replicas have stopped
             3. Validate that replicas get restart proof message for the install command
         """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        for i in range(100):
            await skvbc.send_write_kv_set()
        key, val = await skvbc.send_write_kv_set()
        client = bft_network.random_client()
        client.config._replace(req_timeout_milli=10000)
        version = 'version1'
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.install_cmd(version, bft=False)
        await self.validate_stop_on_wedge_point(bft_network=bft_network, skvbc=skvbc, fullWedge=True)
        await self.verify_restart_ready_proof_msg(bft_network, bft=False)
        await self.try_to_unwedge(bft_network=bft_network, bft=False, restart=True)
        for i in range(100):
            await skvbc.send_write_kv_set()
        initial_prim = 0
        crashed_replica = bft_network.random_set_of_replicas(1, {initial_prim})
        bft_network.stop_replicas(crashed_replica)
        for i in range(151):
            await skvbc.send_write_kv_set()
        version = 'version2'
        await op.install_cmd(version, bft=True)
        await self.validate_stop_on_wedge_point(bft_network=bft_network, skvbc=skvbc, fullWedge=False)
        await self.verify_restart_ready_proof_msg(bft_network, bft=True)  
        bft_network.start_replicas(crashed_replica)
        await bft_network.wait_for_state_transfer_to_start()
        for r in crashed_replica:
            await bft_network.wait_for_state_transfer_to_stop(initial_prim,
                                                              r,
                                                              stop_on_stable_seq_num=False)
        await self.validate_stop_on_wedge_point(bft_network=bft_network, skvbc=skvbc, fullWedge=True)
        await self.try_to_unwedge(bft_network=bft_network, bft=False, restart=False)
        for i in range(100):
            await skvbc.send_write_kv_set()
    
    async def try_to_unwedge(self, bft_network, bft, restart, quorum=None):
        with trio.fail_after(seconds=60):
            client = bft_network.random_client()
            client.config._replace(req_timeout_milli=10000)
            op = operator.Operator(bft_network.config, client,  bft_network.builddir)
            while True:
                try:
                    rep = await op.unwedge(restart=restart, bft=bft, quorum=quorum)
                    data = cmf_msgs.ReconfigurationResponse.deserialize(rep)[0]
                    if data.success:
                        break
                except trio.TooSlowError:
                    pass


    async def validate_stop_on_wedge_point(self, bft_network, skvbc, fullWedge=False):
        with log.start_action(action_type="validate_stop_on_stable_checkpoint") as action:
            with trio.fail_after(seconds=90):
                client = bft_network.random_client()
                client.config._replace(req_timeout_milli=10000)
                op = operator.Operator(bft_network.config, client,  bft_network.builddir)
                done = False
                quorum = None if fullWedge is True else bft_client.MofNQuorum.LinearizableQuorum(bft_network.config, [r.id for r in bft_network.replicas])
                while done is False:
                    stopped_replicas = 0
                    await op.wedge_status(quorum=quorum, fullWedge=fullWedge)
                    rsi_rep = client.get_rsi_replies()
                    done = True
                    for r in rsi_rep.values():
                        res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                        status = res[0].response.stopped
                        if status:
                            stopped_replicas += 1
                    stop_condition = bft_network.config.n if fullWedge is True else (bft_network.config.n - bft_network.config.f)
                    if stopped_replicas < stop_condition:
                        done = False
                with log.start_action(action_type='expect_kv_failure_due_to_wedge'):
                    with self.assertRaises(trio.TooSlowError):
                        await skvbc.send_write_kv_set()

    async def validate_start_on_unwedge(self, bft_network, skvbc, fullWedge=False):
        with log.start_action(action_type="validate_start_on_unwedge") as action:
            with trio.fail_after(seconds=90):
                client = bft_network.random_client()
                client.config._replace(req_timeout_milli=10000)
                op = operator.Operator(bft_network.config, client,  bft_network.builddir)
                done = False
                quorum = None if fullWedge is True else bft_client.MofNQuorum.LinearizableQuorum(bft_network.config, [r.id for r in bft_network.replicas])
                while done is False:
                    started_replicas = 0
                    await op.wedge_status(quorum=quorum, fullWedge=fullWedge)
                    rsi_rep = client.get_rsi_replies()
                    done = True
                    for r in rsi_rep.values():
                        res = cmf_msgs.ReconfigurationResponse.deserialize(r)
                        status = res[0].response.stopped
                        if not status:
                            started_replicas += 1
                    stop_condition = bft_network.config.n if fullWedge is True else (bft_network.config.n - bft_network.config.f)
                    if started_replicas < stop_condition:
                        done = False


    async def validate_stop_on_super_stable_checkpoint(self, bft_network, skvbc):
          with log.start_action(action_type="validate_stop_on_super_stable_checkpoint") as action:
            with trio.fail_after(seconds=120):
                for replica_id in range(bft_network.config.n):
                    while True:
                        with trio.move_on_after(seconds=1):
                            try:
                                key = ['replica', 'Gauges', 'OnCallBackOfSuperStableCP']
                                value = await bft_network.metrics.get(replica_id, *key)
                                if value == 0:
                                    action.log(message_type=f"Replica {replica_id} has not reached super stable checkpoint yet")
                                    await trio.sleep(0.5)
                                    continue
                            except trio.TooSlowError:
                                action.log(message_type=
                                    f"Replica {replica_id} was not able to get super stable checkpoint metric within the timeout")
                                raise
                            else:
                                self.assertEqual(value, 1)
                                action.log(message_type=f"Replica {replica_id} has reached super stable checkpoint")
                                break
                with log.start_action(action_type='expect_kv_failure_due_to_wedge'):
                    with self.assertRaises(trio.TooSlowError):
                        await skvbc.send_write_kv_set()


    async def verify_replicas_are_in_wedged_checkpoint(self, bft_network, previous_checkpoint, replicas):
        with log.start_action(action_type="verify_replicas_are_in_wedged_checkpoint", previous_checkpoint=previous_checkpoint):
            for replica_id in replicas:
                with log.start_action(action_type="verify_replica", replica=replica_id):
                    with trio.fail_after(seconds=60):
                        while True:
                            with trio.move_on_after(seconds=1):
                                checkpoint_after = await bft_network.wait_for_checkpoint(replica_id=replica_id)
                                if checkpoint_after == previous_checkpoint + 2:
                                    break
                                else:
                                    await trio.sleep(1)

    async def verify_last_executed_seq_num(self, bft_network, previous_checkpoint):
        expectedSeqNum = (previous_checkpoint  + 2) * 150
        for r in bft_network.all_replicas():
            lastExecSn = await bft_network.get_metric(r, bft_network, "Gauges", "lastExecutedSeqNum")
            self.assertEqual(expectedSeqNum, lastExecSn)
    
    async def verify_restart_ready_proof_msg(self, bft_network, bft=True):
        restartProofCount = 0
        required = bft_network.config.n if bft is False else (bft_network.config.n - bft_network.config.f)
        with log.start_action(action_type="verify_restart_ready_proof_msg", bft=bft):
            for r in bft_network.all_replicas():
                with log.start_action(action_type="verify_replica", replica=r):
                    with trio.move_on_after(seconds=2):
                        while True:
                            with trio.move_on_after(seconds=5):
                                restartProofMsg = await bft_network.get_metric(r, bft_network, "Counters", "receivedRestartProofMsg")
                                if(restartProofMsg > 0):
                                    restartProofCount += 1
                                    break
                                else:
                                    await trio.sleep(0.5)
            self.assertGreaterEqual (restartProofCount, required)
    
    async def verify_add_remove_status(self, bft_network, config_descriptor, restart_flag=True, quorum_all=True ):
        quorum = bft_client.MofNQuorum.All(bft_network.config, [r for r in range(bft_network.config.n)])
        bft_flag = False
        if quorum_all == False:
            quorum = bft_client.MofNQuorum.LinearizableQuorum(bft_network.config, [r.id for r in bft_network.replicas])
            bft_flag = True
        client = bft_network.random_client()
        op = operator.Operator(bft_network.config, client,  bft_network.builddir)
        await op.add_remove_with_wedge_status(quorum)
        rsi_rep = client.get_rsi_replies()
        for r in rsi_rep.values():
            status = cmf_msgs.ReconfigurationResponse.deserialize(r)[0]
            assert status.response.config_descriptor == config_descriptor
            assert status.response.restart_flag == restart_flag
            assert status.response.wedge_status == True
            assert status.response.bft_flag == bft_flag

        


    async def validate_state_consistency(self, skvbc, key, val):
        return await skvbc.assert_kv_write_executed(key, val)

    async def _wait_for_st(self, bft_network, ro_replica_id, seqnum_threshold=150):
        # TODO replace the below function with the library function:
        # await tracker.skvbc.tracked_fill_and_wait_for_checkpoint(
        # initial_nodes=bft_network.all_replicas(),
        # num_of_checkpoints_to_add=1)
        with trio.fail_after(seconds=70):
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
                        if lastExecutedSeqNum >= seqnum_threshold:
                            log.log_message(message_type="Replica" + str(ro_replica_id) + " : lastExecutedSeqNum:" + str(lastExecutedSeqNum))
                            break

    async def validate_epoch_number(self, bft_network, epoch_number, replicas):
        with trio.fail_after(seconds=70):
            succ = False
            # the ro replica should be able to survive these failures
            while not succ:
                succ = True
                for r in replicas:
                    curr_epoch = await bft_network.get_metric(r, bft_network, "Gauges", "epoch_number", component="epoch_manager")
                    if curr_epoch != epoch_number:
                        succ = False
                        break
    async def validate_initial_key_exchange(self, bft_network, replicas, metrics_id, expected):
        total = 0
        with trio.fail_after(seconds=10):
            succ = False
            while not succ:
                succ = True
                total = 0
                for r in replicas:
                    count = await bft_network.get_metric(r, bft_network, 'Counters', metrics_id,"KeyExchangeManager")
                    total += count
                    if total != expected:
                        succ = False
                    else:
                        succ = True
                        break
            assert total == expected

    async def wait_until_cre_gets_replicas_master_keys(self, bft_network, replicas):
        with trio.fail_after(60):
            succ = False
            while succ is False:
                succ = True
                for r in replicas:
                    master_key_path = os.path.join(bft_network.testdir, "replicas_rsa_keys", str(r), "pub_key")
                    if os.path.isfile(master_key_path) is False:
                        succ = False
                        break


if __name__ == '__main__':
    unittest.main()
