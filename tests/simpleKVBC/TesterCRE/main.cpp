// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <getopt.h>

#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "bftclient/StateControl.hpp"
#include "config/test_comm_config.hpp"
#include "client/reconfiguration/config.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "crypto_utils.hpp"
#include "secrets_manager_plain.h"
#include "secrets_manager_enc.h"
#include "client/reconfiguration/default_handlers.hpp"
#include <variant>

using namespace bftEngine;
using namespace bft::communication;
using namespace concord::client::reconfiguration;
using std::string;
using bft::client::ClientConfig;
using bft::client::ClientId;
using bft::client::Client;
struct creParams {
  string commConfigFile;
  string certFolder;
  string replicasKeysFolder;
  ClientConfig bftConfig;
  Config CreConfig;
};
creParams setupCreParams(int argc, char** argv) {
  // We assume that cre bft client is the highest external client id in the system
  static struct option longOptions[] = {{"id", required_argument, 0, 'i'},
                                        {"fval", required_argument, 0, 'f'},
                                        {"cval", required_argument, 0, 'c'},
                                        {"replicas", required_argument, 0, 'r'},
                                        {"network-configuration-file", optional_argument, 0, 'n'},
                                        {"cert-folder", optional_argument, 0, 'k'},
                                        {"txn-signing-key-path", optional_argument, 0, 't'},
                                        {"interval-timeout", optional_argument, 0, 'o'},
                                        {0, 0, 0, 0}};
  creParams cre_param;
  cre_param.replicasKeysFolder = "./replicas_rsa_keys";
  ClientConfig& client_config = cre_param.bftConfig;
  int o = 0;
  int optionIndex = 0;
  LOG_INFO(GL, "Command line options:");
  while ((o = getopt_long(argc, argv, "i:f:c:r:n:k:t:o:", longOptions, &optionIndex)) != -1) {
    switch (o) {
      case 'i': {
        client_config.id = ClientId{concord::util::to<uint16_t>(optarg)};
        cre_param.CreConfig.id_ = concord::util::to<uint16_t>(optarg);
      } break;

      case 'f': {
        client_config.f_val = concord::util::to<uint16_t>(optarg);
      } break;

      case 'c': {
        client_config.c_val = concord::util::to<uint16_t>(optarg);
      } break;

      case 'r': {
        int tempnVal = concord::util::to<uint32_t>(optarg);
        for (int i = 0; i < tempnVal; i++) {
          client_config.all_replicas.emplace(bft::client::ReplicaId{static_cast<uint16_t>(i)});
        }
      } break;

      case 'n': {
        cre_param.commConfigFile = optarg;
      } break;

      case 't': {
        client_config.transaction_signing_private_key_file_path = optarg;
      } break;

      case 'o': {
        cre_param.CreConfig.interval_timeout_ms_ = concord::util::to<uint64_t>(optarg);
      } break;

      case 'k': {
        cre_param.certFolder = optarg;
      } break;

      case '?': {
        throw std::runtime_error("invalid arguments");
      } break;

      default:
        break;
    }
  }
  return cre_param;
}

auto logger = logging::getLogger("skvbtest.cre");

ICommunication* createCommunication(const ClientConfig& cc,
                                    const std::string& commFileName,
                                    const std::string& certFolder,
                                    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl>& sm,
                                    bool& enc) {
  TestCommConfig testCommConfig(logger);
  uint16_t numOfReplicas = cc.all_replicas.size();
  uint16_t clients = cc.id.val;
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName, certFolder);
  if (conf.secretData_.has_value()) {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerEnc>(conf.secretData_.value());
    enc = true;
  } else {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
    enc = false;
  }
#else
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#endif

  return CommFactory::create(conf);
}

class ClientsAddRemoveHandler : public IStateHandler {
 public:
  ClientsAddRemoveHandler(uint64_t init_update_block) : init_last_update_block_{init_update_block} {}

  bool validate(const State& state) const override {
    if (state.blockid < init_last_update_block_) return false;
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    return std::holds_alternative<concord::messages::ClientsAddRemoveExecuteCommand>(crep.response);
  }
  bool execute(const State& state, WriteState& out) override {
    LOG_INFO(getLogger(), "execute clientsAddRemoveCommand");
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    concord::messages::ClientsAddRemoveExecuteCommand command =
        std::get<concord::messages::ClientsAddRemoveExecuteCommand>(crep.response);

    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientsAddRemoveUpdateCommand creq;
    creq.config_descriptor = command.config_descriptor;
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {req_buf, [this, command]() {
             LOG_INFO(this->getLogger(), "completed scaling procedure for " << command.config_descriptor);
           }};
    return true;
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(
        logging::getLogger("concord.client.reconfiguration.testerCre.ClientsAddRemoveHandler"));
    return logger_;
  }
  uint64_t init_last_update_block_;
};

class ClientsRestartHandler : public IStateHandler {
 public:
  ClientsRestartHandler(uint64_t init_update_block, uint16_t clientId)
      : init_last_update_block_{init_update_block}, clientId_{clientId} {}

  bool validate(const State& state) const override {
    LOG_INFO(this->getLogger(), "validate restart command ");
    if (state.blockid < init_last_update_block_) return false;
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    return std::holds_alternative<concord::messages::ClientsRestartCommand>(crep.response);
  }
  bool execute(const State& state, WriteState& out) override {
    LOG_INFO(getLogger(), "execute clientsRestartCommand");
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    concord::messages::ClientsRestartCommand command =
        std::get<concord::messages::ClientsRestartCommand>(crep.response);

    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientsRestartUpdate creq;
    creq.sender_id = clientId_;
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {req_buf,
           [this, command]() { LOG_INFO(this->getLogger(), "completed cleint restart command " << KVLOG(clientId_)); }};
    return true;
  }

 private:
  logging::Logger getLogger() const {
    static logging::Logger logger_(
        logging::getLogger("concord.client.reconfiguration.testerCre.ClientsRestartHandler"));
    return logger_;
  }
  uint64_t init_last_update_block_;
  uint16_t clientId_;
};

int main(int argc, char** argv) {
  auto creParams = setupCreParams(argc, argv);
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  bool enc;
  std::unique_ptr<ICommunication> comm_ptr(
      createCommunication(creParams.bftConfig, creParams.commConfigFile, creParams.certFolder, sm_, enc));
  Client* bft_client = new Client(std::move(comm_ptr), creParams.bftConfig);
  PollBasedStateClient* pollBasedClient =
      new PollBasedStateClient(bft_client, creParams.CreConfig.interval_timeout_ms_, 0, creParams.CreConfig.id_);
  // First, lets find the latest update per action
  uint64_t last_pk_status{0};
  uint64_t last_scaling_status{0};
  uint64_t last_resatrt_status{0};
  bool succ = false;
  auto states = pollBasedClient->getStateUpdate(succ);
  while (!succ) {
    states = pollBasedClient->getStateUpdate(succ);
  }
  for (const auto& s : states) {
    concord::messages::ClientStateReply csp;
    concord::messages::deserialize(s.data, csp);
    if (std::holds_alternative<concord::messages::ClientExchangePublicKey>(csp.response)) {
      last_pk_status = s.blockid;
    }
    if (std::holds_alternative<concord::messages::ClientsAddRemoveUpdateCommand>(csp.response)) {
      last_scaling_status = s.blockid;
    }
    if (std::holds_alternative<concord::messages::ClientsRestartCommand>(csp.response)) {
      last_resatrt_status = s.blockid;
    }
  }
  ClientReconfigurationEngine cre(creParams.CreConfig, pollBasedClient, std::make_shared<concordMetrics::Aggregator>());
  std::vector<uint32_t> bft_clients;
  bft_clients.emplace_back(creParams.bftConfig.id.val);
  cre.registerHandler(std::make_shared<concord::client::reconfiguration::handlers::ClientTlsKeyExchangeHandler>(
      creParams.bftConfig.transaction_signing_private_key_file_path.value(),
      creParams.certFolder,
      enc,
      bft_clients,
      creParams.bftConfig.id.val,
      false,
      sm_));
  cre.registerHandler(std::make_shared<concord::client::reconfiguration::handlers::ClientMasterKeyExchangeHandler>(
      creParams.CreConfig.id_,
      creParams.bftConfig.transaction_signing_private_key_file_path.value(),
      sm_,
      last_pk_status));
  cre.registerHandler(std::make_shared<concord::client::reconfiguration::handlers::ClientRestartHandler>(
      last_resatrt_status, creParams.CreConfig.id_));
  cre.registerHandler(std::make_shared<concord::client::reconfiguration::handlers::ReplicaMainKeyPublicationHandler>(
      creParams.replicasKeysFolder));
  cre.registerHandler(std::make_shared<ClientsAddRemoveHandler>(last_scaling_status));

  bft::client::StateControl::instance().setRestartFunc([&cre, argv]() {
    cre.stop();
    execv(argv[0], argv);
  });
  cre.start();
  while (true) std::this_thread::sleep_for(1s);
}
