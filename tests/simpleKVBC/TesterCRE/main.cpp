// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
// This module creates an instance of ClientImp class using input
// parameters and launches a bunch of tests created by TestsBuilder towards
// concord::consensus::ReplicaImp objects.

#include <getopt.h>

#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "config/test_comm_config.hpp"
#include "client/reconfiguration/config.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "crypto_utils.hpp"
#include "secrets_manager_plain.h"
#include <variant>
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

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
                                    const std::string& certFolder) {
  TestCommConfig testCommConfig(logger);
  uint16_t numOfReplicas = cc.all_replicas.size();
  uint16_t clients = cc.id.val;
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName, certFolder);
#else
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#endif

  return CommFactory::create(conf);
}

class KeyExchangeCommandHandler : public IStateHandler {
 public:
  KeyExchangeCommandHandler(uint16_t clientId, const std::string& key_path, std::function<void()> restart)
      : clientId_{clientId}, key_path_{key_path}, restart_{std::move(restart)} {
    sm_.reset(new concord::secretsmanager::SecretsManagerPlain());
  }
  bool validate(const State& state) const {
    concord::messages::ClientReconfigurationStateReply crep;
    concord::messages::deserialize(state.data, crep);
    return std::holds_alternative<concord::messages::ClientKeyExchangeCommand>(crep.response);
  };
  bool execute(const State& state, WriteState& out) {
    LOG_INFO(getLogger(), "execute key exchange request");
    concord::messages::ClientReconfigurationStateReply crep;
    concord::messages::deserialize(state.data, crep);
    concord::messages::ClientKeyExchangeCommand command =
        std::get<concord::messages::ClientKeyExchangeCommand>(crep.response);

    // Generate new key pair
    auto hex_keys = concord::util::Crypto::instance().generateRsaKeyPair(
        2048, concord::util::Crypto::KeyFormat::HexaDecimalStrippedFormat);
    auto pem_keys = concord::util::Crypto::instance().hexToPem(hex_keys);

    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientExchangePublicKey creq;
    fs::path new_key_path = key_path_;
    new_key_path += ".new";
    sm_->encryptFile(new_key_path.string(), pem_keys.first);
    std::string new_pub_key = hex_keys.second;
    creq.sender_id = clientId_;
    creq.pub_key = new_pub_key;
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {req_buf, [this, new_key_path]() {
             fs::path old_path = this->key_path_;
             old_path += ".old";
             fs::copy(this->key_path_, old_path, fs::copy_options::update_existing);
             fs::copy(new_key_path, this->key_path_, fs::copy_options::update_existing);
             fs::remove(new_key_path);
             fs::remove(old_path);
             LOG_INFO(this->getLogger(), "restarting the component");
             this->restart_();
           }};
    return true;
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.testerCre.KeyExchangeHandler"));
    return logger_;
  }
  uint16_t clientId_;
  fs::path key_path_;
  std::unique_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  std::function<void()> restart_;
};

class ClientsAddRemoveHandler : public IStateHandler {
 public:
  bool validate(const State& state) const override {
    concord::messages::ClientReconfigurationStateReply crep;
    concord::messages::deserialize(state.data, crep);
    return std::holds_alternative<concord::messages::ClientsAddRemoveCommand>(crep.response);
  }
  bool execute(const State& state, WriteState& out) override {
    LOG_INFO(getLogger(), "execute clientsAddRemoveCommand");
    concord::messages::ClientReconfigurationStateReply crep;
    concord::messages::deserialize(state.data, crep);
    concord::messages::ClientsAddRemoveCommand command =
        std::get<concord::messages::ClientsAddRemoveCommand>(crep.response);

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
  logging::Logger getLogger() {
    static logging::Logger logger_(
        logging::getLogger("concord.client.reconfiguration.testerCre.ClientsAddRemoveHandler"));
    return logger_;
  }
};

int main(int argc, char** argv) {
  auto creParams = setupCreParams(argc, argv);
  std::unique_ptr<ICommunication> comm_ptr(
      createCommunication(creParams.bftConfig, creParams.commConfigFile, creParams.certFolder));
  Client* bft_client = new Client(std::move(comm_ptr), creParams.bftConfig);
  IStateClient* pollBasedClient =
      new PollBasedStateClient(bft_client, creParams.CreConfig.interval_timeout_ms_, 0, creParams.CreConfig.id_);
  ClientReconfigurationEngine cre(creParams.CreConfig, pollBasedClient, std::make_shared<concordMetrics::Aggregator>());
  cre.registerHandler(std::make_shared<KeyExchangeCommandHandler>(
      creParams.CreConfig.id_, creParams.bftConfig.transaction_signing_private_key_file_path.value(), [&] {
        cre.stop();
        execv(argv[0], argv);
      }));
  cre.registerHandler(std::make_shared<ClientsAddRemoveHandler>());
  cre.start();
  while (true) std::this_thread::sleep_for(1s);
}
