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
#include "secrets_manager_enc.h"
#include "client/reconfiguration/default_handlers.hpp"
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
                                    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl>& sm) {
  TestCommConfig testCommConfig(logger);
  uint16_t numOfReplicas = cc.all_replicas.size();
  uint16_t clients = cc.id.val;
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName, certFolder);
  if (conf.secretData.has_value()) {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerEnc>(conf.secretData.value());
  } else {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
  }
#else
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#endif

  return CommFactory::create(conf);
}

class KeyExchangeCommandHandler : public IStateHandler {
 public:
  KeyExchangeCommandHandler(uint16_t clientId,
                            const std::string& key_path,
                            const std::string& certFolder,
                            uint64_t init_update_block,
                            uint64_t init_tls_update_block,
                            std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm)
      : clientId_{clientId}, key_path_{key_path}, tls_key_path_{certFolder}, sm_{sm} {
    init_last_update_block_ = init_update_block;
    init_last_tls_update_block_ = std::stol(
        plain_sm_.decryptFile((tls_key_path_ / std::to_string(clientId_) / "client" / "client.cert.version").string())
            .value_or("0"));
  }
  bool validate(const State& state) const {
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    if (std::holds_alternative<concord::messages::ClientKeyExchangeCommand>(crep.response)) {
      concord::messages::ClientKeyExchangeCommand command =
          std::get<concord::messages::ClientKeyExchangeCommand>(crep.response);
      if (command.tls && state.blockid > init_last_tls_update_block_) return true;
      if (!command.tls && state.blockid > init_last_update_block_) return true;
    }
    return false;
  };
  bool execute(const State& state, WriteState& out) {
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    concord::messages::ClientKeyExchangeCommand command =
        std::get<concord::messages::ClientKeyExchangeCommand>(crep.response);
    if (!command.tls) return executeTransactionSigningKeyExchange(out);
    if (command.tls) return executeTlsKeyExchange(state.blockid, out);
    return false;
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.testerCre.KeyExchangeHandler"));
    return logger_;
  }

  bool executeTransactionSigningKeyExchange(WriteState& out) {
    LOG_INFO(getLogger(), "execute transaction signing key exchange request");
    // Generate new key pair
    auto hex_keys = concord::util::crypto::Crypto::instance().generateRsaKeyPair(
        2048, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat);
    auto pem_keys = concord::util::crypto::Crypto::instance().RsaHexToPem(hex_keys);

    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientExchangePublicKey creq;
    fs::path enc_file_path = key_path_;
    enc_file_path += ".enc";
    bool enc = false;
    std::fstream f(enc_file_path.string());
    if (f.good()) {
      enc_file_path += ".new";
      enc = true;
      sm_->encryptFile(enc_file_path.string(), pem_keys.first);
    }
    fs::path new_key_path = key_path_;
    bool non_enc = false;
    std::fstream f2(new_key_path);
    if (f2.good()) {
      new_key_path += ".new";
      plain_sm_.encryptFile(new_key_path.string(), pem_keys.first);
      non_enc = true;
    }

    std::string new_pub_key = hex_keys.second;
    creq.sender_id = clientId_;
    creq.pub_key = new_pub_key;
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {req_buf, [this, non_enc, new_key_path, enc, enc_file_path]() {
             if (enc) {
               fs::path enc_path = this->key_path_;
               enc_path += ".enc";
               fs::path old_path = enc_path;
               old_path += ".old";
               fs::copy(enc_path, old_path, fs::copy_options::update_existing);
               fs::copy(enc_file_path, enc_path, fs::copy_options::update_existing);
               fs::remove(old_path);
               fs::remove(enc_file_path);
               LOG_INFO(this->getLogger(), "exchanged transaction signing keys (encrypted)");
             }
             if (non_enc) {
               fs::path old_path = this->key_path_;
               old_path += ".old";
               fs::copy(this->key_path_, old_path, fs::copy_options::update_existing);
               fs::copy(new_key_path, this->key_path_, fs::copy_options::update_existing);
               fs::remove(old_path);
               fs::remove(new_key_path);
               LOG_INFO(this->getLogger(), "exchanged transaction signing keys (non encrypted)");
             }
           }};
    return true;
  }

  bool executeTlsKeyExchange(uint32_t version, WriteState& out) {
    LOG_INFO(getLogger(), "execute tls key exchange request");
    // Generate new key pair
    auto new_cert_keys = concord::util::crypto::Crypto::instance().generateECDSAKeyPair(
        concord::util::crypto::KeyFormat::PemFormat, concord::util::crypto::CurveType::secp384r1);
    fs::path enc_file_path = key_path_;
    enc_file_path += ".enc";
    std::string private_key_str;
    std::fstream f(enc_file_path.string());
    if (f.good()) {
      private_key_str = sm_->decryptFile(enc_file_path.string()).value_or("");
      f.close();
    }
    if (private_key_str.empty()) {
      fs::path new_key_path = key_path_;
      std::fstream f2(new_key_path);
      if (f2.good()) {
        private_key_str = plain_sm_.decryptFile(new_key_path.string()).value_or("");
        f2.close();
      }
    }
    if (private_key_str.empty()) LOG_FATAL(logger, "private key does not exist");
    auto current_cert_path = (tls_key_path_ / std::to_string(clientId_) / "client" / "client.cert").string();
    fs::path cert_priv_key_path = tls_key_path_ / std::to_string(clientId_) / "client" / "pk.pem";
    std::string cert_enc_priv_key_path = cert_priv_key_path.string() + ".enc";
    std::ifstream f3(cert_enc_priv_key_path);
    if (f3.good()) {
      sm_->encryptFile(cert_enc_priv_key_path, new_cert_keys.first);
      f3.close();
    }
    std::ifstream f4(cert_priv_key_path);
    if (f4.good()) {
      plain_sm_.encryptFile(cert_priv_key_path.string(), new_cert_keys.first);
      f4.close();
    }
    auto cert = concord::util::crypto::CertificateUtils::generateSelfSignedCert(
        current_cert_path, new_cert_keys.second, private_key_str);
    plain_sm_.encryptFile((tls_key_path_ / std::to_string(clientId_) / "client" / "client.cert").string(), cert);
    auto curr_version = std::to_string(version);
    plain_sm_.encryptFile((tls_key_path_ / std::to_string(clientId_) / "client" / "client.cert.version").string(),
                          curr_version);
    LOG_INFO(this->getLogger(), "exchanged tls certificate");
    return true;
  }
  uint16_t clientId_;
  fs::path key_path_;
  fs::path tls_key_path_;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  concord::secretsmanager::SecretsManagerPlain plain_sm_;
  uint64_t init_last_update_block_;
  uint64_t init_last_tls_update_block_;
};

class ReplicaTLSKeyExchangeHandler : public IStateHandler {
 public:
  ReplicaTLSKeyExchangeHandler(const std::string& cert_root_path) : cert_root_path_{cert_root_path} {}

  bool validate(const State& state) const override {
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    return std::holds_alternative<concord::messages::ReplicaTlsExchangeKey>(crep.response);
  }

  bool execute(const State& state, WriteState& out) override {
    bool succ = true;
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    auto command = std::get<concord::messages::ReplicaTlsExchangeKey>(crep.response);
    auto sender_id = command.sender_id;
    concord::messages::ReconfigurationResponse response;
    std::string bft_replicas_cert_path = cert_root_path_ + "/" + std::to_string(sender_id) + "/server/server.cert";
    auto current_rep_cert = sm_.decryptFile(bft_replicas_cert_path);
    if (current_rep_cert == command.cert) return succ;
    LOG_INFO(GL, "execute replica TLS key exchange using state transfer cre" << KVLOG(sender_id));
    std::string cert = std::move(command.cert);
    sm_.encryptFile(bft_replicas_cert_path, cert);
    LOG_INFO(GL, bft_replicas_cert_path + " is updated on the disk");
    return succ;
  }

 private:
  concord::secretsmanager::SecretsManagerPlain sm_;
  std::string cert_root_path_;
};

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
  std::unique_ptr<ICommunication> comm_ptr(
      createCommunication(creParams.bftConfig, creParams.commConfigFile, creParams.certFolder, sm_));
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
  cre.registerHandler(
      std::make_shared<KeyExchangeCommandHandler>(creParams.CreConfig.id_,
                                                  creParams.bftConfig.transaction_signing_private_key_file_path.value(),
                                                  creParams.certFolder,
                                                  last_pk_status,
                                                  0,
                                                  sm_));
  cre.registerHandler(std::make_shared<ClientsAddRemoveHandler>(last_scaling_status));
  cre.registerHandler(std::make_shared<ClientsRestartHandler>(last_resatrt_status, creParams.CreConfig.id_));
  cre.registerHandler(std::make_shared<ReplicaTLSKeyExchangeHandler>(creParams.certFolder));
  cre.registerHandler(std::make_shared<concord::client::reconfiguration::handlers::ReplicaMainKeyPublicationHandler>(
      creParams.replicasKeysFolder));
  cre.start();
  while (true) std::this_thread::sleep_for(1s);
}
