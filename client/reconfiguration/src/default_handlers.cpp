// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/reconfiguration/default_handlers.hpp"
#include "bftclient/StateControl.hpp"
#include "concord.cmf.hpp"
#include "crypto_utils.hpp"
#include "kvstream.h"

#include <variant>
#include <experimental/filesystem>
#include <fstream>
namespace fs = std::experimental::filesystem;

namespace concord::client::reconfiguration::handlers {

template <typename T>
bool validateInputState(const State& state, std::optional<uint64_t> init_block = std::nullopt) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  if (init_block.has_value() && init_block.value() > crep.block_id) return false;
  return std::holds_alternative<T>(crep.response);
}

template <typename T>
T getCmdFromInputState(const State& state) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::get<T>(crep.response);
}
bool ReplicaMainKeyPublicationHandler::validate(const State& state) const {
  return validateInputState<concord::messages::ReplicaMainKeyUpdate>(state);
}
bool ReplicaMainKeyPublicationHandler::execute(const State& state, WriteState&) {
  auto cmd = getCmdFromInputState<concord::messages::ReplicaMainKeyUpdate>(state);
  fs::path path = fs::path(output_dir_) / std::to_string(cmd.sender_id);
  auto curr_key = file_handler_.decryptFile((path / "pub_key").string()).value_or("");
  if (curr_key != cmd.key) {
    LOG_INFO(getLogger(), "executing ReplicaMainKeyPublicationHandler");
    if (!fs::exists(path)) fs::create_directories(path);
    file_handler_.encryptFile((path / "pub_key").string(), cmd.key);
  }
  return true;
}
bool ClientTlsKeyExchangeHandler::validate(const State& state) const {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  if (std::holds_alternative<concord::messages::ClientKeyExchangeCommand>(crep.response)) {
    concord::messages::ClientKeyExchangeCommand command =
        std::get<concord::messages::ClientKeyExchangeCommand>(crep.response);
    if (command.tls && state.blockid > init_last_tls_update_block_) return true;
  }
  return false;
}
void ClientTlsKeyExchangeHandler::exchangeTlsKeys(const std::string& pkey_path,
                                                  const std::string& cert_path,
                                                  const uint64_t blockid) {
  // Generate new key pair
  auto new_cert_keys = concord::util::crypto::Crypto::instance().generateECDSAKeyPair(
      concord::util::crypto::KeyFormat::PemFormat, concord::util::crypto::CurveType::secp384r1);

  std::string master_key = sm_->decryptFile(master_key_path_).value_or(std::string());
  if (master_key.empty()) master_key = psm_.decryptFile(master_key_path_).value_or(std::string());
  if (master_key.empty()) LOG_FATAL(getLogger(), "unable to read the node master key");
  auto cert =
      concord::util::crypto::CertificateUtils::generateSelfSignedCert(cert_path, new_cert_keys.second, master_key);

  sm_->encryptFile(pkey_path, new_cert_keys.first);
  psm_.encryptFile(cert_path, cert);
  psm_.encryptFile(version_path_, std::to_string(blockid));
  init_last_tls_update_block_ = blockid;
}
bool ClientTlsKeyExchangeHandler::execute(const State& state, WriteState&) {
  auto cmd = getCmdFromInputState<concord::messages::ClientKeyExchangeCommand>(state);
  LOG_INFO(getLogger(), "execute tls key exchange request");
  std::string suffix = enc_ ? ".enc" : "";
  auto root = fs::path(cert_folder_);
  if (use_unified_certificates_) {
    LOG_INFO(getLogger(), "Using unified certificates");
    const fs::path root_path = root / std::to_string(clientservice_pid_);

    const fs::path pkey_path = root_path / ("pk.pem" + suffix);
    const fs::path cert_path = root_path / "node.cert";

    LOG_INFO(getLogger(), KVLOG(pkey_path, cert_path));
    exchangeTlsKeys(pkey_path.string(), cert_path.string(), state.blockid);
    LOG_INFO(getLogger(),
             "exchanged tls certificate for clientservice " << KVLOG(clientservice_pid_, init_last_tls_update_block_));
  } else {
    for (const auto& c : bft_clients_) {
      const fs::path root_path = root / std::to_string(c);

      const fs::path pkey_path = root_path / "client" / ("pk.pem" + suffix);
      const fs::path cert_path = root_path / "client" / "client.cert";

      exchangeTlsKeys(pkey_path.string(), cert_path.string(), state.blockid);
      LOG_INFO(getLogger(), "exchanged tls certificate for bft client " << c);
    }
  }
  LOG_INFO(getLogger(), "done tls exchange");
  LOG_INFO(getLogger(), "restarting the node");
  bft::client::StateControl::instance().restart();
  return true;
}

ClientTlsKeyExchangeHandler::ClientTlsKeyExchangeHandler(
    const std::string& master_key_path,
    const std::string& cert_folder,
    bool enc,
    const std::vector<uint32_t>& bft_clients,
    uint16_t clientservice_pid,
    bool use_unified_certificates,
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm)
    : master_key_path_{master_key_path},
      cert_folder_{cert_folder},
      enc_{enc},
      sm_{sm},
      bft_clients_{bft_clients},
      clientservice_pid_{clientservice_pid},
      use_unified_certificates_{use_unified_certificates} {
  version_path_ = cert_folder + "/version";
  if (!fs::exists(version_path_)) fs::create_directories(version_path_);
  version_path_ += "/exchange.version";
  init_last_tls_update_block_ = std::stol(psm_.decryptFile(version_path_).value_or("0"));
  LOG_INFO(getLogger(), KVLOG(use_unified_certificates_, clientservice_pid_, init_last_tls_update_block_));
}
ClientMasterKeyExchangeHandler::ClientMasterKeyExchangeHandler(
    uint32_t client_id,
    const std::string& master_key_path,
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm,
    uint64_t last_update_block)
    : client_id_{client_id}, master_key_path_{master_key_path}, sm_{sm}, init_last_update_block_{last_update_block} {}
bool ClientMasterKeyExchangeHandler::validate(const State& state) const {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  if (std::holds_alternative<concord::messages::ClientKeyExchangeCommand>(crep.response)) {
    concord::messages::ClientKeyExchangeCommand command =
        std::get<concord::messages::ClientKeyExchangeCommand>(crep.response);
    if (!command.tls && state.blockid > init_last_update_block_) return true;
  }
  return false;
}
bool ClientMasterKeyExchangeHandler::execute(const State& state, WriteState& out) {
  LOG_INFO(getLogger(), "execute transaction signing key exchange request");
  // Generate new key pair
  auto hex_keys = concord::util::crypto::Crypto::instance().generateRsaKeyPair(
      2048, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat);
  auto pem_keys = concord::util::crypto::Crypto::instance().RsaHexToPem(hex_keys);

  concord::messages::ReconfigurationRequest rreq;
  concord::messages::ClientExchangePublicKey creq;
  concord::secretsmanager::ISecretsManagerImpl& sm = master_key_path_.find(".enc") != std::string::npos ? *sm_ : psm_;
  sm.encryptFile(master_key_path_ + ".new", pem_keys.first);

  std::string new_pub_key = hex_keys.second;
  creq.sender_id = client_id_;
  creq.pub_key = new_pub_key;
  rreq.command = creq;
  std::vector<uint8_t> req_buf;
  concord::messages::serialize(req_buf, rreq);
  out = {req_buf, [&]() {
           fs::copy(master_key_path_, master_key_path_ + ".old");
           fs::copy(master_key_path_ + ".new", master_key_path_, fs::copy_options::update_existing);
           fs::remove(master_key_path_ + ".new");
           fs::remove(master_key_path_ + ".old");
           LOG_INFO(getLogger(), "exchanged transaction signing keys " << master_key_path_);
           LOG_INFO(getLogger(), "restarting the node");
           bft::client::StateControl::instance().restart();
         }};
  return true;
}
bool ClientRestartHandler::validate(const State& state) const {
  return validateInputState<concord::messages::ClientsRestartCommand>(state, init_update_block_);
}
bool ClientRestartHandler::execute(const State& state, WriteState& out) {
  LOG_INFO(getLogger(), "executing ClientRestartHandler");
  auto command = getCmdFromInputState<concord::messages::ClientsRestartCommand>(state);
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::ClientsRestartUpdate creq;
  creq.sender_id = client_id_;
  rreq.command = creq;
  std::vector<uint8_t> req_buf;
  concord::messages::serialize(req_buf, rreq);
  out = {req_buf, [&, command]() {
           LOG_INFO(this->getLogger(), "completed client restart command " << client_id_);
           bft::client::StateControl::instance().restart();
         }};
  return true;
}
}  // namespace concord::client::reconfiguration::handlers
