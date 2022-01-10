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
#include "concord.cmf.hpp"
#include "crypto_utils.hpp"

#include <variant>
#include <experimental/filesystem>
#include <fstream>
namespace fs = std::experimental::filesystem;

namespace concord::client::reconfiguration::handlers {

template <typename T>
bool validateInputState(const State& state) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
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
    if (!fs::exists(path)) fs::create_directories(path);
    file_handler_.encryptFile((path / "pub_key").string(), cmd.key);
  }
  latest_known_update_ = state.blockid;
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
bool ClientTlsKeyExchangeHandler::execute(const State& state, WriteState&) {
  auto cmd = getCmdFromInputState<concord::messages::ClientKeyExchangeCommand>(state);
  LOG_INFO(getLogger(), "execute tls key exchange request");
  // Generate new key pair
  auto new_cert_keys = concord::util::crypto::Crypto::instance().generateECDSAKeyPair(
      concord::util::crypto::KeyFormat::PemFormat, concord::util::crypto::CurveType::secp384r1);
  std::string suffix = enc_ ? ".enc" : "";
  std::string master_key = sm_->decryptString(master_key_path_).value_or(std::string());
  if (master_key.empty()) LOG_FATAL(getLogger(), "unable to read the node master key");
  auto root = fs::path(cert_folder_);
  for (const auto& c : bft_clients_) {
    fs::path pkey_path = root / std::to_string(c) / "client" / ("pk.pem" + suffix);
    fs::path cert_path = root / std::to_string(c) / "client" / "client.cert";

    auto cert =
        concord::util::crypto::CertificateUtils::generateSelfSignedCert(cert_path, new_cert_keys.second, master_key);
    sm_->encryptFile(pkey_path.string(), new_cert_keys.first);
    psm_.encryptFile(cert_path.string(), cert);
    psm_.encryptFile(version_path_, std::to_string(state.blockid));
    init_last_tls_update_block_ = state.blockid;
    LOG_INFO(getLogger(), "exchanged tls certificate for bft client " << c);
  }
  LOG_INFO(getLogger(), "done tls exchange");
  LOG_INFO(getLogger(), "restarting the node");
  bft::client::StateControl::instance().restart();
  return true;
}
}  // namespace concord::client::reconfiguration::handlers