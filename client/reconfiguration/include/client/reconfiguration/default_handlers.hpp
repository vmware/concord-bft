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

#pragma once

#include "cre_interfaces.hpp"
#include "secrets_manager_plain.h"

#include <vector>

namespace concord::client::reconfiguration::handlers {
class ReplicaMainKeyPublicationHandler : public IStateHandler {
 public:
  ReplicaMainKeyPublicationHandler(const std::string& output_dir) : output_dir_{output_dir} {}
  bool validate(const State&) const override;
  bool execute(const State&, WriteState&) override;

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(
        logging::getLogger("concord.client.reconfiguration.ReplicaMainKeyPublicationHandler"));
    return logger_;
  }
  std::string output_dir_;
  concord::secretsmanager::SecretsManagerPlain file_handler_;
};

class ClientTlsKeyExchangeHandler : public IStateHandler {
 public:
  ClientTlsKeyExchangeHandler(const std::string& master_key_path,
                              const std::string& cert_folder,
                              bool enc,
                              const std::vector<uint32_t>& bft_clients,
                              uint16_t clientservice_pid,
                              bool use_unified_certificates,
                              std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm);
  bool validate(const State&) const override;
  bool execute(const State&, WriteState&) override;
  void exchangeTlsKeys(const std::string& pkey_path, const std::string& cert_path, const uint64_t blockid);

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.ClientTlsKeyExchangeHandler"));
    return logger_;
  }
  std::string master_key_path_;
  std::string cert_folder_;
  bool enc_;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  std::vector<uint32_t> bft_clients_;
  uint16_t clientservice_pid_;
  bool use_unified_certificates_;
  uint64_t init_last_tls_update_block_;
  concord::secretsmanager::SecretsManagerPlain psm_;
  std::string version_path_;
};

class ClientMasterKeyExchangeHandler : public IStateHandler {
 public:
  ClientMasterKeyExchangeHandler(uint32_t client_id,
                                 const std::string& master_key_path,
                                 std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm,
                                 uint64_t last_update_block);
  bool validate(const State&) const override;
  bool execute(const State&, WriteState&) override;

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.ClientMasterKeyExchangeHandler"));
    return logger_;
  }
  uint32_t client_id_;
  std::string master_key_path_;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  uint64_t init_last_update_block_;
  concord::secretsmanager::SecretsManagerPlain psm_;
};

class ClientRestartHandler : public IStateHandler {
 public:
  ClientRestartHandler(uint64_t init_update_block, uint16_t client_id)
      : init_update_block_{init_update_block}, client_id_{client_id} {}
  bool validate(const State&) const override;
  bool execute(const State&, WriteState&) override;

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.ClientRestartHandler"));
    return logger_;
  }

  uint64_t init_update_block_;
  uint32_t client_id_;
};
}  // namespace concord::client::reconfiguration::handlers