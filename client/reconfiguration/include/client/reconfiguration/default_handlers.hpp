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
  std::string output_dir_;
  concord::secretsmanager::SecretsManagerPlain file_handler_;
  uint32_t latest_known_update_{0};
class ClientTlsKeyExchangeHandler : public IStateHandler {
 public:
  ClientTlsKeyExchangeHandler(const std::string& master_key_path,
                              const std::string& cert_folder,
                              bool enc,
                              const std::vector<uint32_t> bft_clients,
                              std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm);
  bool validate(const State&) const override;
  bool execute(const State&, WriteState&) override;

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
  uint64_t init_last_tls_update_block_;
  concord::secretsmanager::SecretsManagerPlain psm_;
  std::string version_path_;
};

}  // namespace concord::client::reconfiguration::handlers