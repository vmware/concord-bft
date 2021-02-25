// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <string>

#include "secrets_manager_impl.h"
#include "Logger.hpp"

namespace concord::secretsmanager {

// Dummy implementation of ISecretsManagerImpl. Doesn't do any encryption. Only reads/writes files.
class SecretsManagerPlain : public ISecretsManagerImpl {
  logging::Logger logger = logging::getLogger("secrets-manager-plain");

 public:
  bool encryptFile(std::string_view file_path, const std::string& input) override;
  std::optional<std::string> decryptFile(std::string_view path) override;
};

}  // namespace concord::secretsmanager