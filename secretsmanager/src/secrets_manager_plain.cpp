// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This convenience header combines different block implementations.

#include "secrets_manager_plain.h"
#include <string>

namespace concord::secretsmanager {

bool SecretsManagerPlain::encryptFile(std::string_view file_path, const std::string& input) {
  try {
    writeFile(file_path, input);
  } catch (std::exception& e) {
    LOG_ERROR(logger, "Error opening file for writing " << file_path << ": " << e.what());
    return false;
  }

  return true;
}

std::optional<std::string> SecretsManagerPlain::decryptFile(std::string_view path) {
  std::string data;
  try {
    data = readFile(path);
  } catch (std::exception& e) {
    LOG_WARN(logger, "Unable to open file for reading    " << path << ": " << e.what());
    return std::nullopt;
  }

  return std::optional<std::string>{data};
}

std::optional<std::string> SecretsManagerPlain::decryptFile(const std::ifstream& file) {
  std::string data;
  try {
    data = readFile(file);
  } catch (std::exception& e) {
    LOG_ERROR(logger, "Error opening stream for reading " << e.what());
    return std::nullopt;
  }

  return std::optional<std::string>{data};
}

std::optional<std::string> SecretsManagerPlain::encryptString(const std::string& input) { return input; }

std::optional<std::string> SecretsManagerPlain::decryptString(const std::string& input) { return input; }

}  // namespace concord::secretsmanager
