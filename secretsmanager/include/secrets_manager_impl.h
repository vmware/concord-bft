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

#pragma once

#include <string>
#include <optional>

namespace concord::secretsmanager {

class ISecretsManagerImpl {
 protected:
  std::string readFile(std::string_view path);
  std::string readFile(const std::ifstream& file);
  void writeFile(std::string_view file_path, const std::string& content);

 public:
  // Creates an encrypted file on the filesystem from string
  virtual bool encryptFile(std::string_view file_path, const std::string& input) = 0;

  virtual std::optional<std::string> encryptString(const std::string& input) = 0;

  // Loads an encrypted file and returns it as a string buffer
  // The function is intended for loading private keys, so the file should be reasonably sized.
  virtual std::optional<std::string> decryptFile(std::string_view path) = 0;
  virtual std::optional<std::string> decryptFile(const std::ifstream& file) = 0;

  virtual std::optional<std::string> decryptString(const std::string& input) = 0;

  virtual ~ISecretsManagerImpl() = default;
};

}  // namespace concord::secretsmanager