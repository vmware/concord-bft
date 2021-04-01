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
#include <memory>
#include <set>
#include <functional>

#include <cryptopp/osrng.h>

#include "Logger.hpp"

#include "secrets_manager_impl.h"
#include "secret_data.h"

namespace concord::secretsmanager {

struct KeyParams;

// SecretsManagerEnc handles encryption and decryption of files.
// The following flow is used for encryption:
// 1. SecretData are the input parameters for SecretsManagerEnc. They contain an algorithm name, symmetric key and IV.
// 2. The key and iv are used to encrypt the payload with a supported block cipher (AES256 CBC).
// 3. The buffer is base64 encoded and written to file.
class SecretsManagerEnc : public ISecretsManagerImpl {
 public:
  SecretsManagerEnc(const SecretData& secrets);

  SecretData getInitialSecretData() { return initial_secret_data_; }

  // Creates an encrypted file on the filesystem from string input
  bool encryptFile(std::string_view file_path, const std::string& input) override;

  std::optional<std::string> encryptString(const std::string& input) override;

  // Loads an encrypted file and returns it as a string buffer
  // The function is intended for loading private keys, so the file
  // should be reasonably sized.
  std::optional<std::string> decryptFile(std::string_view path) override;
  std::optional<std::string> decryptFile(const std::ifstream& file) override;
  std::optional<std::string> decryptString(const std::string& input) override;

  // = default won't work here. The destructor needs to be defined in the cpp due to the forward declarations and
  // unique_ptr
  ~SecretsManagerEnc();

 private:
  const std::set<std::string> supported_encs_{"AES/CBC/PKCS5Padding", "AES/CBC/PKCS7Padding"};

  logging::Logger logger_ = logging::getLogger("concord.bft.secrets-manager-enc");
  const std::unique_ptr<KeyParams> key_params_;

  const SecretData initial_secret_data_;

  std::optional<std::string> encrypt(const std::string& data);
  std::optional<std::string> decrypt(const std::string& data);
};

}  // namespace concord::secretsmanager