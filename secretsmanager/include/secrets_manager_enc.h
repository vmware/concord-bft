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

// SecretsManagerEnc handles encryption and decryption of files.
// The following flow is used for encryption:
// 1. A random salt is generated
// 2. The salt and the password (provided by the configuration) are used to derive a secret key and iv. OpenSSL
//   OPENSSL_EVP_BytesToKey() is used for key derivation (openssl_pass.h).
// 3. The key is used to encrypt the payload with a supported block cipher (AES256 CBC).
// 4. The encrypted cipher text and the salt are concatenated.
//    The contents of the buffer is: Salted__<8 bytes salt><cipher>
// 5. The buffer is base64 encoded and written to file.
class SecretsManagerEnc : public ISecretsManagerImpl {
  std::string password_;
  std::string digest_;
  std::string algo_;
  uint32_t key_length_;

  const std::set<std::string> supported_encs_{"AES/CBC/PKCS5Padding", "AES/CBC/PKCS7Padding"};
  const std::set<std::string> supported_digests_{"SHA-256"};
  const uint32_t SALT_SIZE = 8;

  logging::Logger logger = logging::getLogger("concord.bft.secrets-manager-enc");
  CryptoPP::BlockingRng rand;

 public:
  SecretsManagerEnc(const SecretData& secrets);

  // Creates an encrypted file on the filesystem from string input
  bool encryptFile(std::string_view file_path, const std::string& input) override;

  // Loads an encrypted file and returns it as a string buffer
  // The function is intended for loading private keys, so the file
  // should be reasonably sized.
  std::optional<std::string> decryptFile(std::string_view path) override;

  ~SecretsManagerEnc() = default;
};

}  // namespace concord::secretsmanager