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

#include "secrets_manager_enc.h"

#include "aes.h"
#include "openssl_pass.h"
#include "base64.h"

namespace concord::secretsmanager {

SecretsManagerEnc::SecretsManagerEnc(const SecretData& secrets) {
  if (supported_digests_.find(secrets.digest) == supported_digests_.end()) {
    std::string digests;
    for (auto& d : supported_digests_) {
      digests.append(d + " ");
    }
    throw std::runtime_error("Unsupported digets " + secrets.digest + "Supported digests: " + digests);
  }

  if (supported_encs_.find(secrets.algo) == supported_encs_.end()) {
    std::string encs;
    for (auto& e : supported_encs_) {
      encs.append(e + " ");
    }
    throw std::runtime_error("Unsupported encryption algorithm " + secrets.algo + "Supported encryptions: " + encs);
  }

  password_ = secrets.password;
  key_length_ = secrets.key_length / 8;  // key is passed in bits
}

bool SecretsManagerEnc::encryptFile(std::string_view file_path, const std::string& input) {
  std::vector<uint8_t> salt(SALT_SIZE);
  rand.GenerateBlock(salt.data(), salt.size());
  auto key_params = deriveKeyPass(password_, salt, key_length_, AES_CBC::getBlockSize());
  AES_CBC e(key_params);

  auto cipher_text = e.encrypt(input);
  auto ct_encoded = base64Enc(salt, cipher_text);

  try {
    writeFile(file_path, ct_encoded);
  } catch (std::ios_base::failure& e) {
    LOG_ERROR(logger, "Error opening file for writing " << file_path << ": " << e.what());
    return false;
  }

  return true;
}

std::optional<std::string> SecretsManagerEnc::decryptFile(std::string_view path) {
  std::string data;
  try {
    data = readFile(path);
  } catch (std::ios_base::failure& e) {
    LOG_ERROR(logger, "Error opening file for reading    " << path << ": " << e.what());
    return std::nullopt;
  }

  auto dec = base64Dec(data);
  auto key_params = deriveKeyPass(password_, dec.salt, key_length_, AES_CBC::getBlockSize());
  AES_CBC e(key_params);
  auto pt = e.decrypt(dec.cipher_text);

  return std::optional<std::string>{pt};
}

}  // namespace concord::secretsmanager