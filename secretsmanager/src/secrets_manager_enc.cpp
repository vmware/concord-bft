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
#include "base64.h"

namespace concord::secretsmanager {

SecretsManagerEnc::SecretsManagerEnc(const SecretData& secrets)
    : key_params_{std::make_unique<KeyParams>(secrets.key, secrets.iv)}, initial_secret_data_{secrets} {
  if (supported_encs_.find(secrets.algo) == supported_encs_.end()) {
    std::string encs;
    for (auto& e : supported_encs_) {
      encs.append(e + " ");
    }
    throw std::runtime_error("Unsupported encryption algorithm " + secrets.algo + "Supported encryptions: " + encs);
  }
}

bool SecretsManagerEnc::encryptFile(std::string_view file_path, const std::string& input) {
  auto ct_encoded = encrypt(input);
  if (!ct_encoded.has_value()) {
    return false;
  }

  try {
    writeFile(file_path, *ct_encoded);
  } catch (std::exception& e) {
    LOG_ERROR(logger_, "Error opening file for writing " << file_path << ": " << e.what());
    return false;
  }

  return true;
}

std::optional<std::string> SecretsManagerEnc::encryptString(const std::string& input) { return encrypt(input); }

std::optional<std::string> SecretsManagerEnc::decryptFile(std::string_view path) {
  std::string data;
  try {
    data = readFile(path);
  } catch (std::exception& e) {
    LOG_WARN(logger_, "Error opening file for reading " << path << ": " << e.what());
    return std::nullopt;
  }

  return decrypt(data);
}

std::optional<std::string> SecretsManagerEnc::decryptFile(const std::ifstream& file) {
  std::string data;
  try {
    data = readFile(file);
  } catch (std::exception& e) {
    LOG_WARN(logger_, "Error reading from file stream: " << e.what());
    return std::nullopt;
  }

  return decrypt(data);
}

std::optional<std::string> SecretsManagerEnc::decryptString(const std::string& input) { return decrypt(input); }

std::optional<std::string> SecretsManagerEnc::decrypt(const std::string& data) {
  try {
    // AES_CBC is created on each call fir thread safety
    auto aes = AES_CBC(*key_params_);
    auto cipher_text = base64Dec(data);
    auto pt = aes.decrypt(cipher_text);

    return std::optional<std::string>{pt};
  } catch (std::exception& e) {
    LOG_ERROR(logger_, "Decryption error: " << e.what());
  }

  return std::nullopt;
}

std::optional<std::string> SecretsManagerEnc::encrypt(const std::string& data) {
  try {
    // AES_CBC is created on each call fir thread safety
    auto aes = AES_CBC(*key_params_);
    auto cipher_text = aes.encrypt(data);
    return std::optional<std::string>{base64Enc(cipher_text)};
  } catch (std::exception& e) {
    LOG_ERROR(logger_, "Encryption error: " << e.what());
  }

  return std::nullopt;
}

SecretsManagerEnc::~SecretsManagerEnc() {}

}  // namespace concord::secretsmanager