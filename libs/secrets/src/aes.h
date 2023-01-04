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

#include <vector>
#include <string>
#include <string_view>

#include "crypto/crypto.hpp"
#include "key_params.h"

namespace concord::secretsmanager {

class IAesMode {
 public:
  IAesMode(const KeyParams& params, const std::string& a_info = "") : params_{params}, additional_info(a_info) {}
  virtual std::vector<uint8_t> encrypt(std::string_view input) = 0;
  virtual std::string decrypt(const std::vector<uint8_t>& cipher) = 0;
  const KeyParams getKeyParams() { return params_; }
  const std::string getAdditionalInfo() { return additional_info; }
  // Not adding setters as these algo/modes must be set at construction time
  virtual ~IAesMode() = default;

 private:
  KeyParams params_;
  std::string additional_info;
};

class AES_CBC : public IAesMode {
 public:
  AES_CBC(const KeyParams& params, const std::string& a_info = "") : IAesMode(params, a_info) {}
  std::vector<uint8_t> encrypt(std::string_view input) override;
  std::string decrypt(const std::vector<uint8_t>& cipher) override;
  ~AES_CBC() = default;
};

class AES_GCM : public IAesMode {
 public:
  AES_GCM(const KeyParams& params, const std::string& a_info = "") : IAesMode(params, a_info) {}
  std::vector<uint8_t> encrypt(std::string_view input) override;
  std::string decrypt(const std::vector<uint8_t>& cipher) override;
  ~AES_GCM() = default;
};

}  // namespace concord::secretsmanager
