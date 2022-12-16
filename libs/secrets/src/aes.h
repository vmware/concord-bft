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

class iAES_Mode {
 public:
  iAES_Mode(const KeyParams& params,
            concord::crypto::SignatureAlgorithm algo = concord::crypto::SignatureAlgorithm::EdDSA)
      : params_{params}, algo_{algo} {}
  virtual std::vector<uint8_t> encrypt(std::string_view input) = 0;
  virtual std::string decrypt(const std::vector<uint8_t>& cipher) = 0;
  const KeyParams getKeyParams() { return params_; }
  const concord::crypto::SignatureAlgorithm getAlgorithm() { return algo_; }
  // Not adding setters as these algo/modes must be set at construction time
  virtual ~iAES_Mode() = default;

 private:
  KeyParams params_;
  concord::crypto::SignatureAlgorithm algo_;
};

class AES_CBC : public iAES_Mode {
 public:
  AES_CBC(const KeyParams& params,
          concord::crypto::SignatureAlgorithm algo = concord::crypto::SignatureAlgorithm::EdDSA)
      : iAES_Mode(params, algo) {}
  std::vector<uint8_t> encrypt(std::string_view input) override;
  std::string decrypt(const std::vector<uint8_t>& cipher) override;
  ~AES_CBC() = default;
};

class AES_GCM : public iAES_Mode {
 public:
  AES_GCM(const KeyParams& params,
          concord::crypto::SignatureAlgorithm algo = concord::crypto::SignatureAlgorithm::EdDSA)
      : iAES_Mode(params, algo) {}
  std::vector<uint8_t> encrypt(std::string_view input) override;
  std::string decrypt(const std::vector<uint8_t>& cipher) override;
  ~AES_GCM() = default;
};

}  // namespace concord::secretsmanager
