// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <memory>

#include "crypto/crypto.hpp"
#include "crypto/signer.hpp"

namespace concord::crypto::cryptopp {

constexpr static uint32_t RSA_SIGNATURE_LENGTH = 2048U;

class ECDSASigner : public ISigner {
 public:
  ECDSASigner(const std::string& str_priv_key,
              concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat);
  std::string sign(const std::string& data);
  size_t signBuffer(const Byte* const dataIn, size_t dataLen, Byte* sigOutBuffer) override;
  size_t signatureLength() const override;
  std::string getPrivKey() const override { return key_str_; }
  ~ECDSASigner();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

class RSASigner : public ISigner {
 public:
  RSASigner(const std::string& str_priv_key,
            concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat);
  size_t signBuffer(const Byte* const dataIn, size_t dataLen, Byte* sigOutBuffer) override;
  size_t signatureLength() const override;
  std::string getPrivKey() const override { return key_str_; }
  ~RSASigner();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

}  // namespace concord::crypto::cryptopp
