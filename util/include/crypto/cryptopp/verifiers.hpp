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
#include "types.hpp"
#include "crypto/crypto.hpp"
#include "crypto/verifier.hpp"

namespace concord::crypto::cryptopp {

class ECDSAVerifier : public IVerifier {
 public:
  ECDSAVerifier(const std::string &str_pub_key,
                concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat);
  bool verifyBuffer(const concord::Byte *msg, size_t msgLen, const concord::Byte *sig, size_t sigLen) const override;
  uint32_t signatureLength() const override;
  std::string getPubKey() const override { return key_str_; }
  ~ECDSAVerifier();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

class RSAVerifier : public IVerifier {
 public:
  RSAVerifier(const std::string &str_pub_key,
              concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat);
  bool verifyBuffer(const concord::Byte *msg, size_t msgLen, const concord::Byte *sig, size_t sigLen) const override;
  uint32_t signatureLength() const override;
  std::string getPubKey() const override { return key_str_; }
  ~RSAVerifier();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

}  // namespace concord::crypto::cryptopp
