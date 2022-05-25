// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
#pragma once
#include "../ISecretKey.h"
#include <array>

class SSLEdDSAPrivateKey : public IShareSecretKey {
 public:
  static constexpr const size_t KeyByteSize = 32;
  static constexpr const size_t SignatureByteSize = 64;
  using EdDSAPrivateKeyBytes = std::array<uint8_t, KeyByteSize>;
  using EdDSASignatureBytes = std::array<uint8_t, SignatureByteSize>;

  SSLEdDSAPrivateKey(const EdDSAPrivateKeyBytes& bytes);
  ~SSLEdDSAPrivateKey() override = default;

  std::string sign(const std::string& message) const;
  std::string sign(const uint8_t* msg, size_t len) const;
  void sign(const uint8_t* msg, size_t len, uint8_t* signature, size_t& signatureLength) const;
  std::string toString() const override;

  static SSLEdDSAPrivateKey fromHexString(const std::string& hexString);

 private:
  SSLEdDSAPrivateKey() = default;

  EdDSAPrivateKeyBytes bytes_;
};
