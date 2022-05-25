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
#include "../IPublicKey.h"
#include <array>

class SSLEdDSAPublicKey : public IShareVerificationKey {
 public:
  static constexpr const size_t KeyByteSize = 32;
  using EdDSAPublicKeyBytes = std::array<uint8_t, KeyByteSize>;

  SSLEdDSAPublicKey(const EdDSAPublicKeyBytes& bytes);
  ~SSLEdDSAPublicKey() override = default;

  std::string serialize() const;
  bool verify(const uint8_t* message,
              const size_t messageLen,
              const uint8_t* signature,
              const size_t signatureLen) const;
  bool verify(const std::string& message, const std::string& signature) const;
  std::string toString() const override;

  static SSLEdDSAPublicKey fromHexString(const std::string& hexString);

 private:
  SSLEdDSAPublicKey() = default;
  EdDSAPublicKeyBytes bytes_;
};
