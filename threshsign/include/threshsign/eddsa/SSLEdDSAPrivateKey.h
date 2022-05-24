//
// Created by yflum on 26/04/2022.
//

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
