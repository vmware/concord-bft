//
// Created by yflum on 26/04/2022.
//
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
