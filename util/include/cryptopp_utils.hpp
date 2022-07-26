// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include "crypto/interface/signer.hpp"
#include "crypto/interface/verifier.hpp"
#include "crypto_utils.hpp"

namespace concord::crypto::cryptopp {

constexpr static uint16_t RSA_SIGNATURE_LENGTH = 2048;

class ECDSAVerifier : public IVerifier {
 public:
  ECDSAVerifier(const std::string& str_pub_key,
                concord::util::crypto::KeyFormat fmt = concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat);
  bool verify(const std::string& data, const std::string& sig) const override;
  uint32_t signatureLength() const override;
  std::string getPubKey() const override { return key_str_; }
  ~ECDSAVerifier();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

class ECDSASigner : public ISigner {
 public:
  ECDSASigner(const std::string& str_priv_key,
              concord::util::crypto::KeyFormat fmt = concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat);
  std::string sign(const std::string& data) override;
  uint32_t signatureLength() const override;
  std::string getPrivKey() const override { return key_str_; }
  ~ECDSASigner();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

class RSAVerifier : public IVerifier {
 public:
  RSAVerifier(const std::string& str_pub_key,
              concord::util::crypto::KeyFormat fmt = concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat);
  bool verify(const std::string& data, const std::string& sig) const override;
  uint32_t signatureLength() const override;
  std::string getPubKey() const override { return key_str_; }
  ~RSAVerifier();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

class RSASigner : public ISigner {
 public:
  RSASigner(const std::string& str_priv_key,
            concord::util::crypto::KeyFormat fmt = concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat);
  std::string sign(const std::string& data) override;
  uint32_t signatureLength() const override;
  std::string getPrivKey() const override { return key_str_; }
  ~RSASigner();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  std::string key_str_;
};

class Crypto {
 public:
  static Crypto& instance() {
    static Crypto crypto;
    return crypto;
  }

  Crypto();
  ~Crypto();
  std::pair<std::string, std::string> generateRsaKeyPair(
      const uint32_t sig_length,
      const concord::util::crypto::KeyFormat fmt = concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat) const;
  std::pair<std::string, std::string> generateECDSAKeyPair(
      const concord::util::crypto::KeyFormat fmt,
      concord::util::crypto::CurveType curve_type = concord::util::crypto::CurveType::secp256k1) const;
  std::pair<std::string, std::string> RsaHexToPem(const std::pair<std::string, std::string>& key_pair) const;
  std::pair<std::string, std::string> ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) const;
  concord::util::crypto::KeyFormat getFormat(const std::string& key_str) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace concord::crypto::cryptopp