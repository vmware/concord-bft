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

#include <utility>
#include <string>
#include <memory>
namespace concord::util::crypto {
enum class KeyFormat : std::uint16_t { HexaDecimalStrippedFormat, PemFormat };
class IVerifier {
 public:
  virtual bool verify(const std::string& data, const std::string& sig) = 0;
  virtual uint32_t signatureLength() = 0;
  virtual ~IVerifier() = default;
};

class ISigner {
 public:
  virtual std::string sign(const std::string& data) = 0;
  virtual uint32_t signatureLength() = 0;
  virtual ~ISigner() = default;
};

class ECDSAVerifier : public IVerifier {
 public:
  ECDSAVerifier(const std::string& str_pub_key, KeyFormat fmt);
  bool verify(const std::string& data, const std::string& sig) override;
  uint32_t signatureLength() override;
  ~ECDSAVerifier();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class ECDSASigner : public ISigner {
 public:
  ECDSASigner(const std::string& str_pub_key, KeyFormat fmt);
  std::string sign(const std::string& data) override;
  uint32_t signatureLength() override;
  ~ECDSASigner();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
class RSAVerifier : public IVerifier {
 public:
  RSAVerifier(const std::string& str_pub_key, KeyFormat fmt);
  bool verify(const std::string& data, const std::string& sig) override;
  uint32_t signatureLength() override;
  ~RSAVerifier();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class RSASigner : public ISigner {
 public:
  RSASigner(const std::string& str_priv_key, KeyFormat fmt);
  std::string sign(const std::string& data) override;
  uint32_t signatureLength() override;
  ~RSASigner();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class Crypto {
 public:
  static Crypto& instance() {
    static Crypto crypto;
    return crypto;
  }

  Crypto();
  ~Crypto();
  std::pair<std::string, std::string> generateRsaKeyPair(const uint32_t sig_length, const KeyFormat fmt) const;
  std::pair<std::string, std::string> generateECDSAKeyPair(const KeyFormat fmt) const;
  std::pair<std::string, std::string> RsaHexToPem(const std::pair<std::string, std::string>& key_pair) const;
  std::pair<std::string, std::string> ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace concord::util::crypto