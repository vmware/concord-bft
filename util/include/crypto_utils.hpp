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

#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/evp.h>

namespace concord::util::crypto {
enum class KeyFormat : std::uint16_t { HexaDecimalStrippedFormat, PemFormat };
enum class CurveType : std::uint16_t { secp256k1, secp384r1 };

class CertificateUtils {
 public:
  static std::string generateSelfSignedCert(const std::string& origin_cert_path,
                                            const std::string& pub_key,
                                            const std::string& signing_key);
  static bool verifyCertificate(X509* cert, const std::string& pub_key);
  static bool verifyCertificate(X509* cert_to_verify, const std::string& cert_root_directory, uint32_t& remote_peer_id);
};
class IVerifier {
 public:
  virtual bool verify(const std::string& data, const std::string& sig) const = 0;
  virtual uint32_t signatureLength() const = 0;
  virtual ~IVerifier() = default;
  virtual std::string getPubKey() const = 0;
};

class ISigner {
 public:
  virtual std::string sign(const std::string& data) = 0;
  virtual uint32_t signatureLength() const = 0;
  virtual ~ISigner() = default;
  virtual std::string getPrivKey() const = 0;
};

class ECDSAVerifier : public IVerifier {
 public:
  ECDSAVerifier(const std::string& str_pub_key, KeyFormat fmt);
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
  ECDSASigner(const std::string& str_priv_key, KeyFormat fmt);
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
  RSAVerifier(const std::string& str_pub_key, KeyFormat fmt);
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
  RSASigner(const std::string& str_priv_key, KeyFormat fmt);
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
  std::pair<std::string, std::string> generateRsaKeyPair(const uint32_t sig_length, const KeyFormat fmt) const;
  std::pair<std::string, std::string> generateECDSAKeyPair(const KeyFormat fmt,
                                                           CurveType curve_type = CurveType::secp256k1) const;
  std::pair<std::string, std::string> RsaHexToPem(const std::pair<std::string, std::string>& key_pair) const;
  std::pair<std::string, std::string> ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) const;
  KeyFormat getFormat(const std::string& key_str) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace concord::util::crypto