// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <threshsign/ThresholdSignaturesSchemes.h>
#include "PrimitiveTypes.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/integer.h>
#pragma GCC diagnostic pop

#include <memory>
#include <sstream>
#include <string>

using std::string;

namespace bftEngine {
namespace impl {
class CryptographyWrapper {
 public:
  static void init(const char* randomSeed);
  static void init();
};

// TODO(GG): define generic signer/verifier (not sure we want to use RSA)

class ISigner {
 public:
  virtual std::string sign(const std::string& data_to_sign) = 0;
  virtual ~ISigner() = default;
};

class IVerifier {
 public:
  virtual bool verify(const std::string& data_to_verify, const std::string& signature) = 0;
  virtual ~IVerifier() = default;
};

class ECDSASigner : public ISigner {
 public:
  ECDSASigner(const std::string& path_to_pem_file);
  ~ECDSASigner();
  std::string sign(const std::string& data_to_sign) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class ECDSAVerifier : public IVerifier {
 public:
  ECDSAVerifier(const std::string& path_to_pem_file);
  ~ECDSAVerifier();
  bool verify(const std::string& data_to_verify, const std::string& signature);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class RSASigner {
 public:
  RSASigner(const char* privteKey, const char* randomSeed);
  explicit RSASigner(const char* privateKey, KeyFormat format = KeyFormat::HexaDecimalStrippedFormat);
  RSASigner(const RSASigner&) = delete;
  RSASigner(RSASigner&&);
  ~RSASigner();
  RSASigner& operator=(const RSASigner&) = delete;
  RSASigner& operator=(RSASigner&&);
  size_t signatureLength() const;
  bool sign(const char* inBuffer,
            size_t lengthOfInBuffer,
            char* outBuffer,
            size_t lengthOfOutBuffer,
            size_t& lengthOfReturnedData) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class RSAVerifier {
 public:
  RSAVerifier(const char* publicKey, const char* randomSeed);
  explicit RSAVerifier(const char* publicKey, KeyFormat format = KeyFormat::HexaDecimalStrippedFormat);
  explicit RSAVerifier(const string& publicKeyPath);
  RSAVerifier(const RSAVerifier&) = delete;
  RSAVerifier(RSAVerifier&&);
  ~RSAVerifier();
  RSAVerifier& operator=(const RSAVerifier&) = delete;
  RSAVerifier& operator=(RSAVerifier&&);
  size_t signatureLength() const;
  bool verify(const char* data, size_t lengthOfData, const char* signature, size_t lengthOfOSignature) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class DigestUtil {
 public:
  static size_t digestLength();
  static bool compute(const char* input, size_t inputLength, char* outBufferForDigest, size_t lengthOfBufferForDigest);

  class Context {
   public:
    Context();
    void update(const char* data, size_t len);
    void writeDigest(char* outDigest);  // write digest to outDigest, and invalidate the Context object
    ~Context();

   private:
    void* internalState;
  };
};

}  // namespace impl
}  // namespace bftEngine
