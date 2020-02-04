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

class RSASigner {
 public:
  RSASigner(const char* privteKey, const char* randomSeed);
  RSASigner(const char* privateKey);
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
  std::unique_ptr<Impl> impl;
};

class RSAVerifier {
 public:
  RSAVerifier(const char* publicKey, const char* randomSeed);
  RSAVerifier(const char* publicKey);
  RSAVerifier(const RSAVerifier&) = delete;
  RSAVerifier(RSAVerifier&&);
  ~RSAVerifier();
  RSAVerifier& operator=(const RSAVerifier&) = delete;
  RSAVerifier& operator=(RSAVerifier&&);
  size_t signatureLength() const;
  bool verify(const char* data, size_t lengthOfData, const char* signature, size_t lengthOfOSignature) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl;
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
