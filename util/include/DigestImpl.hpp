// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <openssl/evp.h>
#include <memory>

#include "Digest.hpp"

namespace concord::util::digest {

class DigestCreator {
 public:
  virtual ~DigestCreator() = default;

  virtual void init() = 0;
  virtual void compute() = 0;
  virtual void update() = 0;
  virtual void final() = 0;
};

template <class T>  // Template 'T' must be sub-type of DigestCreator. Concept of sub-typing.
class DigestHolder {
  static_assert(std::is_base_of_v<DigestCreator, T>);  // Checks whether 'T' is a derived class of DigestCreator.
  std::array<uint8_t, DIGEST_SIZE> d;                  // Stores digest.

 public:
  std::unique_ptr<T> dhPtr;

  // All functions of Digest class.
  DigestHolder();
  DigestHolder(unsigned char initVal);
  DigestHolder(const char* other);
  DigestHolder(char* buf, size_t len);

  char* content() const;  // Can be replaced by getForUpdate().
  void makeZero();
  std::string toString() const;
  void print();
  const char* const get() const;
  char* getForUpdate();
  bool isZero() const;
  int hash() const;
  bool operator==(const DigestHolder& other) const;
  bool operator!=(const DigestHolder& other) const;
  DigestHolder& operator=(const DigestHolder& other);
  void digestOfDigest(const DigestHolder& inDigest, DigestHolder& outDigest);
};

// Implements digest using Crypto++ library.
class CryptoppDigestCreator : public DigestCreator {
 public:
  void init() {}
  void compute() {}
  void update() {}
  void final() {}
};

// Implements digest using OpenSSL library.
template <const EVP_MD* (*EVPMethod)()>
class OpenSSLDigestCreator : public DigestCreator {
  EVP_MD_CTX* ctx_;

 public:
  ~OpenSSLDigestCreator() {
    if (nullptr != ctx_) {
      EVP_MD_CTX_destroy(ctx_);
    }
  }
  void init() {}
  void compute() {}
  void update() {}
  void final() {}
};

#define USE_CRYPTOPP

#if defined USE_CRYPTOPP
using DGST = DigestHolder<CryptoppDigestCreator>;
#elif defined USE_OPENSSL_SHA256
using DGST = DigestHolder<OpenSSLDigestCreator<EVP_sha256> >;
#elif defined USE_OPENSSL_SHA3_256
using DGST = DigestHolder<OpenSSLDigestCreator<EVP_sha3_256> >;
#endif
}  // namespace concord::util::digest