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

#include <type_traits>
#include <openssl/evp.h>
#include <cstdint>
#include <memory>

#include "sha_hash.hpp"
#include "DigestType.hpp"
#include "hex_tools.h"

namespace concord::util::digest {

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

class DigestCreator {
 public:
  virtual ~DigestCreator() = default;

  virtual void init() = 0;
  virtual void compute() = 0;
  virtual void update() = 0;
  virtual void final() = 0;
};

template <typename CREATOR, typename = std::enable_if_t<std::is_base_of_v<DigestCreator, CREATOR>>>
class DigestHolder {
 public:
  DigestHolder() { std::memset(d, 0, DIGEST_SIZE); }
  DigestHolder(unsigned char initVal) { std::memset(d, initVal, DIGEST_SIZE); }
  DigestHolder(const char* other) { std::memcpy(d, other, DIGEST_SIZE); }
  DigestHolder(char* buf, size_t len) { DigestUtil::compute(buf, len, (char*)d, DIGEST_SIZE); }
  DigestHolder(const DigestHolder& other) { std::memcpy(d, other.d, DIGEST_SIZE); }

  char* content() const { return (char*)d; }  // Can be replaced by getForUpdate().
  void makeZero() { std::memset(d, 0, DIGEST_SIZE); }
  std::string toString() const { return concordUtils::bufferToHex(d, DIGEST_SIZE, false); }
  void print() { printf("digest=[%s]", toString().c_str()); }
  const char* const get() const { return d; }
  char* getForUpdate() { return d; }

  bool isZero() const {
    for (int i = 0; i < DIGEST_SIZE; ++i) {
      if (d[i] != 0) return false;
    }
    return true;
  }

  int hash() const {
    uint64_t* p = (uint64_t*)d;
    int h = (int)p[0];
    return h;
  }

  bool operator==(const DigestHolder& other) const {
    int r = std::memcmp(d, other.d, DIGEST_SIZE);
    return (r == 0);
  }

  bool operator!=(const DigestHolder& other) const {
    int r = std::memcmp(d, other.d, DIGEST_SIZE);
    return (r != 0);
  }

  DigestHolder& operator=(const DigestHolder& other) {
    if (this == &other) {
      return *this;
    }
    std::memcpy(d, other.d, DIGEST_SIZE);
    return *this;
  }

  static void digestOfDigest(const DigestHolder& inDigest, DigestHolder& outDigest) {
    DigestUtil::compute(inDigest.d, sizeof(DigestHolder), outDigest.d, sizeof(DigestHolder));
  }

  static void calcCombination(const DigestHolder& inDigest, int64_t inDataA, int64_t inDataB, DigestHolder& outDigest) {
    const size_t X = ((DIGEST_SIZE / sizeof(uint64_t)) / 2);

    std::memcpy(outDigest.d, inDigest.d, DIGEST_SIZE);

    uint64_t* ptr = (uint64_t*)outDigest.d;
    size_t locationA = ptr[0] % X;
    size_t locationB = (ptr[0] >> 8) % X;
    ptr[locationA] = ptr[locationA] ^ (inDataA);
    ptr[locationB] = ptr[locationB] ^ (inDataB);
  }

 private:
  char d[DIGEST_SIZE];  // DIGEST_SIZE should be >= 8 bytes;  // Stores digest.
};

// Implements digest using Crypto++ library.
class CryptoppDigestCreator : public DigestCreator {
 public:
  void init() override {}
  void compute() override {}
  void update() override {}
  void final() override {}
  virtual ~CryptoppDigestCreator() = default;
};

// Implements digest using OpenSSL library.
template <typename SHACTX,
          typename = std::enable_if_t<std::is_same_v<SHACTX, concord::util::SHA2_256> ||
                                      std::is_same_v<SHACTX, concord::util::SHA3_256>>>
class OpenSSLDigestCreator : public DigestCreator {
 public:
  virtual ~OpenSSLDigestCreator() = default;
  void init() override {}
  void compute() override {}
  void update() override {}
  void final() override {}

 private:
  SHACTX hash_ctx_;
};
}  // namespace concord::util::digest
