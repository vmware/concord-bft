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

#include <memory.h>
#include <stdint.h>
#include <string>
#include <array>
#include "DigestType.hpp"

namespace concord::util::digest {

using BlockDigest = std::array<std::uint8_t, DIGEST_SIZE>;

class Digest {
 public:
  Digest() { memset(d, 0, DIGEST_SIZE); }

  Digest(unsigned char initVal) { memset(d, initVal, DIGEST_SIZE); }

  Digest(const char* other) { memcpy(d, other, DIGEST_SIZE); }

  Digest(char* buf, size_t len);

  Digest(const Digest& other) { memcpy(d, other.d, DIGEST_SIZE); }

  bool isZero() const {
    for (int i = 0; i < DIGEST_SIZE; i++) {
      if (d[i] != 0) return false;
    }
    return true;
  }

  bool operator==(const Digest& other) const {
    int r = memcmp(d, other.d, DIGEST_SIZE);
    return (r == 0);
  }

  bool operator!=(const Digest& other) const {
    int r = memcmp(d, other.d, DIGEST_SIZE);
    return (r != 0);
  }

  Digest& operator=(const Digest& other) {
    if (this == &other) {
      return *this;
    }
    memcpy(d, other.d, DIGEST_SIZE);
    return *this;
  }

  int hash() const {
    uint64_t* p = (uint64_t*)d;
    int h = (int)p[0];
    return h;
  }

  void makeZero() { memset(d, 0, DIGEST_SIZE); }

  char* content() const { return (char*)d; }

  std::string toString() const;

  void print();

  static void calcCombination(const Digest& inDigest,
                              int64_t inDataA,
                              int64_t inDataB,
                              Digest& outDigest)  // TODO(GG): consider to change this function (TBD - check security)
  {
    const size_t X = ((DIGEST_SIZE / sizeof(uint64_t)) / 2);

    memcpy(outDigest.d, inDigest.d, DIGEST_SIZE);

    uint64_t* ptr = (uint64_t*)outDigest.d;
    size_t locationA = ptr[0] % X;
    size_t locationB = (ptr[0] >> 8) % X;
    ptr[locationA] = ptr[locationA] ^ (inDataA);
    ptr[locationB] = ptr[locationB] ^ (inDataB);
  }

  static void digestOfDigest(const Digest& inDigest, Digest& outDigest);

  const char* const get() const { return d; }

  char* getForUpdate() { return d; }

 protected:
  char d[DIGEST_SIZE];  // DIGEST_SIZE should be >= 8 bytes
};

static_assert(DIGEST_SIZE >= sizeof(uint64_t), "Digest size should be >= sizeof(uint64_t)");
static_assert(sizeof(Digest) == DIGEST_SIZE, "sizeof(Digest) != DIGEST_SIZE");

inline std::ostream& operator<<(std::ostream& os, const Digest& digest) {
  os << digest.toString();
  return os;
}

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
}  // namespace concord::util::digest