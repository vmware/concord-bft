// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <memory.h>
#include <stdint.h>
#include <string>

#include "SimpleBCStateTransfer.hpp"

namespace bftEngine {
namespace bcst {
namespace impl {
class STDigest : StateTransferDigest {
 public:
  STDigest() { memset(content, 0, BLOCK_DIGEST_SIZE); }
  STDigest(const char* other) { memcpy(content, other, BLOCK_DIGEST_SIZE); }

  // NOLINTNEXTLINE(bugprone-copy-constructor-init)
  STDigest(const STDigest& other) { memcpy(content, other.content, BLOCK_DIGEST_SIZE); }

  bool isZero() const {
    for (uint32_t i = 0; i < BLOCK_DIGEST_SIZE; i++) {
      if (content[i] != 0) return false;
    }
    return true;
  }

  bool operator==(const STDigest& other) const {
    int r = memcmp(content, other.content, BLOCK_DIGEST_SIZE);
    return (r == 0);
  }

  bool operator!=(const STDigest& other) const {
    int r = memcmp(content, other.content, BLOCK_DIGEST_SIZE);
    return (r != 0);
  }

  STDigest& operator=(const STDigest& other) {
    memcpy(content, other.content, BLOCK_DIGEST_SIZE);
    return *this;
  }

  void makeZero() { memset(content, 0, BLOCK_DIGEST_SIZE); }

  std::string toString() const;
  const char* const get() const { return content; }

  char* getForUpdate() { return content; }
};

inline std::ostream& operator<<(std::ostream& os, const STDigest& digest) {
  os << digest.toString();
  return os;
}

class DigestContext {
 public:
  DigestContext();

  void update(const char* data, size_t len);

  // write digest to outDigest, and invalidate the Context object
  void writeDigest(char* outDigest);

  ~DigestContext();

 protected:
  void* internalState;
};

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
