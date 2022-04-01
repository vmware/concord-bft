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

#include "digest_creator.hpp"
#include "digest.hpp"
#include "sha_hash.hpp"

namespace concord::util::digest {

template <typename SHACTX,
          typename = std::enable_if_t<std::is_same_v<SHACTX, concord::util::SHA2_256> ||
                                      std::is_same_v<SHACTX, concord::util::SHA3_256>>>
class OpenSSLDigestCreator : public DigestCreator {
 public:
  void init() {}
  void update(const char* data, size_t len) {}
  void finish(char* outDigest) {}
  size_t digestLength() { return 0; }
  bool compute(const char* input, size_t inputLength, char* outBufferForDigest, size_t lengthOfBufferForDigest) {
    return true;
  }

  virtual ~OpenSSLDigestCreator() {}

 private:
  SHACTX hash_ctx_;
};
}  // namespace concord::util::digest
