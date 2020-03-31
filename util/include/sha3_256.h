// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <array>

#include <openssl/evp.h>

#include "assertUtils.hpp"

namespace concord {
namespace util {

// A simple wrapper class around OpenSSL versions > 1.1.1, that implements
// SHA3-256.
class SHA3_256 {
 public:
  static constexpr size_t SIZE_IN_BYTES = 32;
  typedef std::array<uint8_t, SIZE_IN_BYTES> Digest;

  SHA3_256() : ctx_(EVP_MD_CTX_new()) { Assert(ctx_ != nullptr); }

  ~SHA3_256() { EVP_MD_CTX_destroy(ctx_); }

  // Don't allow copying
  SHA3_256(const SHA3_256&) = delete;
  SHA3_256& operator=(const SHA3_256&) = delete;

  // Compute a digest for an entire buffer and return an array containing the
  // digest.
  //
  // This is the simplest way to hash something as all setup is done for you.
  // Use the 3 method mechanism below if you need to hash multiple buffers.
  Digest digest(const void* buf, size_t size) {
    Assert(!updating_);
    Assert(EVP_MD_CTX_reset(ctx_) == 1);
    Assert(EVP_DigestInit_ex(ctx_, EVP_sha3_256(), NULL) == 1);

    Assert(EVP_DigestUpdate(ctx_, buf, size) == 1);

    Digest digest;
    unsigned int _digest_len;
    Assert(EVP_DigestFinal_ex(ctx_, digest.data(), &_digest_len) == 1);
    Assert(_digest_len == SIZE_IN_BYTES);
    return digest;
  }

  // The following 3 methods are used to compute digests piecemeal, by
  // continuously appending new data to be hashed.

  void init() {
    Assert(!updating_);
    Assert(EVP_MD_CTX_reset(ctx_) == 1);
    Assert(EVP_DigestInit_ex(ctx_, EVP_sha3_256(), NULL) == 1);
    updating_ = true;
  }

  // Add more data to a digest.
  void update(const void* buf, size_t size) {
    Assert(updating_);
    Assert(EVP_DigestUpdate(ctx_, buf, size) == 1);
  }

  Digest finish() {
    Digest digest;
    unsigned int _digest_len;
    Assert(EVP_DigestFinal_ex(ctx_, digest.data(), &_digest_len) == 1);
    Assert(_digest_len == SIZE_IN_BYTES);
    updating_ = false;
    return digest;
  }

 private:
  EVP_MD_CTX* ctx_;
  bool updating_ = false;
};

}  // namespace util
}  // namespace concord
