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

  SHA3_256() : ctx_(EVP_MD_CTX_new()) { Assert(ctx_ != nullptr); }

  ~SHA3_256() { EVP_MD_CTX_destroy(ctx_); }

  // Don't allow copying
  SHA3_256(const SHA3_256&) = delete;
  SHA3_256& operator=(const SHA3_256&) = delete;

  // Compute a digest for an entire buffer and return an array containing the
  // digest.
  std::array<uint8_t, SIZE_IN_BYTES> digest(const void* buf, size_t size) {
    Assert(EVP_MD_CTX_reset(ctx_) == 1);
    Assert(EVP_DigestInit_ex(ctx_, EVP_sha3_256(), NULL) == 1);

    if (size != 0) {
      Assert(EVP_DigestUpdate(ctx_, buf, size) == 1);
    }

    std::array<uint8_t, SIZE_IN_BYTES> digest;
    unsigned int _digest_len;
    Assert(EVP_DigestFinal_ex(ctx_, digest.data(), &_digest_len) == 1);
    Assert(_digest_len == SIZE_IN_BYTES);
    return digest;
  }

 private:
  EVP_MD_CTX* ctx_;
};

}  // namespace util
}  // namespace concord
