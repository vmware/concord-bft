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

#include "hash.hpp"
#include "crypto/crypto.hpp"
#include "crypto/digest_creator.hpp"

namespace concord::crypto::openssl {

template <typename SHACTX,
          typename = std::enable_if_t<std::is_same_v<SHACTX, concord::crypto::SHA2_256> ||
                                      std::is_same_v<SHACTX, concord::crypto::SHA3_256>>>
class OpenSSLDigestCreator : public concord::crypto::DigestCreator {
 public:
  OpenSSLDigestCreator() = default;

  // Do not allow copying.
  OpenSSLDigestCreator(const OpenSSLDigestCreator&) = delete;
  OpenSSLDigestCreator& operator=(const OpenSSLDigestCreator&) = delete;

  void init() {
    if (!initialized_) {
      initialized_ = true;
      hash_ctx_.init();
    }
  }

  void update(const char* data, size_t len) {
    ConcordAssert(nullptr != data);

    init();
    hash_ctx_.update(data, len);
  }

  void writeDigest(char* outDigest) {
    ConcordAssert(nullptr != outDigest);

    initialized_ = false;
    const auto digest = hash_ctx_.finish();
    memcpy(outDigest, std::string(digest.begin(), digest.end()).c_str(), hash_ctx_.SIZE_IN_BYTES);
  }

  size_t digestLength() const { return hash_ctx_.SIZE_IN_BYTES; }

  bool compute(const char* input, size_t inputLength, char* outBufferForDigest, size_t lengthOfBufferForDigest) {
    ConcordAssert(nullptr != input);
    ConcordAssert(nullptr != outBufferForDigest);

    if (lengthOfBufferForDigest < hash_ctx_.SIZE_IN_BYTES) {
      return false;
    }
    const auto digest = hash_ctx_.digest(input, inputLength);
    memcpy(outBufferForDigest, std::string(digest.begin(), digest.end()).c_str(), hash_ctx_.SIZE_IN_BYTES);

    return true;
  }

 private:
  bool initialized_{false};
  SHACTX hash_ctx_;
};

}  // namespace concord::crypto::openssl

namespace concord::crypto {

using DigestGenerator = concord::crypto::openssl::OpenSSLDigestCreator<concord::crypto::SHA2_256>;
}  // namespace concord::crypto
