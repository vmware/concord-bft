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

#include "digest_creator.hpp"
#include "digest_type.hpp"

#if defined MD5_DIGEST
#include <cryptopp/md5.h>
#define DigestType Weak1::MD5
#elif defined SHA256_DIGEST
#define DigestType CryptoPP::SHA256
#elif defined SHA512_DIGEST
#define DigestType SHA512
#endif

namespace concord::util::digest {

// Implements digest creator using Crypto++ library.
class CryptoppDigestCreator : public DigestCreator {
 public:
  CryptoppDigestCreator();
  virtual ~CryptoppDigestCreator();

  void init() override {}
  void update(const char* data, size_t len) override;
  void writeDigest(char* outDigest) override;
  size_t digestLength() const override;
  bool compute(const char* input,
               size_t inputLength,
               char* outBufferForDigest,
               size_t lengthOfBufferForDigest) override;

 private:
  void* internalState_;
};
}  // namespace concord::util::digest
