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

namespace concord::util::digest {

// It is responsible for generating the digest.
class DigestCreator {
 public:
  virtual ~DigestCreator() = default;

  virtual void init() = 0;
  virtual void update(const char* data, size_t len) = 0;
  virtual void finish(char* outDigest) = 0;
  virtual size_t digestLength() = 0;
  virtual bool compute(const char* input,
                       size_t inputLength,
                       char* outBufferForDigest,
                       size_t lengthOfBufferForDigest) = 0;
};
}  // namespace concord::util::digest
