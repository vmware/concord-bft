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
//
// Design doc:
// https://confluence.eng.vmware.com/pages/viewpage.action?spaceKey=BLOC&title=Align+cryptographic+algorithms+across+Concord

#pragma once

namespace concord::util::digest {

// It is responsible for generating the digest.
class DigestCreator {
 public:
  virtual ~DigestCreator() = default;

  virtual void updateDigest(const char* data, const size_t len) = 0;
  virtual void writeDigest(char* outDigest) = 0;
  virtual size_t digestLength() const = 0;
  virtual bool computeDigest(const char* input,
                             const size_t inputLength,
                             char* outBufferForDigest,
                             const size_t lengthOfBufferForDigest) = 0;
};
}  // namespace concord::util::digest
