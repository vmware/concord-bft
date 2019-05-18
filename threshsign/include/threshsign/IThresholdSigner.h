// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include "threshsign/Serializable.h"
#include "ISecretKey.h"
#include "IPublicKey.h"

class IThresholdSigner : public Serializable {
 public:
  ~IThresholdSigner() override = default;

  virtual int requiredLengthForSignedData() const = 0;
  virtual void signData(const char *hash, int hashLen,
                        char *outSig, int outSigLen) = 0;

  virtual const IShareSecretKey &getShareSecretKey() const = 0;
  virtual const IShareVerificationKey &getShareVerificationKey() const = 0;

  // Serialization/deserialization
  virtual void serialize(UniquePtrToChar &outBuf, int64_t &outBufSize) const = 0;
};
