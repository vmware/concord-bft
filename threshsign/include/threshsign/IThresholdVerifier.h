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

#include <cstddef>
#include <memory>

#include "Serializable.h"
#include "IPublicKey.h"
#include "ThresholdSignaturesTypes.h"
#include "IThresholdAccumulator.h"

class IThresholdVerifier {
 public:
  virtual ~IThresholdVerifier() = default;

 public:
  virtual IThresholdAccumulator *newAccumulator(bool withShareVerification) const = 0;

  virtual bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const = 0;
  virtual int requiredLengthForSignedData() const = 0;

  virtual const IPublicKey &getPublicKey() const = 0;
  virtual const IShareVerificationKey &getShareVerificationKey(ShareID signer) const = 0;

  static const uint32_t maxSize_ = 2048;
  static uint32_t maxSize() { return maxSize_; }
};
