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

#include <vector>

#include "ThresholdSignaturesTypes.h"

template <class SK, class PK>
class IThresholdKeygen {
 protected:
  SK sk;
  PK pk;
  std::vector<SK> skShares;
  std::vector<PK> pkShares;
  NumSharesType numSigners;

 public:
  IThresholdKeygen(NumSharesType numSigners)
      : skShares(static_cast<size_t>(numSigners + 1)),
        pkShares(static_cast<size_t>(numSigners + 1)),
        numSigners(numSigners) {}

  virtual ~IThresholdKeygen() {}

 public:
  NumSharesType getNumSigners() const { return numSigners; }
  const SK& getSecretKey() const { return sk; }
  const PK& getPublicKey() const { return pk; }

  const SK& getShareSecretKey(ShareID i) const { return skShares[static_cast<size_t>(i)]; }
  const PK& getShareVerificationKey(ShareID i) const { return pkShares[static_cast<size_t>(i)]; }

  const std::vector<SK>& getShareSecretKeys() const { return skShares; }
  const std::vector<PK>& getShareVerificationKeys() const { return pkShares; }
};
