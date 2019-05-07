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

#include "threshsign/IThresholdVerifier.h"
#include "threshsign/IThresholdAccumulator.h"

#include "BlsPublicKey.h"
#include "BlsNumTypes.h"
#include "BlsPublicParameters.h"

#include <vector>
#include <memory>

namespace BLS {
namespace Relic {

class BlsThresholdVerifier : public IThresholdVerifier {
 protected:
  const BlsPublicParameters params;

  mutable BlsPublicKey pk;
  const std::vector<BlsPublicKey> vks;
  const G2T gen2;
  const NumSharesType reqSigners, numSigners;

 public:
  BlsThresholdVerifier(const BlsPublicParameters &params, const G2T &pk,
                       NumSharesType reqSigners, NumSharesType numSigners,
                       const std::vector<BlsPublicKey> &verifKeys);

  ~BlsThresholdVerifier() override = default;

  /**
   * For testing and internal use.
   */
 public:
  void serialize(std::ostream &) const override {}
  void deserialize(std::istream &) const override {}

  NumSharesType getNumRequiredShares() const { return reqSigners; }
  NumSharesType getNumTotalShares() const { return numSigners; }
  const BlsPublicParameters &getParams() const { return params; }
  /**
   * NOTE: Used by BlsBatchVerifier to verify shares
   */
  bool verify(const G1T &msgHash, const G1T &sigShare, const G2T &pk) const;

  /**
   * IThresholdVerifier overrides.
   */
 public:
  IThresholdAccumulator *newAccumulator(bool withShareVerification)
  const override;

  void release(IThresholdAccumulator *acc) override {
    delete acc;
  }

  bool verify(const char *msg, int msgLen, const char *sig, int sigLen)
  const override;

  int requiredLengthForSignedData() const override {
    return params.getSignatureSize();
  }

  const IPublicKey &getPublicKey() const override { return pk; }

  const IShareVerificationKey &getShareVerificationKey(ShareID signer)
  const override;
};

} /* namespace Relic */
} /* namespace BLS */
