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
  BlsPublicParameters params_;
  // For multisig publicKey_ is used only for n-out-of-n case
  // TODO [TK] mutable for deserialization - remove as we don't need to serialize the entire class
  mutable BlsPublicKey publicKey_;
  std::vector<BlsPublicKey> publicKeysVector_;
  G2T generator2_;
  NumSharesType reqSigners_ = 0, numSigners_ = 0;

 public:
  BlsThresholdVerifier(const BlsPublicParameters &params,
                       const G2T &pk,
                       NumSharesType reqSigners,
                       NumSharesType numSigners,
                       const std::vector<BlsPublicKey> &verificationKeys);

  ~BlsThresholdVerifier() override = default;

  bool operator==(const BlsThresholdVerifier &other) const;
  bool compare(const BlsThresholdVerifier &other) const { return (*this == other); }

  /**
   * For testing and internal use.
   */
  NumSharesType getNumRequiredShares() const { return reqSigners_; }
  NumSharesType getNumTotalShares() const { return numSigners_; }
  std::vector<BlsPublicKey> getPublicKeysVector() const { return publicKeysVector_; }
  const BlsPublicParameters &getParams() const { return params_; }
  const BlsPublicKey getKey() const { return publicKey_; }
  /**
   * NOTE: Used by BlsBatchVerifier to verify shares
   */
  bool verify(const G1T &msgHash, const G1T &sigShare, const G2T &pk) const;

  /**
   * IThresholdVerifier overrides.
   */
  IThresholdAccumulator *newAccumulator(bool withShareVerification) const override;

  bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const override;

  int requiredLengthForSignedData() const override { return params_.getSignatureSize(); }

  const IPublicKey &getPublicKey() const override { return publicKey_; }

  const IShareVerificationKey &getShareVerificationKey(ShareID signer) const override;

 protected:
  BlsThresholdVerifier() = default;
};

} /* namespace Relic */
} /* namespace BLS */
