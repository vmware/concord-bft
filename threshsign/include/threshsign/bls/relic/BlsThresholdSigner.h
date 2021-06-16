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

#include "threshsign/IThresholdSigner.h"
#include "threshsign/ThresholdSignaturesTypes.h"

#include "BlsSecretKey.h"
#include "BlsPublicKey.h"
#include "BlsNumTypes.h"
#include "BlsPublicParameters.h"

#pragma once

namespace BLS {
namespace Relic {

/**
 * Threshold signature signer and multisignature signer class.
 */
class BlsThresholdSigner : public IThresholdSigner {
 protected:
  BlsPublicParameters params_;

  BlsSecretKey secretKey_;
  BlsPublicKey publicKey_;

  ShareID id_ = 0;  // ID of this signer
  int sigSize_ = 0;
  unsigned char serializedId_[sizeof(ShareID)];  // Serialized ID of the signer

  // Temporary storage for hashing message (avoids allocations)
  G1T hTmp_, sigTmp_;

 public:
  BlsThresholdSigner() = default;
  BlsThresholdSigner(const BlsPublicParameters &params, ShareID id, const BNT &secretKey);
  ~BlsThresholdSigner() override = default;

  bool operator==(const BlsThresholdSigner &other) const;

  int requiredLengthForSignedData() const override { return sigSize_ + static_cast<int>(sizeof(id_)); }

  void signData(const char *hash, int hashLen, char *outSig, int outSigLen) override;

  /**
   * Used for testing and benchmarking.
   */
  G1T signData(const G1T &hashPoint) {
    g1_mul(sigTmp_, hashPoint, secretKey_.x);
    return sigTmp_;
  }

  G1T signData(const unsigned char *msg, int msgLen) {
    G1T hashPoint;
    g1_map(hashPoint, msg, msgLen);
    return signData(hashPoint);
  }

  const IShareSecretKey &getShareSecretKey() const override { return secretKey_; }

  const BlsPublicKey &getPublicKey() const { return publicKey_; }

  const IShareVerificationKey &getShareVerificationKey() const override { return publicKey_; }
};

} /* namespace Relic */
} /* namespace BLS */
