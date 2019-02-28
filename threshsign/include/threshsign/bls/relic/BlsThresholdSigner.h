// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
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
  BlsPublicParameters params;

  BlsSecretKey sk;
  BlsPublicKey pk;

  ShareID id;                            // the ID of this signer
  unsigned char idBuf[sizeof(ShareID)];  // the serialized ID of this signer
  int sigSize;

  G1T hTmp,
      sigTmp;  // temporary storage for hashing message (avoids allocations)

 public:
  BlsThresholdSigner(const BlsPublicParameters& params,
                     ShareID id,
                     const BNT& sk);
  virtual ~BlsThresholdSigner() {}

 public:
  virtual int requiredLengthForSignedData() const {
    return sigSize + static_cast<int>(sizeof(id));
  }

  virtual void signData(const char* hash,
                        int hashLen,
                        char* outSig,
                        int outSigLen);

 public:
  /**
   * Used for testing and benchmarking.
   */
  G1T signData(const G1T& hashPoint) {
    g1_mul(sigTmp, hashPoint, sk.x);
    return sigTmp;
  }

  G1T signData(const unsigned char* msg, int msgLen) {
    G1T hashPoint;
    g1_map(hashPoint, msg, msgLen);

    return signData(hashPoint);
  }

  const IShareSecretKey& getShareSecretKey() const { return sk; }

  const BlsPublicKey& getPublicKey() const { return pk; }

  const IShareVerificationKey& getShareVerificationKey() const { return pk; }
};

} /* namespace Relic */
} /* namespace BLS */
