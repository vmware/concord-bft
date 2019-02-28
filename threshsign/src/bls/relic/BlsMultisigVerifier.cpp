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

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsMultisigVerifier.h"
#include "threshsign/bls/relic/BlsMultisigAccumulator.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

#include "threshsign/VectorOfShares.h"

#include "Log.h"
#include "XAssert.h"
using std::endl;

namespace BLS {
namespace Relic {

BlsMultisigVerifier::BlsMultisigVerifier(
    const BlsPublicParameters& params,
    NumSharesType reqSigners,
    NumSharesType numSigners,
    const std::vector<BlsPublicKey>& verifKeys)
    : BlsThresholdVerifier(
          params, G2T::Identity(), reqSigners, numSigners, verifKeys) {
  assertEqual(
      verifKeys.size(),
      static_cast<std::vector<BlsPublicKey>::size_type>(numSigners + 1));

  if (reqSigners == numSigners) {
    // the PK is the aggregate PK of all numSigners and is needed to verify
    // n-out-of-n threshold
    for (auto& vk : verifKeys) {
      pk.y.Add(vk.getPoint());
    }
  } else {
    // the PK is computed dynamically based on the signer IDs in the signature
  }
}

IThresholdAccumulator* BlsMultisigVerifier::newAccumulator(
    bool withShareVerification) const {
  if (reqSigners == numSigners && withShareVerification) {
    logwarn << "BLS n-out-of-n multisig typically has share verification "
               "disabled in Concord. Are you sure you need this?"
            << endl;
  }

  return new BlsMultisigAccumulator(
      vks, reqSigners, numSigners, withShareVerification);
}

/**
 * NOTE(Alin): There are many other ways of encoding the signer IDs along the
 * signature. For simplicity, we just serialize the bit vector of signer IDs.
 * However, if more efficient variable-length encodings are to be used, then the
 * API must change. Right now, IThresholdVerifier::requiredLengthForSignedData()
 * is used to fetch the signature size, which is "too early": not enough info
 * about the signer IDs to determine the variable size of the signature.
 */
int BlsMultisigVerifier::requiredLengthForSignedData() const {
  int sigSize = params.getSignatureSize();

  if (reqSigners != numSigners) sigSize += VectorOfShares::getByteCount();

  return sigSize;
}

bool BlsMultisigVerifier::verify(const char* msg,
                                 int msgLen,
                                 const char* sigBuf,
                                 int sigLen) const {
  // Parse the signer IDs from sigBuf and adjust the PK
  if (reqSigners != numSigners) {
    if (sigLen != requiredLengthForSignedData()) {
      throw std::runtime_error("Signature does not have the right size");
    }

    // need to parse out signer IDs
    VectorOfShares signers;
    const char* idbuf = sigBuf + params.getSignatureSize();
    int idbufLen = VectorOfShares::getByteCount();
    signers.fromBytes(reinterpret_cast<const unsigned char*>(idbuf), idbufLen);

    // for reqSigners != numSigners, need to derive PK from signer IDs
    if (reqSigners != numSigners) {
      pk = G2T::Identity();
      for (ShareID id = signers.first(); signers.isEnd(id) == false;
           id = signers.next(id)) {
        size_t idx = static_cast<size_t>(id);
        pk.y.Add(vks[idx].getPoint());
      }
    }
  }

  // Once the PK is set in 'pk' can call parent BlsThresholdVerifier to verify
  // the sig
  return BlsThresholdVerifier::verify(
      msg, msgLen, sigBuf, params.getSignatureSize());
}

} /* namespace Relic */
} /* namespace BLS */
