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

#include "threshsign/bls/relic/BlsMultisigAccumulator.h"
#include "threshsign/bls/relic/Library.h"

#include "Logger.hpp"
#include "XAssert.h"

#include <vector>

namespace BLS {
namespace Relic {

BlsMultisigAccumulator::BlsMultisigAccumulator(const std::vector<BlsPublicKey>& verifKeys,
                                               NumSharesType reqSigners,
                                               NumSharesType totalSigners,
                                               bool withShareVerification)
    : BlsAccumulatorBase(verifKeys, reqSigners, totalSigners, withShareVerification) {}

size_t BlsMultisigAccumulator::getFullSignedData(char* outThreshSig, int threshSigLen) {
  aggregateShares();

  return sigToBytes(reinterpret_cast<unsigned char*>(outThreshSig), threshSigLen);
}

size_t BlsMultisigAccumulator::sigToBytes(unsigned char* outThreshSig, int threshSigLen) const {
  int sigSize = Library::Get().getG1PointSize();
  int vectorSize = 0;

  if (reqSigners != totalSigners) {
    vectorSize = VectorOfShares::getByteCount();
  }

  if (threshSigLen < sigSize + vectorSize) {
    throw std::runtime_error("Not enough capacity to store multisignature");
  }

  // include the signature itself
  threshSig.toBytes(reinterpret_cast<unsigned char*>(outThreshSig), sigSize);

  if (reqSigners != totalSigners) {
    // include the signer IDs
    validSharesBits.toBytes(reinterpret_cast<unsigned char*>(outThreshSig + sigSize), vectorSize);
  }

  return static_cast<size_t>(sigSize + vectorSize);
}

void BlsMultisigAccumulator::aggregateShares() {
  threshSig = G1T::Identity();

  // multiply all the signature shares to obtain the multisig
  for (ShareID id = validSharesBits.first(); validSharesBits.isEnd(id) == false; id = validSharesBits.next(id)) {
    size_t i = static_cast<size_t>(id);
    g1_add(threshSig, threshSig, validShares[i]);
  }
}

} /* namespace Relic */
} /* namespace BLS */
