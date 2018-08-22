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

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsMultisigAccumulator.h"
#include "threshsign/bls/relic/Library.h"

#include "Log.h"

#include <vector>

using std::endl;

namespace BLS {
namespace Relic {

BlsMultisigAccumulator::BlsMultisigAccumulator(NumSharesType totalSigners)
    : ThresholdAccumulatorBase(std::vector<BlsPublicKey>(), totalSigners, totalSigners)
{
    g1_set_infty(multiSig);    // set it to the identity element
}

void BlsMultisigAccumulator::onNewSigShareAdded(ShareID id, const G1T& sigShare) {
    logtrace << "Accumulating sigShare # " << id << " in BLS multisig: " << sigShare << std::endl;
    g1_add(multiSig, multiSig, sigShare);
}

} /* namespace Relic */
} /* namespace BLS */
