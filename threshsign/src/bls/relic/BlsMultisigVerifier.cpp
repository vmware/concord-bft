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

#include "threshsign/bls/relic/BlsMultisigVerifier.h"
#include "threshsign/bls/relic/BlsMultisigAccumulator.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

namespace BLS {
namespace Relic {

BlsMultisigVerifier::BlsMultisigVerifier(const BlsPublicParameters& params, const G2T& pk,
        NumSharesType totalSigners, const std::vector<BlsPublicKey>& verifKeys)
    : BlsThresholdVerifier(params, pk, totalSigners, totalSigners, verifKeys)
{
}

BlsMultisigVerifier::~BlsMultisigVerifier() {
}

IThresholdAccumulator * BlsMultisigVerifier::newAccumulator(bool withShareVerification) const
{
    if(withShareVerification)
        std::runtime_error("BLS multisig has share verification disabled");
    return new BlsMultisigAccumulator(totalSigners);
}

} /* namespace Relic */
} /* namespace BLS */
