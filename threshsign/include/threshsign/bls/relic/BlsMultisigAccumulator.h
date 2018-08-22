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

#include "threshsign/IThresholdAccumulator.h"
#include "threshsign/ThresholdAccumulatorBase.h"
#include "threshsign/ThresholdSignaturesTypes.h"

#include "BlsNumTypes.h"
#include "BlsThresholdAccumulator.h"
#include "BlsPublicKey.h"

#include <vector>

namespace BLS {
namespace Relic {

class BlsMultisigAccumulator : public ThresholdAccumulatorBase<BlsPublicKey, G1T, BlsSigshareParser> {
protected:
    G1T multiSig;

public:
    BlsMultisigAccumulator(NumSharesType totalSigners);
    virtual ~BlsMultisigAccumulator() {}

/**
 * IThresholdAccumulator overloads.
 */
public:
    virtual void getFullSignedData(char* outThreshSig, int threshSigLen) {
        sigToBytes(reinterpret_cast<unsigned char*>(outThreshSig), static_cast<int>(threshSigLen));
    }

    virtual IThresholdAccumulator* clone() {
        // Call copy constructor.
        return new BlsMultisigAccumulator(*this);
    }

    virtual bool hasShareVerificationEnabled() const { return false; }

/**
 * ThresholdAccumulatorBase overloads.
 */
public:
    virtual void onNewSigShareAdded(ShareID id, const G1T& sigShare);

    virtual G1T getFinalSignature() {
        return getMultiSignature();
    }

protected:
    virtual bool verifyShare(ShareID id, const G1T& sigShare) {
		(void)id;
		(void)sigShare;
		throw std::runtime_error("Unexpected verifyShare() call on BLS multisig accumulator");
	}

	virtual void onExpectedDigestSet() {}

/**
 * Used internally or for testing.
 */
public:
    const G1T& getMultiSignature() const { return multiSig; }

    void sigToBytes(unsigned char * buf, int len) {
        multiSig.toBytes(buf, len);
    }
};

} /* namespace Relic */
} /* namespace BLS */
