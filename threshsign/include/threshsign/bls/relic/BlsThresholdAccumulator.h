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
#include "threshsign/VectorOfShares.h"

#include "BlsNumTypes.h"
#include "BlsPublicKey.h"

#include <vector>

namespace BLS {
namespace Relic {

/**
 * Computes r = \prod_{i \in s} { a[i]^e[i] } faster than naive method.
 * Thanks to "Fast Batch Verification for Modular Exponentiation and Digital Signatures"
 */
template<class GT>
GT fastMultExp(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e, int maxBits);
template<class GT>
GT fastMultExp(const VectorOfShares& s, ShareID first, int count, const std::vector<GT>& a, const std::vector<BNT>& e, int maxBits);

/**
 * Computes r = \prod_{i \in s} { a[i]^e[i] } faster than naive method by recursing on
 * a fast way to compute a1^e1 * a2^e2.
 *
 * NOTE: Slower than fastMultExp above.
 */
template<class GT>
GT fastMultExpTwo(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e);

// TODO: move this somewhere else?
class BlsSigshareParser {
public:
    std::pair<ShareID, G1T> operator() (const char * sigShare, int len);
};

/**
 * TODO: Threshold accumulators store too much state that could be kept in the parent IThresholdVerifier:
 * 	fieldSize, gen2, maxBits
 */
class BlsThresholdAccumulator : public ThresholdAccumulatorBase<BlsPublicKey, G1T, BlsSigshareParser> {
protected:
    std::vector<BNT> coeffs;    // coefficients l_i^S(0) for the current set of validSharesBits S
    //std::vector<G1T> sharesPow; // sharesPow[i] = shares[i]^coeffs[i] (e.g., (H(m)^{s_i})^{l_i^S(0)}, where S is the set of validSharesBits)

    BNT fieldSize;
    G1T threshSig;
    G1T hash;		// expected hash of the message being signed

    G2T gen2;
	GTT e1, e2;		// used to store pairing results
	int maxBits;

    // True if share verification is enabled
    bool shareVerificationEnabled;

public:
    BlsThresholdAccumulator(const std::vector<BlsPublicKey>& vks,
            NumSharesType reqSigners, NumSharesType totalSigners,
            bool withShareVerification);
    virtual ~BlsThresholdAccumulator() {}

/**
 * IThresholdAccumulator overloads.
 */
public:
    virtual void getFullSignedData(char* outThreshSig, int threshSigLen) {
        computeLagrangeCoeff();
        exponentiateLagrangeCoeff();
        aggregateShares();

        sigToBytes(reinterpret_cast<unsigned char*>(outThreshSig), threshSigLen);
    }

    virtual IThresholdAccumulator* clone() { return new BlsThresholdAccumulator(*this); }

    virtual bool hasShareVerificationEnabled() const { return shareVerificationEnabled; }

/**
 * ThresholdAccumulatorBase overloads.
 */
public:
    virtual void onNewSigShareAdded(ShareID id, const G1T& sigShare) {
        // Do nothing. We aggregate all shares at the end.
        (void)id;
        (void)sigShare;
    }

    virtual G1T getFinalSignature() {
        computeLagrangeCoeff();
        exponentiateLagrangeCoeff();
        aggregateShares();

        return getThresholdSignature();
    }

protected:
    virtual bool verifyShare(ShareID id, const G1T& sigShare);
    virtual void onExpectedDigestSet();

/**
 * Used internally or for testing
 */
public:
    virtual void computeLagrangeCoeff();

    void exponentiateLagrangeCoeff() {
//        // Raise shares[i] to the power of coeffs[i]
//        for(ShareID id = validSharesBits.first(); validSharesBits.isEnd(id) == false; id = validSharesBits.next(id)) {
//            size_t i = static_cast<size_t>(id);
//            g1_mul(sharesPow[i], validShares[i], coeffs[i]);
//        }
    }

    void aggregateShares() {
        g1_set_infty(threshSig);    // set it to the identity element

//        for(ShareID id = validSharesBits.first(); validSharesBits.isEnd(id) == false; id = validSharesBits.next(id)) {
//            size_t i = static_cast<size_t>(id);
//            g1_add(threshSig, threshSig, sharesPow[i]);
//        }
        threshSig = fastMultExp<G1T>(validSharesBits, validShares, coeffs, maxBits);
    }

    const G1T& getThresholdSignature() const { return threshSig; }

    void sigToBytes(unsigned char * buf, int len) { threshSig.toBytes(buf, len); }
};

} /* namespace Relic */
} /* namespace BLS */
