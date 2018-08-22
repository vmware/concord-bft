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

#ifdef ERROR // TODO(GG): should be fixed by encapsulating relic (or windows) definitions in cpp files
#undef ERROR
#endif


#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsThresholdAccumulator.h"
#include "threshsign/bls/relic/Library.h"

#include "BlsAlmostMultisigCoefficients.h"
#include "LagrangeInterpolation.h"

#include <vector>

#include "Log.h"

using std::endl;

namespace BLS {
namespace Relic {

template<class GT>
GT fastMultExp(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e, int maxBits) {
    return fastMultExp(s, s.first(), s.count(), a, e, maxBits);
}

template<class GT>
GT fastMultExp(const VectorOfShares& s, ShareID first, int count, const std::vector<GT>& a, const std::vector<BNT>& e, int maxBits) {
    GT r;
    assertEqual(r, GT::Identity());

    for(int j = maxBits - 1; j >= 0; j--) {
        r.Double();

        ShareID i = first;
        for(int c = 0; c < count; c++) {
            assertFalse(s.isEnd(i));

            size_t idx = static_cast<size_t>(i);
            assertLessThanOrEqual(e[idx].getBits(), maxBits);

            if(e[idx].getBit(j))
                r.Add(a[idx]);

            // Next share
            i = s.next(i);
        }
    }

    return r;
}

template G1T fastMultExp<G1T>(const VectorOfShares& s, const std::vector<G1T>& a, const std::vector<BNT>& e, int maxBits);
template G1T fastMultExp<G1T>(const VectorOfShares& s, ShareID first, int count, const std::vector<G1T>& a, const std::vector<BNT>& e, int maxBits);
template G2T fastMultExp<G2T>(const VectorOfShares& s, const std::vector<G2T>& a, const std::vector<BNT>& e, int maxBits);
template G2T fastMultExp<G2T>(const VectorOfShares& s, ShareID first, int count, const std::vector<G2T>& a, const std::vector<BNT>& e, int maxBits);


template<class GT>
GT fastMultExpTwo(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e) {
    GT r;
    assertEqual(r, GT::Identity());

    for(ShareID i = s.first(); s.isEnd(i) == false; i = s.next(s.next(i))) {
        ShareID j = s.next(i);
        size_t ii = static_cast<size_t>(i);

        if(s.isEnd(j) == false) {
            size_t jj = static_cast<size_t>(j);
            // r += a1^e1 + a2^e2
            r.Add(GT::TimesTwice(a[ii], e[ii], a[jj], e[jj]));
        } else {
            // r += a^e
            r.Add(GT::Times(a[ii], e[ii]));
        }
    }

    return r;
}

template G1T fastMultExpTwo<G1T>(const VectorOfShares& s, const std::vector<G1T>& a, const std::vector<BNT>& e);
template G2T fastMultExpTwo<G2T>(const VectorOfShares& s, const std::vector<G2T>& a, const std::vector<BNT>& e);

std::pair<ShareID, G1T> BlsSigshareParser::operator() (const char * sigShare, int len) {
    // Get the signer's index from the signature share
    BNT idNum(reinterpret_cast<const unsigned char*>(sigShare), sizeof(ShareID));
    ShareID id = static_cast<ShareID>(idNum.toDigit());

    // Get the sigshare
    G1T sigShareNum(reinterpret_cast<const unsigned char*>(sigShare + sizeof(ShareID)), len - static_cast<int>(sizeof(ShareID)));

    return std::make_pair(id, sigShareNum);
}

BlsThresholdAccumulator::BlsThresholdAccumulator(const std::vector<BlsPublicKey>& vks,
        NumSharesType reqSigners, NumSharesType totalSigners, bool withShareVerification)
    : ThresholdAccumulatorBase(vks, reqSigners, totalSigners),
      maxBits(Library::Get().getG2OrderNumBits()),
      shareVerificationEnabled(withShareVerification)
{
    coeffs.resize(static_cast<size_t>(totalSigners + 1));
    //sharesPow.resize(static_cast<size_t>(totalSigners + 1));
    assertEqual(vks.size(), static_cast<size_t>(totalSigners + 1));

    fieldSize = BLS::Relic::Library::Get().getG2Order();
    g2_get_gen(gen2);	// NOTE: requires BLS::Relic::Library::Get() call above to be made
}

void BlsThresholdAccumulator::computeLagrangeCoeff() {
    lagrangeCoeffAccumReduced(validSharesBits, coeffs, fieldSize);
}

void BlsThresholdAccumulator::onExpectedDigestSet() {
	assertNotNull(expectedDigest);
	assertStrictlyPositive(expectedDigestLen);

    g1_map(hash, expectedDigest, expectedDigestLen);
}

bool BlsThresholdAccumulator::verifyShare(ShareID id, const G1T& sigShare) {
	assertTrue(hasExpectedDigest());
	assertInclusiveRange(1, id, totalSigners);

	// Pair hash with PK
	// FIXME: RELIC: This should not require non-const y
	G2T& vk = const_cast<G2T&>(vks[static_cast<size_t>(id)].getPoint());
	//logtrace << "Checking hash " << hash << " against VK " << vk << endl;
	pc_map(e1, hash, vk);

	// Pair signature with group's generator
	// FIXME: RELIC: This should not require non-const sigShare
	pc_map(e2, const_cast<G1T&>(sigShare), gen2);

	// Make sure the two pairings are equal
	if (gt_cmp(e1, e2) != CMP_EQ) {
		return false;
	} else {
		return true;
	}
}

} /* namespace Relic */
} /* namespace BLS */
