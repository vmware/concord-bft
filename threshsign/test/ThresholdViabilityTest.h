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

#include <vector>
#include <memory>

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdFactory.h"
#include "threshsign/IThresholdAccumulator.h"
#include "threshsign/IPublicParameters.h"
#include "threshsign/VectorOfShares.h"

#include "Logger.hpp"
#include "Utils.h"
#include "AutoBuf.h"
#include "XAssert.h"

#pragma once

using std::endl;

template <class GroupType,
          class PublicParameters,
          class ThresholdAccumulator,
          class ThresholdSigner,
          class ThresholdVerifier>
class ThresholdViabilityTest {
 protected:
  const PublicParameters& params;
  // Threshold signers
  std::vector<IThresholdSigner*> sks;

  // Threshold verifier
  std::unique_ptr<IThresholdVerifier> verif;
  std::unique_ptr<IThresholdAccumulator> shareAccum;

  NumSharesType reqSigners, numSigners;
  bool verifiesShares;

 public:
  ThresholdViabilityTest(const PublicParameters& params, int n, int k)
      : params(params), reqSigners(k), numSigners(n), verifiesShares(false) {}

  virtual ~ThresholdViabilityTest() {
    for (auto it = sks.begin(); it != sks.end(); it++) delete *it;
  }

 public:
  virtual std::unique_ptr<IThresholdFactory> makeThresholdFactory() const = 0;
  virtual GroupType hashMessage(const unsigned char* msg, int msgSize) const = 0;

 public:
  void generateKeys() {
    std::unique_ptr<IThresholdFactory> factory = makeThresholdFactory();

    IThresholdVerifier* verifTmp;
    std::tie(sks, verifTmp) = factory->newRandomSigners(reqSigners, numSigners);
    verif.reset(verifTmp);
  }

  std::unique_ptr<IThresholdAccumulator> createAccumulator(bool withShareVerification) {
    return std::unique_ptr<IThresholdAccumulator>(verif->newAccumulator(withShareVerification));
  }

  ThresholdAccumulator* accumulator() { return dynamic_cast<ThresholdAccumulator*>(shareAccum.get()); }

  ThresholdSigner* signer(ShareID i) { return dynamic_cast<ThresholdSigner*>(sks[static_cast<size_t>(i)]); }

  ThresholdVerifier* verifier() { return dynamic_cast<ThresholdVerifier*>(verif.get()); }

  void test(const unsigned char* msg, int msgSize) {
    testAssertNotNull(verif);
    testAssertNotNull(verifier());

    LOG_INFO(THRESHSIGN_LOG,
             "Testing " << reqSigners << " out of " << numSigners << " signers. Verify shares: " << verifiesShares);
    GroupType h = hashMessage(msg, msgSize);

    VectorOfShares signers;
    VectorOfShares::randomSubset(signers, numSigners, reqSigners);

    // Deletes the old accumulator pointer, creates one without share verification
    shareAccum = createAccumulator(verifiesShares);
    testAssertNotNull(accumulator());
    // If we call this before adding the shares, the shares will be verified at addNumById() time
    shareAccum->setExpectedDigest(msg, msgSize);

    LOG_TRACE(THRESHSIGN_LOG, "Testing numerical API...");
    for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
      testAssertNotNull(signer(i));
      GroupType sigShare = signer(i)->signData(h);

      LOG_TRACE(THRESHSIGN_LOG, "Signed sigshare #" << i << ": " << sigShare);
      accumulator()->addNumById(i, sigShare);
    }

    checkThresholdSignature(msg, msgSize);

    /**
     * Test the char * API too.
     */
    LOG_TRACE(THRESHSIGN_LOG, "Testing char * API too...");

    // We want to call setExpectedDigest at 5 different points:
    //	- after accumulating the first share
    //	- after accumulating the last share
    //	- one other point in between
    std::vector<ShareID> points;
    points.push_back(signers.first());
    if (signers.count() > 1) {
      points.push_back(signers.last());
    }
    if (signers.count() > 2) {
      points.push_back(signers.ith(signers.count() / 2 + 1));
    }

    for (auto p = points.begin(); p != points.end(); p++) {
      // We test what happens when we call setExpectedDigest() after shares have been added.
      shareAccum = createAccumulator(verifiesShares);
      testAssertNotNull(accumulator());

      for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
        testAssertNotNull(signer(i));
        int shareLen = signer(i)->requiredLengthForSignedData();
        AutoCharBuf shareBuf(shareLen);
        signer(i)->signData(reinterpret_cast<const char*>(msg), msgSize, shareBuf, shareLen);

        shareAccum->add(shareBuf, shareLen);

        if (i == *p) {
          LOG_TRACE(THRESHSIGN_LOG, "Calling setExpectedDigest() after adding share " << i);
          shareAccum->setExpectedDigest(msg, msgSize);
        }
      }

      checkThresholdSignature(msg, msgSize);
    }
  }

  void checkThresholdSignature(const unsigned char* msg, int msgLen) {
    int sigLen = verifier()->requiredLengthForSignedData();
    AutoBuf<char> sig(sigLen);
    accumulator()->getFullSignedData(sig, sigLen);

    // LOG_DEBUG(THRESHSIGN_LOG, "Verifying signature(" << Utils::bin2hex(msg) << "): " << Utils::bin2hex(sig, sigLen));
    if (false == verifier()->verify(reinterpret_cast<const char*>(msg), msgLen, sig, sigLen)) {
      LOG_ERROR(THRESHSIGN_LOG,
                reqSigners << " out of " << numSigners << " signature " << threshsign::Utils::bin2hex(sig, sigLen)
                           << " did not verify");
      throw std::logic_error("Signature did not verify");
    }
  }
};
