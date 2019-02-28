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

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <stdexcept>
#include <inttypes.h>

#include "Log.h"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"

using namespace std;

#include "threshsign/bls/relic/BlsThresholdScheme.h"
#include "bls/relic/BlsBatchVerifier.h"
#include "app/RelicMain.h"

using namespace BLS::Relic;

void batchVerifyHelper(BlsBatchVerifier& ver,
                       int numBadShares,
                       const G1T& msgPoint,
                       const VectorOfShares& badSubset,
                       const VectorOfShares& goodSubset,
                       bool checkRoot);

void runBatchVerificationTest(int k,
                              int n,
                              int maxShares,
                              int numBadShares = 0) {
  testAssertLessThanOrEqual(numBadShares, k);
  testAssertLessThanOrEqual(k, n);
  testAssertLessThanOrEqual(k, maxShares);

  BlsPublicParameters params = PublicParametersFactory::getWhatever();
  BlsThresholdFactory factory(params);
  std::vector<IThresholdSigner*> signers(static_cast<size_t>(n + 1));
  IThresholdVerifier* verifierTemp;

  std::tie(signers, verifierTemp) = factory.newRandomSigners(k, n);
  BlsThresholdVerifier* verifier =
      dynamic_cast<BlsThresholdVerifier*>(verifierTemp);
  BlsBatchVerifier batchVer(*verifier, maxShares);

  // Batch verifying shares from 1 to k
  VectorOfShares allSubset;
  for (ShareID id = 1; id <= k; id++) {
    allSubset.add(id);
  }

  // Pick a random subset of bad shares
  VectorOfShares badSubset;
  VectorOfShares::randomSubset(badSubset, k, numBadShares);

  // Compute remaining subset of good shares
  VectorOfShares goodSubset;
  for (ShareID id = 1; id <= k; id++) {
    if (badSubset.contains(id) == false) {
      goodSubset.add(id);
    }
  }

  const char* msg = "some message";
  int msgLen = static_cast<int>(strlen(msg));
  const unsigned char* buf = reinterpret_cast<const unsigned char*>(msg);

  for (ShareID id = allSubset.first(); allSubset.isEnd(id) == false;
       id = allSubset.next(id)) {
    BlsThresholdSigner* signer =
        dynamic_cast<BlsThresholdSigner*>(signers[static_cast<size_t>(id)]);

    logtrace << "Signing share #" << id << endl;
    G1T sig = signer->signData(buf, msgLen);

    if (badSubset.contains(id)) {
      logtrace << "Inserting bad share #" << id << std::endl;
      // Change the signature to sig=sig*2, making it invalid...
      sig.Double();
    }

    batchVer.addShare(id, sig);
  }

  G1T msgPoint;
  g1_map(msgPoint, buf, msgLen);

  // logdbg << "Verifying starting from root..." << std::endl;
  batchVerifyHelper(
      batchVer, numBadShares, msgPoint, badSubset, goodSubset, true);

  if (numBadShares > 0) {
    // logdbg << "Verifying but skipping root (since we have bad shares)..." <<
    // std::endl;
    batchVerifyHelper(
        batchVer, numBadShares, msgPoint, badSubset, goodSubset, false);
  }

  for (IThresholdSigner* signer : signers) delete signer;
  delete verifierTemp;
}

void batchVerifyHelper(BlsBatchVerifier& ver,
                       int numBadShares,
                       const G1T& msgPoint,
                       const VectorOfShares& badSubset,
                       const VectorOfShares& goodSubset,
                       bool checkRoot) {
  std::vector<ShareID> badShares;
  std::vector<ShareID> goodShares;

  bool shouldVerify = numBadShares == 0;

  // Find bad shares
  if (ver.batchVerify(msgPoint, true, badShares, checkRoot) != shouldVerify) {
    logerror << "Expected batch verification to return '"
             << (shouldVerify ? "true" : "false") << "'" << endl;
    throw std::logic_error("batchVerify() returned wrong result");
  }

  testAssertEqual(badShares.size(),
                  static_cast<std::vector<ShareID>::size_type>(numBadShares));
  testAssertEqual(
      badShares.size(),
      static_cast<std::vector<ShareID>::size_type>(badSubset.count()));
  for (ShareID id : badShares) {
    testAssertTrue(badSubset.contains(id));
  }

  // Find good shares
  if (ver.batchVerify(msgPoint, false, goodShares, checkRoot) != shouldVerify) {
    logerror << "Expected batch verification to return '"
             << (shouldVerify ? "true" : "false") << "'" << endl;
    throw std::logic_error("batchVerify() returned wrong result");
  }

  testAssertEqual(
      goodShares.size(),
      static_cast<std::vector<ShareID>::size_type>(goodSubset.count()));
  for (ShareID id : goodShares) {
    testAssertTrue(goodSubset.contains(id));
  }

#ifdef TRACE
  if (numBadShares > 0) {
    std::cout << "Found all " << numBadShares << " bad shares: ";
    std::copy(badShares.begin(),
              badShares.end(),
              std::ostream_iterator<ShareID>(std::cout, " "));
    std::cout << endl;
  }
#endif
}

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)args;
  (void)lib;

  for (int k = 1; k < 17; k++) {
    int n = k + 2;
    logdbg << "Testing the BLS batch verifier with k = " << k
           << " and n = " << n << endl;
    for (int bad = 0; bad <= k; bad++) {
      // logdbg << " * numBadShares = " << bad << endl;
      runBatchVerificationTest(k, n, k, bad);
      runBatchVerificationTest(k, n, n, bad);
      runBatchVerificationTest(k, n, MAX_NUM_OF_SHARES, bad);
    }
  }

  return 0;
}
