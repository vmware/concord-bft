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

#include "Logger.hpp"
#include "Utils.h"

#include <iostream>
#include <fstream>
#include <vector>

#include "lib/IThresholdSchemeBenchmark.h"
#include "lib/Benchmark.h"

#include "app/RelicMain.h"
#include "bls/relic/LagrangeInterpolation.h"

#include "threshsign/bls/relic/BlsThresholdScheme.h"

extern "C" {
#include <relic/relic.h>
#include <relic/relic_err.h>
}

using namespace BLS::Relic;
using std::endl;

class ThresholdBlsRelicBenchmark : public IThresholdSchemeBenchmark {
 private:
  G1T h, sig;
  char* threshSig;
  G2T pk, gen2;
  BNT sk;

  // to store them in an std::vector: neither std::vector<bn_t> nor std::vector<bn_t*> work.
  std::vector<IThresholdSigner*> sks;  // secret key shares for all signers
  std::unique_ptr<IThresholdVerifier> verifier;
  std::unique_ptr<IThresholdAccumulator> accum;

  std::vector<BlsPublicKey> pks;  // public keys (corresp. to SK shares) for all signers
  std::vector<char*> shares;      // signature shares for all signers

  bool useMultisig;

 public:
  ThresholdBlsRelicBenchmark(const BlsPublicParameters& p, int k, int n, bool useMultisig)
      : IThresholdSchemeBenchmark(p, k, n), threshSig(nullptr), useMultisig(useMultisig) {
    // numBenchIters = 2;
    assertStrictlyGreaterThan(k, 0);
    assertLessThanOrEqual(k, n);
    // WARNING: Players are numbered from 1 to n (pos [0] is irrelevant in these arrays)
    sks.resize(static_cast<size_t>(numSigners + 1));
    pks.resize(static_cast<size_t>(numSigners + 1));
    shares.resize(static_cast<size_t>(numSigners + 1), nullptr);

    g2_get_gen(gen2);

    generateKeys();
    LOG_INFO(THRESHSIGN_LOG,
             "Created new threshold BLS benchmark: numIters = " << numBenchIters << ", k = " << k << ", n = " << n
                                                                << ", sec = " << p.getSecurityLevel() << " bits");
  }

  ~ThresholdBlsRelicBenchmark() {
    for (auto& sk : sks) {
      delete sk;
    }

    for (auto& sigShare : shares) {
      delete[] sigShare;
    }

    delete[] threshSig;
  }

 protected:
  void generateKeys() {
    const BlsPublicParameters& blsParams = dynamic_cast<const BLS::Relic::BlsPublicParameters&>(params);
    sk.RandomMod(blsParams.getGroupOrder());
    pk = G2T::Times(gen2, sk);

    BlsThresholdFactory factory(blsParams, useMultisig);
    IThresholdVerifier* verifTmp;
    std::tie(sks, verifTmp) = factory.newRandomSigners(reqSigners, numSigners);
    verifier.reset(verifTmp);

    // Compute SK size
    skBits = bn_size_bin(sk);
    LOG_DEBUG(THRESHSIGN_LOG, "SK size: " << skBits << " bytes");

    // Compute PK size
    pkBits = g2_size_bin(pk, 1);
    LOG_DEBUG(THRESHSIGN_LOG, "PK size (compressed): " << pkBits << " bytes");
    LOG_DEBUG(THRESHSIGN_LOG, "PK size (uncompressed): " << g2_size_bin(pk, 0) << " bytes");

    // Compute signature sizes
    sigBits = verifier->requiredLengthForSignedData();
    threshSig = new char[static_cast<size_t>(sigBits)];
    LOG_DEBUG(THRESHSIGN_LOG, "Signature size: " << sigBits << " bytes");

    // For BLS, sigshare size is just |sig| + ceil(\log_2{numSigners}), but we should probably not count that
    // since we can use the IP address in most application as the identifier.
    sigShareBits = dynamic_cast<BlsThresholdSigner*>(sks[1])->requiredLengthForSignedData();
    for (size_t idx = 0; idx < shares.size(); idx++) shares[idx] = new char[static_cast<size_t>(sigShareBits)];
  }

 public:
  virtual void pairing() {
    static GTT tmp;
    pc_map(tmp, h, pk);
  }

  virtual void hash() { g1_map(h, msg, msgSize); }

  virtual void signSingle() { g1_mul(sig, h, sk); }

  virtual void verifySingle() {
    GTT e1, e2;
    pc_map(e1, h, pk);
    pc_map(e2, sig, gen2);

    if (gt_cmp(e1, e2) != CMP_EQ) {
      throw std::logic_error("Your single signing or verification code is wrong.");
    }
  }

  virtual void signShare(ShareID i) {
    size_t idx = static_cast<size_t>(i);
    dynamic_cast<BlsThresholdSigner*>(sks[idx])->signData(
        reinterpret_cast<char*>(msg), msgSize, shares[idx], sigShareBits);
  }

  virtual void verifyShares() {
    assertNotNull(accum);
    accum->setExpectedDigest(msg, msgSize);
  }

  virtual void accumulateShares(const VectorOfShares& signers) {
    // Deletes the old accumulator pointer
    accum.reset(verifier->newAccumulator(hasShareVerify));

    // "Accumulate" the shares
    for (ShareID id = signers.first(); signers.isEnd(id) == false; id = signers.next(id)) {
      size_t idx = static_cast<size_t>(id);
      testAssertNotNull(shares[idx]);
      accum->add(shares[idx], sigShareBits);
    }
  }

  virtual void computeLagrangeCoeff(const VectorOfShares& signers) {
    (void)signers;
    if (useMultisig) return;

    dynamic_cast<BlsThresholdAccumulator*>(accum.get())->computeLagrangeCoeff();
  }

  virtual void exponentiateLagrangeCoeff(const VectorOfShares& signers) {
    (void)signers;
    if (useMultisig) return;

    dynamic_cast<BlsThresholdAccumulator*>(accum.get())->exponentiateLagrangeCoeff();
  }

  virtual void aggregateShares(const VectorOfShares& signers) {
    (void)signers;

    auto accPtr = dynamic_cast<BlsAccumulatorBase*>(accum.get());
    testAssertNotNull(accPtr);
    accPtr->aggregateShares();
  }

  void sanityCheckThresholdSignature(const VectorOfShares& signers) {
    auto accPtr = dynamic_cast<BlsAccumulatorBase*>(accum.get());
    testAssertNotNull(accPtr);
    accPtr->sigToBytes(reinterpret_cast<unsigned char*>(threshSig), sigBits);

    (void)signers;
    LOG_DEBUG(THRESHSIGN_LOG,
              "Threshold signature: " << Utils::bin2hex(reinterpret_cast<unsigned char*>(threshSig), sigBits));

    if (verifier->verify(reinterpret_cast<char*>(msg), msgSize, threshSig, sigBits) == false) {
      throw std::logic_error("Your threshold signing or verification code is wrong.");
    }
  }
};

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)args;
  lib.getPrecomputedInverses();

  pc_param_print();
  std::ofstream outLagr("threshold-lagrange-bls.csv");
  std::ofstream outMulti("threshold-multisig-bls.csv");

  BlsPublicParameters params(PublicParametersFactory::getWhatever());

  LOG_INFO(THRESHSIGN_LOG, "FP_PRIME: " << FP_PRIME);
  LOG_INFO(THRESHSIGN_LOG, "Security level: " << pc_param_level());

  std::vector<std::pair<int, int>> nk = Benchmark::getThresholdTestCases(501, false, true, false);

  //    for(size_t i = 1; i <= 100; i += 25) {
  //        nk.push_back(std::pair<int, int>(i+1, i));
  //    }

  for (bool useMultisig : {true, false}) {
    // for(bool useMultisig : {false, true}) {
    LOG_INFO(THRESHSIGN_LOG, "");
    LOG_INFO(THRESHSIGN_LOG,
             "Benchmarking " << (useMultisig ? "multisig-based" : "Lagrange-based") << " BLS threshold signatures");
    LOG_INFO(THRESHSIGN_LOG, "");

    int j = 0;
    for (auto it = nk.begin(); it != nk.end(); it++) {
      int n = it->first;
      int k = it->second;

      ThresholdBlsRelicBenchmark b(params, k, n, useMultisig);
      b.start();

      // printResults includes the header columns as well
      if (j++ == 0)
        b.printResults(useMultisig ? outMulti : outLagr);
      else
        b.printNumbers(useMultisig ? outMulti : outLagr);
    }
  }

  return 0;
}
