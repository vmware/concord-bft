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

#include <string>
#include <ostream>
#include <set>
#include <stdexcept>

#include "Timer.h"

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/VectorOfShares.h"
#include "threshsign/IPublicParameters.h"

class IThresholdSchemeBenchmark {
 protected:
  const IPublicParameters& params;
  int skBits, pkBits;
  int sigBits;
  int sigShareBits;
  NumSharesType numSigners;  // total number of signers
  NumSharesType reqSigners;  // required number of threshold signers

  bool started;
  int numBenchIters;

  int msgSize;
  unsigned char* msg;  // Message that will be signed

  AveragingTimer hashT,  // hashing the message to be signed
      sigT,              // a single signing op under the non-threshold scheme (e.g., normal BLS) w/o hashing overhead
      verT,     // a single verification op under the non-threshold scheme (e.g., normal BLS) w/o hashing overhead
      sshareT,  // a single share signing op under the threshold scheme
      vshareT,  // verifying a signature share
      pairT,    // computing a pairing, if applicable (e.g., in BLS)
      lagrangeCoeffT,  // computing the Lagrange coefficients
      lagrangeExpT,    // exponentiating the sig shares with Lagrange coeffs
      aggT;            // aggregating the signature shares, once the coefficients are computed

  bool hasPairing;
  bool hasShareVerify;

 public:
  IThresholdSchemeBenchmark(const IPublicParameters& p, int k, int n, int msgSize = 64);
  virtual ~IThresholdSchemeBenchmark();

 public:
  void start();

  // Compute a pairing (bilinear map)
  virtual void pairing() = 0;

  virtual void hash() = 0;

  virtual void signSingle() = 0;
  virtual void verifySingle() = 0;

  /**
   * WARNING: Players are indexed from 1 to N, inclusively. This is because
   * each player gets their share i as the evaluation p(i) of the polynomial p(.)
   * at point i. Since the shared secret key is stored in p(0) no player can have identity 0.
   * (or if they do, then identities need to be mapped to points x_i such that player
   *  i's share becomes p(x_i) rather than p(i))
   */
  virtual void signShare(ShareID i) = 0;
  virtual void verifyShares() = 0;
  virtual void accumulateShares(const VectorOfShares& signers) = 0;
  virtual void computeLagrangeCoeff(const VectorOfShares& signers) = 0;
  virtual void exponentiateLagrangeCoeff(const VectorOfShares& signers) = 0;
  virtual void aggregateShares(const VectorOfShares& signers) = 0;
  virtual void sanityCheckThresholdSignature(const VectorOfShares& signers) = 0;

  void printResults(std::ostream& out);
  void printHeaders(std::ostream& out);
  virtual void printExtraHeaders(std::ostream& out) { (void)out; }
  void printNumbers(std::ostream& out);
  virtual void printExtraNumbers(std::ostream& out) { (void)out; }
};
