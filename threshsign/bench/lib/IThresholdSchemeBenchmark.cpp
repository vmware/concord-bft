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

#include "IThresholdSchemeBenchmark.h"

#include "Logger.hpp"
#include "Utils.h"

using std::endl;

IThresholdSchemeBenchmark::IThresholdSchemeBenchmark(const IPublicParameters& p, int k, int n, int msgSize)
    : params(p),
      skBits(-1),
      pkBits(-1),
      sigBits(-1),
      sigShareBits(-1),
      numSigners(n),
      reqSigners(k),
      started(false),
      numBenchIters(10),
      msgSize(msgSize),
      hasPairing(true),
      hasShareVerify(true) {
  LOG_TRACE(THRESHSIGN_LOG, "msgSize = " << msgSize);
  assertStrictlyGreaterThan(msgSize, 0);

  msg = new unsigned char[static_cast<size_t>(msgSize)];
  // Pick 'some' message.
  for (int i = 0; i < msgSize; i++) {
    // Might overflow, we don't care.
    msg[i] = static_cast<unsigned char>((i + 1) * 2);
  }
}

IThresholdSchemeBenchmark::~IThresholdSchemeBenchmark() { delete[] msg; }

void IThresholdSchemeBenchmark::start() {
  started = true;
  LOG_DEBUG(THRESHSIGN_LOG, " (" << numBenchIters << " iterations per test)");

  for (int i = 0; i < numBenchIters; i++) {
    LOG_DEBUG(THRESHSIGN_LOG,
              "Benchmarking hashing m = " << Utils::bin2hex(msg, msgSize) << ", (" << msgSize << " bytes)");
    // Hash to the signature scheme's group
    hashT.startLap();
    hash();
    hashT.endLap();

    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking signing (no hashing)...");
    // Sign a message (normally, not threshold)
    sigT.startLap();
    signSingle();
    sigT.endLap();

    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking verification (no hashing)...");
    // Verify a message (normally, not threshold)
    verT.startLap();
    verifySingle();
    verT.endLap();

    // Group operations and pairing time
    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking group operations...");
    if (hasPairing) {
      pairT.startLap();
      pairing();
      pairT.endLap();
    }

    // Pick random subset of signers
    VectorOfShares signers;
    VectorOfShares::randomSubset(signers, numSigners, reqSigners);
    assertEqual(signers.count(), reqSigners);

    // Signer i will "sign-share" a message
    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking share signing (" << reqSigners << " out of " << numSigners << ")...");
    for (ShareID id = signers.first(); signers.isEnd(id) == false; id = signers.next(id)) {
      sshareT.startLap();
      signShare(id);
      sshareT.endLap();
    }

    // This begins the accumulation of the shares (creates an accumulator for example and adds the shares to it)
    accumulateShares(signers);

    if (hasShareVerify) {
      LOG_DEBUG(THRESHSIGN_LOG,
                "Benchmarking share verification (" << reqSigners << " out of " << numSigners << ")...");
      // Verify the "sig-shares" of all signers
      vshareT.startLap();
      verifyShares();
      vshareT.endLap();

    } else {
      LOG_DEBUG(THRESHSIGN_LOG, "(Skipping over share verification: hasShareVerify is set to false)");
    }

    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking Lagrange coefficient computation...");
    lagrangeCoeffT.startLap();
    // Compute the lagrange coefficients L_i(0) for each signer i in signers set.
    computeLagrangeCoeff(signers);
    lagrangeCoeffT.endLap();

    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking Lagrange coefficient exponentiation...");
    // Then, exponentiate the sig-share of each signer i by L_i(0)
    // (assuming multiplicative group notation)
    lagrangeExpT.startLap();
    exponentiateLagrangeCoeff(signers);
    lagrangeExpT.endLap();

    LOG_DEBUG(THRESHSIGN_LOG, "Benchmarking signature share aggregation...");
    // Finally, aggregate the signature shares using the lagrange coefficients
    aggT.startLap();
    aggregateShares(signers);
    aggT.endLap();

    sanityCheckThresholdSignature(signers);
  }
}

void IThresholdSchemeBenchmark::printResults(std::ostream& out) {
  if (!started) {
    throw std::logic_error("Must start benchmark before writing results!");
  }

  printHeaders(out);
  printNumbers(out);
}

void IThresholdSchemeBenchmark::printHeaders(std::ostream& out) {
  out << "library,"
      << "cryptosys,"
      << "secparam,"
      << "sk_size,"
      << "pk_size,"
      << "hash_to_group_time,"
      << "pairing_time,"  // might be N/A
      << "sig_size,"
      << "sig_time,"                   // (excludes hashing time)
      << "verify_time,"                // (excludes hashing time)
      << "num_signers,"                // number of total signers (n)
      << "thres_signers,"              // required threshold number of signers (t)
      << "sigshare_size,"              // might be identical to sig_size; for RSA, this includes proof \pi
      << "sigshare_time,"              // might be identical to sig_time
      << "lagrange_coeff_time,"        // (time to compute l_i(0) for all i)
      << "lagrange_exp_time,"          // (time to exponentiate the sig shares with their l_i(0))
      << "aggregate_time,"             // (given all coeffs and sigshares, time to compute final sig)
      << "total_share+lagr+agg_time,"  // hash_to_group_time + sigshare_time + lagrange_coeff_time + lagrange_exp_time +
                                       // aggregate_time + final_aggr_step
      << "verifyshares_time,";         // time to verify all shares
  printExtraHeaders(out);
  out << std::endl;
}

void IThresholdSchemeBenchmark::printNumbers(std::ostream& out) {
  out << params.getLibrary() << ",";        // library
  out << params.getSchemeName() << ",";     // cryptosystem
  out << params.getSecurityLevel() << ",";  // secparam
  out << skBits << ",";                     // sk_size
  out << pkBits << ",";                     // pk_size
  auto hashTime = hashT.averageLapTime();
  out << hashTime << ",";  // hash_to_group_time
  if (hasPairing) {
    out << pairT.averageLapTime() << ",";  // pairing_time
  } else {
    out << "N/A,";
    out << "N/A,";
    out << "N/A,";
  }
  out << sigBits << ",";                // sig_size
  out << sigT.averageLapTime() << ",";  // sig_time
  out << verT.averageLapTime() << ",";  // verify_time
  out << numSigners << ",";             // total number of signers n
  out << reqSigners << ",";             // threshold t
  out << sigShareBits << ",";           // sigshare_size

  auto sigShareTime = sshareT.averageLapTime();
  out << sigShareTime << ",";  // sigshare_time
  auto lagrCoeffTime = lagrangeCoeffT.averageLapTime();
  out << lagrCoeffTime << ",";  // lagrange_coeff_time
  auto lagrExpTime = lagrangeExpT.averageLapTime();
  out << lagrExpTime << ",";  // lagrange_exp_time
  auto aggTime = aggT.averageLapTime();
  out << aggTime << ",";  // aggregate_time

  auto totalShareAggVerTime = sigShareTime + lagrCoeffTime + lagrExpTime + aggTime;

  out << totalShareAggVerTime << ",";

  if (hasShareVerify) {
    // NOTE: Each for loop iteration in start() measures the time to verify all shares.
    out << vshareT.averageLapTime() << ",";  // verifyshare_time
  } else {
    out << "N/A,";
  }
  printExtraNumbers(out);
  out << std::endl;
}
