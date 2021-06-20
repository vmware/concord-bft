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

#include "threshsign/bls/relic/Library.h"

#include "kvstream.h"
#include "Logger.hpp"
#include "Utils.h"

namespace BLS {
namespace Relic {

void PrecomputedInverses::precompute() {
  if (precomputed) return;

  // -c (mod p) = p - 1 - c (mod p)
  // (-a)^{-1} (mod p) = (-1)^{-1} * (a)^{-1} (mod p)
  BNT invOne = (fieldOrder - BNT(2)).invertModPrime(fieldOrder).SlowModulo(fieldOrder);

  LOG_DEBUG(BLS_LOG, "Precomputing i^-1 mod p, for all signers i < " << invs.size());
  for (size_t i = 1; i <= invs.size() - 1; i++) {
    dig_t n = i;

    invs[i] = BNT::invertModPrime(n, fieldOrder);
    // NOTE: Lazy way of testing bn_gcd_ext_dig-based implementation. Commented out to make startup faster.
    // assertEqual(BNT(n).invertModPrime(fieldOrder), invs[i]);
    negInvs[i] = invOne * invs[i];
    assertGreaterThanOrEqual(invs[i], BNT::Zero());
  }

  precomputed = true;
}

LibraryInitializer::LibraryInitializer() {
  if (core_init() != STS_OK) {
    core_clean();
    throw std::runtime_error("Could not initialize RELIC elliptic curve library");
  }

  // conf_print();

  if (pc_param_set_any() != STS_OK) {
    LOG_ERROR(BLS_LOG, "Couldn't set up RELIC elliptic curve");
    throw std::runtime_error("Could not set up RELIC elliptic curve library");
  }
}

LibraryInitializer::~LibraryInitializer() { core_clean(); }

Library::Library() {
  g1_rand(dummyG1);
  numBytesG1 = g1_size_bin(dummyG1, 1);

  g2_rand(dummyG2);
  numBytesG2 = g2_size_bin(dummyG2, 1);

  g2_get_ord(g2size);
  // e.g., if g2size is 4 elements, then we need g^{00} and g^{01}, g^{10},
  // g^{11} to represent them actually.
  g2bits = (g2size - BNT::One()).getBits();

  // Map EC integer IDs to strings and viceversa
  curveIdToName[BN_P254] = std::string("BN-P254");
  curveIdToName[BN_P256] = std::string("BN-P256");
  curveIdToName[B12_P381] = std::string("B12-P381");
  curveIdToName[BN_P382] = std::string("BN-P382");
  curveIdToName[B12_P455] = std::string("B12-P455");
  curveIdToName[B24_P477] = std::string("B24-P477");
  curveIdToName[KSS_P508] = std::string("KSS-P508");
  curveIdToName[BN_P638] = std::string("BN-P638");
  curveIdToName[B12_P638] = std::string("B12-P638");

  for (auto it = curveIdToName.begin(); it != curveIdToName.end(); it++) {
    curveNameToId[it->second] = it->first;
  }

  pi.setFieldOrder(g2size);

  LOG_DEBUG(BLS_LOG, "Successfully initialized RELIC library");
}

Library::~Library() {}

int Library::getCurveByName(const char* curveName) {
  // NOTE: Will throw std::out_of_range if curve is not in
  try {
    return Get().curveNameToId.at(std::string(curveName));
  } catch (const std::out_of_range& e) {
    LOG_FATAL(BLS_LOG, "curveNameToId.at() has failed for" << KVLOG(curveName));
    throw;
  }
}

std::string Library::getCurveName(int curveType) {
  // NOTE: Will throw std::out_of_range if curve is not in
  try {
    return Get().curveIdToName.at(curveType);
  } catch (const std::out_of_range& e) {
    LOG_FATAL(BLS_LOG, "curveIdToName.at() has failed for" << KVLOG(curveType));
    throw;
  }
}

} /* namespace Relic */
} /* namespace BLS */
