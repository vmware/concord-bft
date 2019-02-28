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

#include "threshsign/bls/relic/BlsPublicParameters.h"

#include "threshsign/bls/relic/Library.h"

#include "Utils.h"
#include "Log.h"

using namespace std;

namespace BLS {
namespace Relic {

BlsPublicParameters::BlsPublicParameters(int securityLevel, const int curveType)
    : IPublicParameters(securityLevel, "", "RELIC"), curveType(curveType) {
  name = Library::getCurveName(curveType);

  library += " ";
  library += "(BN precision ";
  library += std::to_string(BN_PRECI);
  library += " bits)";

  BLS::Relic::Library::Get();
  g1_get_gen(gen1);
  g2_get_gen(gen2);

  logalloc << "Created: " << this << endl;
}

BlsPublicParameters::BlsPublicParameters(const BlsPublicParameters& params)
    : IPublicParameters(
          params.getSecurityLevel(), params.getName(), params.getLibrary()),
      curveType(params.curveType) {
  g1_copy(gen1, params.gen1);
  g2_copy(gen2, const_cast<G2T&>(params.gen2));
}

BlsPublicParameters::~BlsPublicParameters() {
  logalloc << "Destroyed: " << this << endl;
}

int BlsPublicParameters::getSignatureSize() const {
  return Library::Get().getG1PointSize();
}

const BNT& BlsPublicParameters::getGroupOrder() const {
  return BLS::Relic::Library::Get().getG2Order();
}

}  // namespace Relic
}  // namespace BLS
