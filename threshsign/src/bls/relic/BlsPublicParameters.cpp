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

#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/Library.h"

#include "Utils.h"
#include "Logger.hpp"

using namespace std;
using namespace concord::serialize;

namespace BLS {
namespace Relic {

BlsPublicParameters::BlsPublicParameters(int securityLevel, int curveType)
    : IPublicParameters(securityLevel, "", "RELIC"), curveType_(curveType) {
  schemeName_ = Library::getCurveName(curveType);

  library_ += " ";
  library_ += "(BN precision ";
  library_ += to_string(BN_PRECI);
  library_ += " bits)";

  BLS::Relic::Library::Get();
  g1_get_gen(generator1_);
  g2_get_gen(generator2_);

  LOG_TRACE(BLS_LOG, "Created: " << this);
}

BlsPublicParameters::BlsPublicParameters(const BlsPublicParameters &params)
    : IPublicParameters(params.getSecurityLevel(), params.getSchemeName(), params.getLibrary()),
      curveType_(params.curveType_) {
  g1_copy(generator1_, params.generator1_);
  g2_copy(generator2_, const_cast<G2T &>(params.generator2_));
}
BlsPublicParameters &BlsPublicParameters::operator=(const BlsPublicParameters &params) {
  securityLevel_ = params.securityLevel_;
  schemeName_ = params.schemeName_;
  library_ = params.library_;
  curveType_ = params.curveType_;
  BLS::Relic::Library::Get();
  g1_get_gen(generator1_);
  g2_get_gen(generator2_);
  return *this;
}

BlsPublicParameters::~BlsPublicParameters() { LOG_TRACE(BLS_LOG, "Destroyed: " << this); }

int BlsPublicParameters::getSignatureSize() const { return Library::Get().getG1PointSize(); }

const BNT &BlsPublicParameters::getGroupOrder() const { return BLS::Relic::Library::Get().getG2Order(); }

bool BlsPublicParameters::operator==(const BlsPublicParameters &other) const {
  bool result = ((other.curveType_ == curveType_) && (other.generator1_ == generator1_) &&
                 (other.generator2_ == generator2_) && (IPublicParameters::compare(other)));
  return result;
}

}  // namespace Relic
}  // namespace BLS
