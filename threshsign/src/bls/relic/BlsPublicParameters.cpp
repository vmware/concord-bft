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
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/Library.h"

#include "Utils.h"
#include "Log.h"

using namespace std;

namespace BLS {
namespace Relic {

const string BlsPublicParameters::className_ = "BlsPublicParameters";
const uint32_t BlsPublicParameters::classVersion_ = 1;

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

  logalloc << "Created: " << this << endl;
}

BlsPublicParameters::BlsPublicParameters(const BlsPublicParameters &params)
    : IPublicParameters(params.getSecurityLevel(), params.getName(),
                        params.getLibrary()), curveType_(params.curveType_) {
  g1_copy(generator1_, params.generator1_);
  g2_copy(generator2_, const_cast<G2T &>(params.generator2_));
}

BlsPublicParameters::~BlsPublicParameters() {
  logalloc << "Destroyed: " << this << endl;
}

int BlsPublicParameters::getSignatureSize() const {
  return Library::Get().getG1PointSize();
}

const BNT &BlsPublicParameters::getGroupOrder() const {
  return BLS::Relic::Library::Get().getG2Order();
}

/************** Serialization **************/

void BlsPublicParameters::serialize(ostream &outStream) const {
  // Serialize the base class
  IPublicParameters::serialize(outStream);

  // Serialize first the class name.
  serializeClassName(className_, outStream);
  serializeDataMembers(outStream);
}

void BlsPublicParameters::serializeDataMembers(ostream &outStream) const {
  // Serialize class version
  outStream.write((char *) &classVersion_, sizeof(classVersion_));

  // generator1_ and generator2_ fields should not be serialized as they are
  // generated on the fly.

  // Serialize curveType_
  outStream.write((char *) &curveType_, sizeof(curveType_));
}

bool BlsPublicParameters::operator==(const BlsPublicParameters &other) const {
  bool result = ((other.curveType_ == curveType_) &&
      (other.generator1_ == generator1_) &&
      (other.generator2_ == generator2_) &&
      (IPublicParameters::compare(other)));
  return result;
}

/************** Deserialization **************/

UniquePtrToClass BlsPublicParameters::create(istream &inStream) {
  // Retrieve the base class
  UniquePtrToClass baseClass(IPublicParameters::create(inStream));

  verifyClassName(className_, inStream);
  verifyClassVersion(classVersion_, inStream);
  inStream.read((char *) &curveType_, sizeof(curveType_));

  return UniquePtrToClass(new BlsPublicParameters(
      ((IPublicParameters *) baseClass.get())->getSecurityLevel(), curveType_));
}

} // end of RELIC namespace
} // end of BLS namespace
