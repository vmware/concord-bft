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

#include <sstream>
#include "threshsign/IPublicParameters.h"

using namespace std;

const string IPublicParameters::className_ = "IPublicParameters";
const uint32_t IPublicParameters::classVersion_ = 1;

IPublicParameters::IPublicParameters(int securityLevel, string schemeName,
                                     string library) :
    securityLevel_(securityLevel), schemeName_(move(schemeName)),
    library_(move(library)) {}

/************** Serialization **************/

void IPublicParameters::serialize(ostream &outStream) const {
  // Serialize first the class name.
  serializeClassName(className_, outStream);
  serializeDataMembers(outStream);
}

void IPublicParameters::serializeDataMembers(ostream &outStream) const {
  // Serialize class version
  outStream.write((char *) &classVersion_, sizeof(classVersion_));

  // Serialize securityLevel_
  outStream.write((char *) &securityLevel_, sizeof(securityLevel_));

  // Serialize schemeName_
  auto sizeOfSchemeName = (int64_t) schemeName_.size();
  // Save a length of the string to the buffer to be able to deserialize it.
  outStream.write((char *) &sizeOfSchemeName, sizeof(sizeOfSchemeName));
  outStream.write(schemeName_.c_str(), sizeOfSchemeName);

  // Serialize library_
  auto sizeOfLibrary = (int64_t) library_.size();
  // Save a length of the string to the buffer to be able to deserialize it.
  outStream.write((char *) &sizeOfLibrary, sizeof(sizeOfLibrary));
  outStream.write(library_.c_str(), sizeOfLibrary);
}

bool IPublicParameters::operator==(const IPublicParameters &other) const {
  bool result = ((other.securityLevel_ == securityLevel_) &&
      (other.library_ == library_) && (other.schemeName_ == schemeName_));
  return result;
}

/************** Deserialization **************/

UniquePtrToClass IPublicParameters::create(istream &inStream) {
  verifyClassName(className_, inStream);

  // Deserialize class version
  verifyClassVersion(classVersion_, inStream);

  // Deserialize securityLevel_
  inStream.read((char *) &securityLevel_, sizeof(securityLevel_));

  // Deserialize schemeName_
  int64_t sizeOfSchemeName = 0;
  inStream.read((char *) &sizeOfSchemeName, sizeof(sizeOfSchemeName));
  UniquePtrToChar schemeName(new char[sizeOfSchemeName]);
  inStream.read(schemeName.get(), sizeOfSchemeName);

  // Deserialize library_
  int64_t sizeOfLibrary = 0;
  inStream.read((char *) &sizeOfLibrary, sizeof(sizeOfLibrary));
  UniquePtrToChar library(new char[sizeOfLibrary]);
  inStream.read(library.get(), sizeOfLibrary);

  return UniquePtrToClass(
      new IPublicParameters(securityLevel_, schemeName.get(), library.get()));
}
