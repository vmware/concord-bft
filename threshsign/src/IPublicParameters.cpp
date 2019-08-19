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
#include <iostream>
#include "threshsign/IPublicParameters.h"

using namespace std;
using namespace serialize;

IPublicParameters::IPublicParameters(int securityLevel, string schemeName, string library) :
    securityLevel_(securityLevel), schemeName_(move(schemeName)),
    library_(move(library)) {}

/************** Serialization **************/

void IPublicParameters::serializeDataMembers(ostream &outStream) const {
  serializeInt(securityLevel_, outStream);
  LOG_TRACE(log_srlz_, "<<< securityLevel_: " << securityLevel_);
  serializeString(schemeName_, outStream);
  LOG_TRACE(log_srlz_, "<<< schemeName_: " << schemeName_);
  serializeString(library_, outStream);
  LOG_TRACE(log_srlz_, "<<< library_: " <<library_);
}

bool IPublicParameters::operator==(const IPublicParameters &other) const {
  bool result = ((other.securityLevel_ == securityLevel_) &&
                 (other.library_ == library_) &&
                 (other.schemeName_ == schemeName_));

  if (other.securityLevel_ != securityLevel_)
    std::cout << "securityLevel_" << std::endl;
  if (other.library_ != library_)
    std::cout << "library_" << std::endl;
  if (other.schemeName_ != schemeName_)
    std::cout << "schemeName_" << std::endl;
  return result;
}

/************** Deserialization **************/

void IPublicParameters::deserializeDataMembers(std::istream& inStream){
  securityLevel_ = deserializeInt<int>(inStream);
  LOG_TRACE(log_srlz_, ">>> securityLevel_: " << securityLevel_);
  schemeName_ = deserializeString(inStream);
  LOG_TRACE(log_srlz_, ">>> schemeName_: " << schemeName_);
  library_    = deserializeString(inStream);
   LOG_TRACE(log_srlz_, "<<< library_: " <<library_);
}

SerializablePtr IPublicParameters::create(istream &inStream) {
  verifyClassVersion(classVersion_, inStream);
  return SerializablePtr(new IPublicParameters);
}
