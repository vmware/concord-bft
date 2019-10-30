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

#include <stdexcept>
#include <memory>
#include <sstream>

#include "BlsPublicParameters.h"
#include "Library.h"

namespace BLS {
namespace Relic {

class PublicParametersFactory {
 public:
  static BlsPublicParameters getByCurveType(const char* curveName) {
    int curveType = Library::getCurveByName(curveName);
    BlsPublicParameters params = getWhatever();

    if (params.getCurveType() != curveType) {
      throw std::runtime_error("RELIC library is not built for the specified curve");
    }

    return params;
  }

  static BlsPublicParameters getWhatever() {
    return BlsPublicParameters(Library::Get().getSecurityLevel(), Library::Get().getCurrentCurve());
  }

  static BlsPublicParameters getBitSecurity128() {
#ifdef __APPLE__
    // TODO: Can't compile 128-bit curves (or higher) on OS X yet
    return getBitSecurityK(112);
#else
    return getBitSecurityK(128);
#endif
  }

  static BlsPublicParameters getBitSecurityK(int k) {
    // NOTE: RELIC library is compiled with support for one curve at a certain security level, so here we check for that
    // level.
    if (Library::Get().getSecurityLevel() != k) {
      std::ostringstream str;
      str << "The RELIC library you are linked against has a different security level than " << k << " bits: it has "
          << Library::Get().getSecurityLevel() << "-bit security.";
      throw std::runtime_error(str.str());
    }

    return BlsPublicParameters(128, Library::Get().getCurrentCurve());
  }
};

}  // namespace Relic
}  // namespace BLS
