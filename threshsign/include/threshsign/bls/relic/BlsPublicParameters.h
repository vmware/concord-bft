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

#include "threshsign/IPublicParameters.h"
#include "BlsNumTypes.h"

namespace BLS {
namespace Relic {

class BlsPublicParameters : public IPublicParameters {
 protected:
  G1T generator1_;
  G2T generator2_;
  int curveType_ = 0;

 public:
  BlsPublicParameters(int securityLevel, int curveType);
  BlsPublicParameters(const BlsPublicParameters &params);
  ~BlsPublicParameters() override;

  bool operator==(const BlsPublicParameters &other) const;

  /**
   * Needed by IThresholdSigner/Verifier.
   */
  int getSignatureSize() const;
  int getCurveType() const { return curveType_; }
  const G2T &getGenerator2() const { return generator2_; }
  const BNT &getGroupOrder() const;

  // Serialization/deserialization
  void serialize(std::ostream &outStream) const override;

  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization. Could not become private/protected,
  // as there is a composition relationship between IPublicParameters and
  // signer/verifier classes.
  BlsPublicParameters() = default; // To be used during deserialization.

 private:
  void serializeDataMembers(std::ostream &outStream) const;

 private:
  static const std::string className_;
  static const uint32_t classVersion_;
};

} // end of Libpbc namespace
} // end of RELIC namespace
