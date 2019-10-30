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

#include "threshsign/ISecretKey.h"

#include "BlsNumTypes.h"

namespace BLS {
namespace Relic {

class BlsSecretKey : public IShareSecretKey {
 protected:
  BNT x;

 public:
  friend class BlsThresholdSigner;

 public:
  explicit BlsSecretKey(const BNT& secretKey) : x(secretKey) {}
  // To be used ONLY during deserialization. Could not become private/protected,
  // as there is a composition relationship between BlsSecretKey and
  // BlsThresholdSigner class.
  BlsSecretKey() = default;

 public:
  std::string toString() const override { return x.toString(); }
  bool operator==(const BlsSecretKey& other) const { return (other.x == x); }
};

} /* namespace Relic */
} /* namespace BLS */
