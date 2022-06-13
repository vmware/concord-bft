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

#include "threshsign/IPublicKey.h"

#include "BlsNumTypes.h"

namespace BLS {
namespace Relic {

class BlsPublicKey : public IShareVerificationKey {
 protected:
  G2T y;

 public:
  friend class BlsThresholdVerifier;
  friend class BlsMultisigVerifier;

 public:
  /**
   * Creates a dummy PK.
   */
  BlsPublicKey() {}

  /**
   * Creates a PK from an SK and forgets the SK!
   */
  BlsPublicKey(const BNT &sk) {
    // FIXME: RELIC won't take const sk
    g2_mul_gen(y, const_cast<BNT &>(sk));
  }

  BlsPublicKey(const G2T &pk) : y(pk) {}

 public:
  std::string toString() const override { return y.toString(); }
  bool operator==(const BlsPublicKey &other) const { return (other.y == y); }

 public:
  const G2T &getPoint() const { return y; }
};

} /* namespace Relic */
} /* namespace BLS */
