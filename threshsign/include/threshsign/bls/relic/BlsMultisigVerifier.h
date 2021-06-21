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

#include <memory>

#include "threshsign/IThresholdAccumulator.h"

#include "BlsNumTypes.h"
#include "BlsThresholdVerifier.h"

namespace BLS {
namespace Relic {

class BlsPublicParameters;

class BlsMultisigVerifier : public BlsThresholdVerifier {
 public:
  BlsMultisigVerifier(const BlsPublicParameters &params,
                      NumSharesType reqSigners,
                      NumSharesType numSigners,
                      const std::vector<BlsPublicKey> &verificationKeys);

  explicit BlsMultisigVerifier(const BlsThresholdVerifier &base);

  ~BlsMultisigVerifier() override = default;

  bool operator==(const BlsMultisigVerifier &other) const;

  IThresholdAccumulator *newAccumulator(bool withShareVerification) const override;

  const IPublicKey &getPublicKey() const override {
    // TODO(Alin): Should return a BlsMultisigPK object which has all signers' VKs
    if (reqSigners_ != numSigners_) {
      throw std::runtime_error("k-out-of-n multisigs do not have a fixed PK");
    }

    return BlsThresholdVerifier::getPublicKey();
  }

  bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const override;

  int requiredLengthForSignedData() const override;
};

} /* namespace Relic */
} /* namespace BLS */
