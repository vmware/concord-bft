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

#include "ThresholdSignaturesTypes.h"

#include <vector>
#include <memory>   // std::unique_ptr
#include <utility>  // std::pair

class IThresholdVerifier;
class IThresholdSigner;
class IShareSecretKey;
class IShareVerificationKey;

class IThresholdFactory {
 public:
  using SignersVerifierTuple =
      std::tuple<std::vector<std::unique_ptr<IThresholdSigner>>, std::unique_ptr<IThresholdVerifier>>;
  virtual ~IThresholdFactory() {}

 public:
  /**
   * Creates an IThresholdVerifier for the specified (k, n) threshold scheme out of the
   * serialized threshold PK and share verification keys.
   *
   * @param   verifKeyStr array of share verification strings, indexed 1 as always!
   */
  virtual IThresholdVerifier* newVerifier(ShareID reqSigners,
                                          ShareID totalSigners,
                                          const char* publicKeyStr,
                                          const std::vector<std::string>& verifKeysStr) const = 0;

  virtual IThresholdSigner* newSigner(ShareID id, const char* secretKeyStr) const = 0;

  /**
   * Generates numSigners IThresholdSigner objects with random SKs and a corresponding IThresholdVerifier.
   * Useful for writing tests!
   */
  virtual SignersVerifierTuple newRandomSigners(NumSharesType reqSigners, NumSharesType numSigners) const = 0;

  /**
   * Generates a single key pair
   */
  virtual std::pair<std::unique_ptr<IShareSecretKey>, std::unique_ptr<IShareVerificationKey>> newKeyPair() const = 0;
};
