// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <memory>
#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "ReplicaConfig.hpp"

namespace bftEngine {

class CryptoManager {
 public:
  static CryptoManager& instance(const ReplicaConfig* config = nullptr, Cryptosystem* cryptoSys = nullptr) {
    static CryptoManager cm_(config, cryptoSys);
    return cm_;
  }

  IThresholdSigner* thresholdSignerForSlowPathCommit() const { return thresholdSigner_.get(); }
  IThresholdVerifier* thresholdVerifierForSlowPathCommit() const { return thresholdVerifierForSlowPathCommit_.get(); }

  IThresholdSigner* thresholdSignerForCommit() const { return thresholdSigner_.get(); }
  IThresholdVerifier* thresholdVerifierForCommit() const { return thresholdVerifierForCommit_.get(); }

  IThresholdSigner* thresholdSignerForOptimisticCommit() const { return thresholdSigner_.get(); }
  IThresholdVerifier* thresholdVerifierForOptimisticCommit() const {
    return thresholdVerifierForOptimisticCommit_.get();
  }

 private:
  CryptoManager(const ReplicaConfig* config, Cryptosystem* cryptoSys) {
    multiSigCryptoSystem_.reset(cryptoSys);
    thresholdSigner_.reset(multiSigCryptoSystem_->createThresholdSigner());
    thresholdVerifierForSlowPathCommit_.reset(
        multiSigCryptoSystem_->createThresholdVerifier(config->fVal * 2 + config->cVal + 1));
    thresholdVerifierForCommit_.reset(
        multiSigCryptoSystem_->createThresholdVerifier(config->fVal * 3 + config->cVal + 1));
    thresholdVerifierForOptimisticCommit_.reset(multiSigCryptoSystem_->createThresholdVerifier(config->numReplicas));

    // TODO [TK] logic for loading keys from reserved pages
    //    // We want to generate public key for n-out-of-n case
    //    multiSigCryptoSystem_.reset(new Cryptosystem(MULTISIG_BLS_SCHEME,
    //                                                 "BN-P254",
    //                                                 config->numReplicas,
    //                                                 config->numReplicas));
  }

  CryptoManager(const CryptoManager&) = delete;
  CryptoManager(const CryptoManager&&) = delete;
  CryptoManager& operator=(const CryptoManager&) = delete;
  CryptoManager& operator=(const CryptoManager&&) = delete;

  std::unique_ptr<Cryptosystem> multiSigCryptoSystem_;

  std::unique_ptr<IThresholdSigner> thresholdSigner_;
  // signer and verifier of a threshold signature (for threshold N-fVal-cVal out of N)
  std::unique_ptr<IThresholdVerifier> thresholdVerifierForSlowPathCommit_;

  // verifier of a threshold signature (for threshold N-cVal out of N)
  // If cVal==0, then should be nullptr
  std::unique_ptr<IThresholdVerifier> thresholdVerifierForCommit_;

  // verifier of a threshold signature (for threshold N out of N)
  std::unique_ptr<IThresholdVerifier> thresholdVerifierForOptimisticCommit_;
};
}  // namespace bftEngine
