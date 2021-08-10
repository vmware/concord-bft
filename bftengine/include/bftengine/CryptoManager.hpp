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
#include "IKeyExchanger.hpp"
#include "Logger.hpp"

namespace bftEngine {
typedef std::int64_t SeqNum;                    // TODO [TK] redefinition
constexpr uint16_t checkpointWindowSize = 150;  // TODO [TK] redefinition

class CryptoManager : public IKeyExchanger, public IMultiSigKeyGenerator {
 public:
  /**
   * Singleton access method
   * For the first time should be called with a non-null argument
   */
  static CryptoManager& instance(std::unique_ptr<Cryptosystem>&& cryptoSys = nullptr) {
    static CryptoManager cm_(std::move(cryptoSys));
    return cm_;
  }
  std::shared_ptr<IThresholdSigner> thresholdSignerForSlowPathCommit(const SeqNum sn) const {
    return get(sn)->thresholdSigner_;
  }
  std::shared_ptr<IThresholdVerifier> thresholdVerifierForSlowPathCommit(const SeqNum sn) const {
    return get(sn)->thresholdVerifierForSlowPathCommit_;
  }
  std::shared_ptr<IThresholdSigner> thresholdSignerForCommit(const SeqNum sn) const {
    return get(sn)->thresholdSigner_;
  }
  std::shared_ptr<IThresholdVerifier> thresholdVerifierForCommit(const SeqNum sn) const {
    return get(sn)->thresholdVerifierForCommit_;
  }
  std::shared_ptr<IThresholdSigner> thresholdSignerForOptimisticCommit(const SeqNum sn) const {
    return get(sn)->thresholdSigner_;
  }
  std::shared_ptr<IThresholdVerifier> thresholdVerifierForOptimisticCommit(const SeqNum sn) const {
    return get(sn)->thresholdVerifierForOptimisticCommit_;
  }
  // IMultiSigKeyGenerator methods
  std::pair<std::string, std::string> generateMultisigKeyPair() override {
    LOG_INFO(logger(), "Generating new multisig key pair");
    return cryptoSystems_.rbegin()->second->cryptosys_->generateNewKeyPair();
  }
  // IKeyExchanger methods
  // onPrivateKeyExchange and onPublicKeyExchange callbacks for a given checkpoint may be called in a different order.
  // Therefore the first called will create a CryptoSys
  void onPrivateKeyExchange(const std::string& secretKey,
                            const std::string& verificationKey,
                            const SeqNum& sn) override {
    LOG_INFO(logger(), KVLOG(sn, verificationKey));
    auto sys_wrapper = create(sn);
    sys_wrapper->cryptosys_->updateKeys(secretKey, verificationKey);
    sys_wrapper->init();
  }
  void onPublicKeyExchange(const std::string& verificationKey,
                           const std::uint16_t& signerIndex,
                           const SeqNum& sn) override {
    LOG_INFO(logger(), KVLOG(sn, signerIndex, verificationKey));
    auto sys = create(sn);
    // the +1 is due to Crypto system starts counting from 1
    sys->cryptosys_->updateVerificationKey(verificationKey, signerIndex + 1);
    sys->init();
  }

  void onCheckpoint(const uint64_t& checkpoint) {
    LOG_INFO(logger(), "chckp: " << checkpoint);
    // clearOldKeys();
  }

 private:
  /**
   *  Holds Cryptosystem, signers and verifiers per checkpoint
   */
  struct CryptoSystemWrapper {
    CryptoSystemWrapper(std::unique_ptr<Cryptosystem>&& cs) : cryptosys_(std::move(cs)) {}
    CryptoSystemWrapper(const CryptoSystemWrapper&) = delete;
    std::unique_ptr<Cryptosystem> cryptosys_;
    std::shared_ptr<IThresholdSigner> thresholdSigner_;
    // signer and verifier of a threshold signature (for threshold N-fVal-cVal out of N)
    std::shared_ptr<IThresholdVerifier> thresholdVerifierForSlowPathCommit_;

    // verifier of a threshold signature (for threshold N-cVal out of N)
    // If cVal==0, then should be nullptr
    std::shared_ptr<IThresholdVerifier> thresholdVerifierForCommit_;

    // verifier of a threshold signature (for threshold N out of N)
    std::shared_ptr<IThresholdVerifier> thresholdVerifierForOptimisticCommit_;

    void init() {
      std::uint16_t f{ReplicaConfig::instance().getfVal()};
      std::uint16_t c{ReplicaConfig::instance().getcVal()};
      std::uint16_t numSigners{ReplicaConfig::instance().getnumReplicas()};
      thresholdSigner_.reset(cryptosys_->createThresholdSigner());
      thresholdVerifierForSlowPathCommit_.reset(cryptosys_->createThresholdVerifier(f * 2 + c + 1));
      thresholdVerifierForCommit_.reset(cryptosys_->createThresholdVerifier(f * 3 + c + 1));
      thresholdVerifierForOptimisticCommit_.reset(cryptosys_->createThresholdVerifier(numSigners));
    }
  };

  // accessing existing Cryptosystems
  std::shared_ptr<CryptoSystemWrapper> get(const SeqNum& sn) const {
    // find last chckp that is less than a chckp of a given sn
    uint64_t chckp = (sn - 1) / checkpointWindowSize;
    auto it = cryptoSystems_.rbegin();
    while (it != cryptoSystems_.rend()) {
      if (it->first <= chckp) {
        // LOG_TRACE(logger(), KVLOG(sn, chckp, it->first, it->second));
        return it->second;
      }
      it++;
    }
    LOG_FATAL(logger(), "Cryptosystem not found for checkpoint: " << chckp << "seqnum: " << sn);
    ConcordAssert(false && "should never reach here");
  }
  // create CryptoSys for sn if still doesn't exist
  std::shared_ptr<CryptoSystemWrapper> create(const SeqNum& sn) {
    // Cryptosystem for this sn will be activated upon reaching a second checkpoint from now
    uint64_t chckp = sn / checkpointWindowSize + 2;
    if (auto it = cryptoSystems_.find(chckp); it != cryptoSystems_.end()) return it->second;
    // copy construct new Cryptosystem from a last one as we want it to include all the existing keys
    std::unique_ptr<Cryptosystem> cs =
        std::make_unique<Cryptosystem>(*cryptoSystems_.rbegin()->second->cryptosys_.get());
    LOG_INFO(logger(), "created new Cryptosytem for checkpoint: " << chckp);
    return cryptoSystems_.insert(std::make_pair(chckp, std::make_shared<CryptoSystemWrapper>(std::move(cs))))
        .first->second;
  }

  // store at most 2 cryptosystems
  void clearOldKeys() {
    while (cryptoSystems_.size() > 2) {
      LOG_INFO(logger(), "delete Cryptosytem for checkpoint: " << cryptoSystems_.begin()->first);
      cryptoSystems_.erase(cryptoSystems_.begin());
    }
  }

  CryptoManager(std::unique_ptr<Cryptosystem>&& cryptoSys) {
    // default cryptosystem is always at chckp 0
    cryptoSystems_.insert(std::make_pair(0, std::make_shared<CryptoSystemWrapper>(std::move(cryptoSys))));
    cryptoSystems_.begin()->second->init();
  }
  logging::Logger& logger() const {
    static logging::Logger logger_ = logging::getLogger("concord.bft.crypto-mgr");
    return logger_;
  }
  CryptoManager(const CryptoManager&) = delete;
  CryptoManager(const CryptoManager&&) = delete;
  CryptoManager& operator=(const CryptoManager&) = delete;
  CryptoManager& operator=(const CryptoManager&&) = delete;

  // chckp -> CryptoSys
  std::map<std::uint64_t, std::shared_ptr<CryptoSystemWrapper>> cryptoSystems_;
};
}  // namespace bftEngine
