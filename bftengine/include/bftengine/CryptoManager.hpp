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

#include <mutex>
#include <memory>

#include "log/logger.hpp"
#include "crypto/threshsign/ThresholdSignaturesTypes.h"
#include "crypto/threshsign/IThresholdSigner.h"
#include "crypto/threshsign/IThresholdVerifier.h"
#include "ReplicaConfig.hpp"
#include "IKeyExchanger.hpp"
#include "crypto/crypto.hpp"

namespace bftEngine {
typedef std::int64_t SeqNum;                    // TODO [TK] redefinition
constexpr uint16_t checkpointWindowSize = 150;  // TODO [TK] redefinition

class CryptoManager : public IKeyExchanger, public IMultiSigKeyGenerator {
 public:
  /**
   * Singleton access method
   * For the first time should be called with a non-null argument
   */

  static std::shared_ptr<CryptoManager> s_cm;

  static std::shared_ptr<CryptoManager> init(std::unique_ptr<Cryptosystem>&& cryptoSys);
  static CryptoManager& instance();
  static void reset(std::shared_ptr<CryptoManager> other);

  std::shared_ptr<IThresholdSigner> thresholdSignerForSlowPathCommit(const SeqNum sn) const;
  std::shared_ptr<IThresholdVerifier> thresholdVerifierForSlowPathCommit(const SeqNum sn) const;
  std::shared_ptr<IThresholdSigner> thresholdSignerForCommit(const SeqNum sn) const;
  std::shared_ptr<IThresholdVerifier> thresholdVerifierForCommit(const SeqNum sn) const;
  std::shared_ptr<IThresholdSigner> thresholdSignerForOptimisticCommit(const SeqNum sn) const;
  std::shared_ptr<IThresholdVerifier> thresholdVerifierForOptimisticCommit(const SeqNum sn) const;
  std::unique_ptr<Cryptosystem>& getLatestCryptoSystem() const;

  /**
   * @return An algorithm identifier for the latest threshold signature scheme
   */
  concord::crypto::SignatureAlgorithm getLatestSignatureAlgorithm() const;

  // IMultiSigKeyGenerator methods
  std::tuple<std::string, std::string, concord::crypto::SignatureAlgorithm> generateMultisigKeyPair() override;

  // IKeyExchanger methods
  // onPrivateKeyExchange and onPublicKeyExchange callbacks for a given checkpoint may be called in a different order.
  // Therefore the first called will create ca CryptoSys
  void onPrivateKeyExchange(const std::string& secretKey,
                            const std::string& verificationKey,
                            const SeqNum& sn) override;

  void onPublicKeyExchange(const std::string& verificationKey,
                           const std::uint16_t& signerIndex,
                           const SeqNum& sn) override;

  void onCheckpoint(uint64_t newCheckpoint);

  // Important note:
  // CryptoManager's cryptosystems are currently implemented using a naive eddsa multisig scheme
  // The following methods break the abstraction of the threshsign library in order
  // to extract ISigner and IVerifier object.
  // This abstraction is broken to allow using the consensus key as the replica's main key (In SigManager), thus
  // enabling an operator to change it (key rotation).
  // This code will need to be refactored if a different cryptosystem is used.
  std::shared_ptr<IThresholdSigner> getSigner(SeqNum seq) const;
  std::shared_ptr<IThresholdVerifier> getMultisigVerifier(SeqNum seq) const;
  std::array<std::pair<SeqNum, std::shared_ptr<IThresholdVerifier>>, 2> getLatestVerifiers() const;
  std::array<std::shared_ptr<IThresholdSigner>, 2> getLatestSigners() const;

 private:
  /**
   *  Holds Cryptosystem, signers and verifiers per checkpoint
   */
  struct CryptoSystemWrapper {
    CryptoSystemWrapper(std::unique_ptr<Cryptosystem>&& cs);
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

    void init();
  };
  using SeqToSystemMap = std::map<std::uint64_t, std::shared_ptr<CryptoSystemWrapper>>;

  // accessing existing Cryptosystems
  std::shared_ptr<CryptoSystemWrapper> get(const SeqNum& sn) const;

  // create CryptoSys for sn if still doesn't exist
  std::shared_ptr<CryptoSystemWrapper> create(const SeqNum& sn);

  CryptoManager(std::unique_ptr<Cryptosystem>&& cryptoSys);
  logging::Logger& logger() const;
  CryptoManager(const CryptoManager&) = delete;
  CryptoManager(const CryptoManager&&) = delete;
  CryptoManager& operator=(const CryptoManager&) = delete;
  CryptoManager& operator=(const CryptoManager&&) = delete;

  void assertMapSizeValid() const;
  const SeqToSystemMap& getSeqToSystem() const;

  // chckp -> CryptoSys
  // TODO: this can be converted to a concurrent queue instead of using a mutex
  SeqToSystemMap cryptoSystems_;
  // Old cryptosystems can be removed on a checkpoint, which might invalidate
  // existing cryptoSystems_ iterators. We thus protect cryptoSystems_ access with a mutex
  // and rely on shared_ptr to keep old cryptosystems alive in concurrent threads when they lag
  mutable std::mutex mutex_;
};
}  // namespace bftEngine
