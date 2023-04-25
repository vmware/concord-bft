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

#include "ReplicaConfig.hpp"
#include "CryptoManager.hpp"
#include "util/containers.hpp"

namespace bftEngine {

std::shared_ptr<CryptoManager> CryptoManager::s_cm{nullptr};

std::shared_ptr<CryptoManager> CryptoManager::init(std::unique_ptr<Cryptosystem>&& cryptoSys) {
  ConcordAssert(cryptoSys != nullptr);
  s_cm.reset(new CryptoManager(std::move(cryptoSys)));
  LOG_INFO(s_cm->logger(), "CryptoManager Initialized");
  return s_cm;
}

CryptoManager& CryptoManager::instance() {
  ConcordAssertNE(s_cm.get(), nullptr);
  return *s_cm;
}
std::shared_ptr<IThresholdSigner> CryptoManager::thresholdSignerForSlowPathCommit(const SeqNum sn) const {
  return get(sn)->thresholdSigner_;
}
std::shared_ptr<IThresholdVerifier> CryptoManager::thresholdVerifierForSlowPathCommit(const SeqNum sn) const {
  return get(sn)->thresholdVerifierForSlowPathCommit_;
}
std::shared_ptr<IThresholdSigner> CryptoManager::thresholdSignerForCommit(const SeqNum sn) const {
  return get(sn)->thresholdSigner_;
}
std::shared_ptr<IThresholdVerifier> CryptoManager::thresholdVerifierForCommit(const SeqNum sn) const {
  return get(sn)->thresholdVerifierForCommit_;
}
std::shared_ptr<IThresholdSigner> CryptoManager::thresholdSignerForOptimisticCommit(const SeqNum sn) const {
  return get(sn)->thresholdSigner_;
}
std::shared_ptr<IThresholdVerifier> CryptoManager::thresholdVerifierForOptimisticCommit(const SeqNum sn) const {
  return get(sn)->thresholdVerifierForOptimisticCommit_;
}

std::unique_ptr<Cryptosystem>& CryptoManager::getLatestCryptoSystem() const {
  return checkpointToSystem().rbegin()->second->cryptosys_;
}

/**
 * @return An algorithm identifier for the latest threshold signature scheme
 */
concord::crypto::SignatureAlgorithm CryptoManager::getLatestSignatureAlgorithm() const {
  const std::unordered_map<std::string, concord::crypto::SignatureAlgorithm> typeToAlgorithm{
      {MULTISIG_EDDSA_SCHEME, concord::crypto::SignatureAlgorithm::EdDSA},
  };
  auto currentType = getLatestCryptoSystem()->getType();
  return typeToAlgorithm.at(currentType);
}

// IMultiSigKeyGenerator methods
std::tuple<std::string, std::string, concord::crypto::SignatureAlgorithm> CryptoManager::generateMultisigKeyPair() {
  LOG_INFO(logger(), "Generating new multisig key pair");
  auto [priv, pub] = getLatestCryptoSystem()->generateNewKeyPair();
  return {priv, pub, getLatestSignatureAlgorithm()};
}

std::set<SeqNum> CryptoManager::syncPrivateKeysAfterST(
    const std::map<SeqNum, std::pair<std::string, std::string>>& candidateKeys) {
  ConcordAssert(ReplicaConfig::instance().singleSignatureScheme);
  std::lock_guard<std::mutex> guard(mutex_);
  std::set<SeqNum> candidatesToPersist;
  for (auto& [keyGenerationSn, keyPair] : candidateKeys) {
    auto effeciveCheckpoint = (keyGenerationSn / checkpointWindowSize) + 2;
    if (checkpointToSystem().find(effeciveCheckpoint) != checkpointToSystem().end()) {
      auto& cryptoSystem = checkpointToSystem().at(effeciveCheckpoint)->cryptosys_;
      auto& [privateKey, publicKey] = keyPair;
      LOG_INFO(logger(),
               "Found candidate matching the same checkpoint as an existing cryptoSystem"
                   << KVLOG(keyGenerationSn, effeciveCheckpoint, publicKey, cryptoSystem->getMyVerificationKey()));
      if (cryptoSystem->getMyVerificationKey() == publicKey) {
        cryptoSystem->updateKeys(privateKey, publicKey);
        candidatesToPersist.emplace(keyGenerationSn);
        LOG_INFO(logger(), "Updated private key for cryptosystem with checkpoint:" << effeciveCheckpoint);
      }
    }
  }
  return candidatesToPersist;
}

void CryptoManager::onPrivateKeyExchange(const std::string& secretKey,
                                         const std::string& verificationKey,
                                         const SeqNum& keyGenerationSn) {
  LOG_INFO(logger(), "Private key exchange:" << KVLOG(keyGenerationSn, verificationKey));
  auto sys_wrapper = create(keyGenerationSn);
  sys_wrapper->cryptosys_->updateKeys(secretKey, verificationKey);
  sys_wrapper->init();
}

void CryptoManager::onPublicKeyExchange(const std::string& verificationKey,
                                        const std::uint16_t& signerIndex,
                                        const SeqNum& keyGenerationSn) {
  LOG_INFO(logger(), "Public key exchange:" << KVLOG(keyGenerationSn, signerIndex, verificationKey));
  auto sys = create(keyGenerationSn);
  sys->cryptosys_->updateVerificationKey(verificationKey, signerIndex);
  sys->init();
}

void CryptoManager::onCheckpoint(uint64_t newCheckpoint) {
  std::lock_guard<std::mutex> guard(mutex_);
  std::vector<CheckpointNum> checkpointsToRemove;
  auto checkpointOfCurrentCryptoSystem = getCheckpointOfCryptosystemForSeq(newCheckpoint * checkpointWindowSize);

  concord::util::containers::erase_if(
      cryptoSystems_, [this, checkpointOfCurrentCryptoSystem, newCheckpoint](const auto& checkpointToSystem) {
        auto checkpoint = checkpointToSystem.first;
        if (checkpoint < checkpointOfCurrentCryptoSystem) {
          LOG_INFO(logger(), "Removing stale cryptosystem " << KVLOG(checkpoint, newCheckpoint, cryptoSystems_.size()));
          return true;
        }
        return false;
      });
  assertMapSizeValid();
}

std::shared_ptr<IThresholdSigner> CryptoManager::getSigner(SeqNum seq) const { return get(seq)->thresholdSigner_; }

std::array<std::pair<SeqNum, std::shared_ptr<IThresholdVerifier>>, 2> CryptoManager::getLatestVerifiers() const {
  std::lock_guard<std::mutex> guard(mutex_);
  auto riter = checkpointToSystem().rbegin();
  std::array<std::pair<SeqNum, std::shared_ptr<IThresholdVerifier>>, 2> result;
  result[0] = {riter->first, riter->second->thresholdVerifierForOptimisticCommit_};
  riter++;
  if (riter != checkpointToSystem().rend() && riter->second != nullptr) {
    result[1] = {riter->first, riter->second->thresholdVerifierForOptimisticCommit_};
  }
  return result;
}

std::array<std::shared_ptr<IThresholdSigner>, 2> CryptoManager::getLatestSigners() const {
  std::lock_guard<std::mutex> guard(mutex_);
  auto riter = checkpointToSystem().rbegin();
  ConcordAssertNE(riter->second, nullptr);
  std::array<std::shared_ptr<IThresholdSigner>, 2> result;
  result[0] = riter->second->thresholdSigner_;
  ++riter;
  if (riter != checkpointToSystem().rend() && riter->second != nullptr) {
    result[1] = riter->second->thresholdSigner_;
  }
  return result;
}

CryptoManager::CryptoSystemWrapper::CryptoSystemWrapper(std::unique_ptr<Cryptosystem>&& cs)
    : cryptosys_(std::move(cs)) {}

void CryptoManager::CryptoSystemWrapper::init() {
  std::uint16_t f{ReplicaConfig::instance().getfVal()};
  std::uint16_t c{ReplicaConfig::instance().getcVal()};
  std::uint16_t numSigners{ReplicaConfig::instance().getnumReplicas()};
  thresholdSigner_.reset(cryptosys_->createThresholdSigner());
  thresholdVerifierForSlowPathCommit_.reset(cryptosys_->createThresholdVerifier(f * 2 + c + 1));
  thresholdVerifierForCommit_.reset(cryptosys_->createThresholdVerifier(f * 3 + c + 1));
  thresholdVerifierForOptimisticCommit_.reset(cryptosys_->createThresholdVerifier(numSigners));
}

CheckpointNum CryptoManager::getCheckpointOfCryptosystemForSeq(const SeqNum sn) const {
  // find last chckp that is less than a chckp of a given sn
  const uint64_t checkpointUpperBound = sn == 0 ? 0 : ((sn - 1) / checkpointWindowSize);
  for (auto riter = checkpointToSystem().rbegin(); riter != checkpointToSystem().rend(); ++riter) {
    auto& [checkpointCandidate, cryptosystem] = *riter;
    if (checkpointCandidate <= checkpointUpperBound) {
      LOG_DEBUG(logger(),
                "Found cryptosystem for " << KVLOG(sn,
                                                   checkpointUpperBound,
                                                   checkpointCandidate,
                                                   cryptosystem,
                                                   checkpointToSystem().size(),
                                                   checkpointWindowSize));
      return checkpointCandidate;
    }
  }

  // Replicas might encounter old messages, crashing is not a valid behavior
  auto fallbackSystemCheckpoint = checkpointToSystem().rbegin()->first;
  LOG_WARN(logger(),
           "Cryptosystem not found, returning latest"
               << KVLOG(sn, checkpointUpperBound, checkpointToSystem().size(), fallbackSystemCheckpoint));
  return fallbackSystemCheckpoint;
}

std::shared_ptr<CryptoManager::CryptoSystemWrapper> CryptoManager::get(const SeqNum& sn) const {
  std::lock_guard<std::mutex> guard(mutex_);
  return checkpointToSystem().at(getCheckpointOfCryptosystemForSeq(sn));
}

std::shared_ptr<CryptoManager::CryptoSystemWrapper> CryptoManager::create(const SeqNum& keyGenerationSn) {
  std::lock_guard<std::mutex> guard(mutex_);
  assertMapSizeValid();
  // Cryptosystem for this sn will be activated upon reaching a second checkpoint from now
  CheckpointNum chckp = (keyGenerationSn / checkpointWindowSize) + 2;
  if (auto it = checkpointToSystem().find(chckp); it != checkpointToSystem().end()) {
    LOG_WARN(logger(), "Cryptosystem already exists for checkpoint " << KVLOG(keyGenerationSn, chckp));
    return it->second;
  }

  // copy construct new Cryptosystem from a last one as we want it to include all the existing keys
  // Cryptosystems are created either by key exchanges or ST, in ST they are rebuilt in ascending sequence order
  // If a key exchange execution results in copying the wrong cryptosystem, the key state will be rebuilt
  // in the next ST cycle
  std::unique_ptr<Cryptosystem> cs =
      std::make_unique<Cryptosystem>(*checkpointToSystem().rbegin()->second->cryptosys_.get());

  auto insert_result =
      cryptoSystems_.insert(std::make_pair(chckp, std::make_shared<CryptoSystemWrapper>(std::move(cs))));
  auto ret = insert_result.first->second;
  LOG_INFO(logger(),
           "Created new cryptosystem for checkpoint: " << chckp << ", insertion success: " << insert_result.second
                                                       << KVLOG(cryptoSystems_.size(), keyGenerationSn));
  return ret;
}

CryptoManager::CryptoManager(std::unique_ptr<Cryptosystem>&& cryptoSys)
    : cryptoSystems_{{0, std::make_shared<CryptoSystemWrapper>(std::move(cryptoSys))}} {
  // default cryptosystem is always at chckp 0
  cryptoSystems_.begin()->second->init();
}

logging::Logger& CryptoManager::logger() const {
  static logging::Logger logger_ = logging::getLogger("concord.bft.crypto-mgr");
  return logger_;
}

void CryptoManager::assertMapSizeValid() const { ConcordAssertGE(cryptoSystems_.size(), 1); }

const CryptoManager::CheckpointToSystemMap& CryptoManager::checkpointToSystem() const {
  assertMapSizeValid();
  return cryptoSystems_;
}

void CryptoManager::reset(std::shared_ptr<CryptoManager> other) { s_cm = other; }

}  // namespace bftEngine
