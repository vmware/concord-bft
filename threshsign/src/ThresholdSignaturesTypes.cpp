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

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/IThresholdFactory.h"
#include "threshsign/ThresholdSignaturesSchemes.h"
#include "yaml_utils.hpp"
#include "Logger.hpp"
#include "string.hpp"

using concord::util::isValidHexString;

Cryptosystem::Cryptosystem(const std::string& sysType,
                           const std::string& sysSubtype,
                           uint16_t sysNumSigners,
                           uint16_t sysThreshold)
    : type_(sysType),
      subtype_(sysSubtype),
      numSigners_(sysNumSigners),
      threshold_(sysThreshold),
      forceMultisig_(numSigners_ == threshold_),
      signerID_(NID),
      publicKey_("uninitialized") {
  if (!isValidCryptosystemSelection(sysType, sysSubtype, sysNumSigners, sysThreshold)) {
    throw std::runtime_error(
        "Invalid cryptosystem selection:"
        " primary type: " +
        sysType + ", subtype: " + sysSubtype + ", with " + std::to_string(sysNumSigners) +
        " signers and threshold of " + std::to_string(sysThreshold) + ".");
  }
}

// Helper function to generateNewPseudorandomKeys.
IThresholdFactory* Cryptosystem::createThresholdFactory() {
#ifdef USE_RELIC
  if (type_ == MULTISIG_BLS_SCHEME || forceMultisig_) {
    return new BLS::Relic::BlsThresholdFactory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype_.c_str()),
                                               true);
  }
  if (type_ == THRESHOLD_BLS_SCHEME) {
    return new BLS::Relic::BlsThresholdFactory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype_.c_str()));
  }
#endif

#ifdef USE_EDDSA_OPENSSL
  if (type_ == MULTISIG_EDDSA_SCHEME) {
    return new EdDSAMultisigFactory();
  }
#endif

  // This should never occur because Cryptosystem validates its parameters
  // in its constructor.
  throw std::runtime_error(
      "Using cryptosystem of unsupported"
      " type: " +
      type_ + ".");
}

void Cryptosystem::generateNewPseudorandomKeys() {
  std::unique_ptr<IThresholdFactory> factory(createThresholdFactory());

  auto [signers, verifier] = factory->newRandomSigners(threshold_, numSigners_);
  if (type_ == THRESHOLD_BLS_SCHEME) publicKey_ = verifier->getPublicKey().toString();

  verificationKeys_.clear();
  verificationKeys_.resize(static_cast<size_t>(numSigners_ + 1));
  verificationKeys_[0] = "";  // Account for 1-indexing of signer IDs.
  for (uint16_t i = 1; i <= numSigners_; ++i) {
    verificationKeys_[i] = verifier->getShareVerificationKey(static_cast<ShareID>(i)).toString();
  }

  privateKeys_.clear();
  privateKeys_.resize(static_cast<size_t>(numSigners_ + 1));
  privateKeys_[0] = "";  // Account for 1-indexing of signer IDs.
  for (uint16_t i = 1; i <= numSigners_; ++i) {
    privateKeys_[i] = signers[i]->getShareSecretKey().toString();
  }

  signerID_ = NID;
}

std::pair<std::string, std::string> Cryptosystem::generateNewKeyPair() {
  std::unique_ptr<IThresholdFactory> factory(createThresholdFactory());
  auto keyPair = factory->newKeyPair();
  return std::make_pair(keyPair.first->toString(), keyPair.second->toString());
}

std::string Cryptosystem::getSystemPublicKey() const {
  if ((forceMultisig_ || type_ == THRESHOLD_BLS_SCHEME) && publicKey_.length() < 1) {
    throw std::runtime_error(
        "A public key has not been"
        " generated or loaded for this cryptosystem.");
  }
  return publicKey_;
}

std::vector<std::string> Cryptosystem::getSystemVerificationKeys() const {
  std::vector<std::string> output;
  if (verificationKeys_.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw std::runtime_error(
        "Verification keys have not been"
        " generated or loaded for this cryptosystem.");
  }
  // This should create a new copy of verificationKeys since we are returning by
  // value.
  return verificationKeys_;
}

std::vector<std::string> Cryptosystem::getSystemPrivateKeys() const {
  std::vector<std::string> output;
  if (privateKeys_.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw std::runtime_error(
        "Private keys have not been"
        " generated or loaded for this cryptosystem.");
  }
  // This should create a new copy of privateKeys since we are returning by
  // value.
  return privateKeys_;
}

std::string Cryptosystem::getPrivateKey(uint16_t signerIndex) const {
  if ((signerIndex < 1) || (signerIndex > numSigners_))
    throw std::out_of_range(__PRETTY_FUNCTION__ + std::string("Signer index out of range: ") +
                            std::to_string(signerIndex));

  if (privateKeys_.size() == static_cast<uint16_t>(numSigners_ + 1)) {
    return privateKeys_[signerIndex];
  } else if ((privateKeys_.size() == 1) && (signerID_ == signerIndex)) {
    return privateKeys_.front();
  }

  throw std::runtime_error(
      "Private keys have not been"
      " generated or loaded for this cryptosystem.");
}

void Cryptosystem::loadKeys(const std::string& publicKey, const std::vector<std::string>& verificationKeys) {
  validatePublicKey(publicKey);
  if (verificationKeys.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw std::runtime_error(
        "Incorrect number of verification keys provided: " + std::to_string(verificationKeys.size()) + " (expected " +
        std::to_string(numSigners_ + 1) + ").");
  }
  for (size_t i = 1; i <= numSigners_; ++i) validateVerificationKey(verificationKeys[i]);

  verificationKeys_.clear();
  privateKeys_.clear();
  publicKey_ = publicKey;

  signerID_ = NID;

  verificationKeys_ = verificationKeys;
}

void Cryptosystem::loadPrivateKey(uint16_t signerIndex, const std::string& key) {
  if ((signerIndex < 1) || (signerIndex > numSigners_))
    throw std::out_of_range(__PRETTY_FUNCTION__ + std::string("Signer index out of range: ") +
                            std::to_string(signerIndex));

  validatePrivateKey(key);

  signerID_ = signerIndex;
  privateKeys_.clear();
  privateKeys_.push_back(key);
}

void Cryptosystem::updateKeys(const std::string& shareSecretKey, const std::string& shareVerificationKey) {
  privateKeys_.clear();
  privateKeys_.push_back(shareSecretKey);
  verificationKeys_[signerID_] = shareVerificationKey;
  // we don't care about publicKey_ since for multisig it's computed dynamically
}

void Cryptosystem::updateVerificationKey(const std::string& shareVerificationKey, const std::uint16_t& signerIndex) {
  verificationKeys_[signerIndex] = shareVerificationKey;
}

IThresholdVerifier* Cryptosystem::createThresholdVerifier(uint16_t threshold) {
  if (publicKey_.length() < 1) {
    throw std::runtime_error(
        "Attempting to create a threshold"
        " verifier for a cryptosystem with no public key loaded.");
  }
  if (verificationKeys_.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw std::runtime_error(
        "Attempting to create a threshold"
        " verifier for a cryptosystem without verification keys loaded.");
  }

  std::unique_ptr<IThresholdFactory> factory(createThresholdFactory());
  return factory->newVerifier(
      (threshold > 0) ? threshold : threshold_, numSigners_, publicKey_.c_str(), verificationKeys_);
}

IThresholdSigner* Cryptosystem::createThresholdSigner() {
  if (privateKeys_.size() != 1) {
    if (privateKeys_.size() < 1) {
      throw std::runtime_error(
          "Attempting to create a"
          " threshold signer for a cryptosystem with no private keys loaded.");
    } else {
      throw std::runtime_error(
          "Attempting to create a"
          " threshold signer for a cryptosystem with more than one private key"
          " loaded without selecting a signer.");
    }
  }

  std::unique_ptr<IThresholdFactory> factory(createThresholdFactory());
  // Note we add 1 to the signer ID because IThresholdSigner seems to use a
  // convention in which signer IDs are 1-indexed.
  return factory->newSigner(signerID_, privateKeys_.front().c_str());
}

void Cryptosystem::validateKey(const std::string& key, size_t expectedSize) const {
  auto isValidHex = isValidHexString(key);
  if ((expectedSize == 0 || (key.length() == expectedSize)) && isValidHex) {
    return;
  }

  throw std::runtime_error("Invalid key for this cryptosystem (type " + type_ + " and subtype " + subtype_ +
                           "): " + key + " expected key size: " + std::to_string(expectedSize) + ", Actual key size: " +
                           std::to_string(key.length()) + ", IsValidHex: " + std::to_string(isValidHex));
}

void Cryptosystem::validatePublicKey(const std::string& key) const {
#ifdef USE_EDDSA_OPENSSL
  UNUSED(key);
  return;
#else
  constexpr const size_t expectedKeyLength = 130u;
  validateKey(key, expectedKeyLength);
#endif
}

void Cryptosystem::validateVerificationKey(const std::string& key) const {
#ifdef USE_EDDSA_OPENSSL
  constexpr const size_t expectedKeyLength = EdDSAPublicKeyByteSize * 2;
#else
  constexpr const size_t expectedKeyLength = 130u;
#endif
  validateKey(key, expectedKeyLength);
}

void Cryptosystem::validatePrivateKey(const std::string& key) const {
#ifdef USE_EDDSA_OPENSSL
  constexpr const size_t expectedKeyLength = EdDSAPrivateKeyByteSize * 2;
#else
  // We currently do not validate the length of the private key's string
  // representation because the length of its serialization varies slightly.
  constexpr const size_t expectedKeyLength = 0;
#endif
  validateKey(key, expectedKeyLength);
}

bool Cryptosystem::isValidCryptosystemSelection(const std::string& type, const std::string& subtype) {
#ifdef USE_RELIC
  if (type == MULTISIG_BLS_SCHEME) {
    try {
      BLS::Relic::BlsThresholdFactory factory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
      return true;
    } catch (std::exception& e) {
      LOG_FATAL(THRESHSIGN_LOG, e.what());
      return false;
    }
  }
  if (type == THRESHOLD_BLS_SCHEME) {
    try {
      BLS::Relic::BlsThresholdFactory factory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
      return true;
    } catch (std::exception& e) {
      return false;
    }
  }
#endif
#ifdef USE_EDDSA_OPENSSL
  UNUSED(subtype);
  if (type == MULTISIG_EDDSA_SCHEME) {
    return true;
  }
#endif
  return false;
}

bool Cryptosystem::isValidCryptosystemSelection(const std::string& type,
                                                const std::string& subtype,
                                                uint16_t numSigners,
                                                uint16_t threshold) {
  // Automatically return false if numSigners and threshold are inherently
  // invalid. Note we have chosen to disallow 0 as either numSigners or
  // threshold, as such Cryptosystems would not be useful, but supporting them
  // could introduce additional corner cases to the Cryptosystem class or code
  // using it.
  if ((numSigners < 1) || (threshold < 1) || (threshold > numSigners)) {
    return false;
  }

  return isValidCryptosystemSelection(type, subtype);
}

const std::vector<std::pair<std::string, std::string>>& Cryptosystem::getAvailableCryptosystemTypes() {
  static const std::vector<std::pair<std::string, std::string>> cryptoSystems = {
#ifdef USE_RELIC
      {MULTISIG_BLS_SCHEME, "an elliptical curve type, for example, BN-P254"},
      {THRESHOLD_BLS_SCHEME, "an elliptical curve type, for example, BN-P254"},
#endif
#ifdef USE_EDDSA_OPENSSL
      {MULTISIG_EDDSA_SCHEME, "EdDSA 25519"}
#endif
  };
  return cryptoSystems;
}
void Cryptosystem::writeConfiguration(std::ostream& output, const std::string& prefix, const uint16_t& replicaId) {
  uint16_t numReplicas = getNumSigners();
  output << "\n# " << prefix << " threshold cryptosystem configuration.\n";
  output << prefix << "_cryptosystem_type: " << getType() << "\n";
  output << prefix << "_cryptosystem_subtype_parameter: " << getSubtype() << "\n";
  output << prefix << "_cryptosystem_num_signers: " << numReplicas << "\n";
  if (getType() == THRESHOLD_BLS_SCHEME || getType() == MULTISIG_EDDSA_SCHEME)
    output << prefix << "_cryptosystem_threshold: " << getThreshold() << "\n";
  output << prefix << "_cryptosystem_public_key: " << getSystemPublicKey() << "\n";
  std::vector<std::string> verificationKeys = getSystemVerificationKeys();
  output << prefix << "_cryptosystem_verification_keys:\n";
  for (uint16_t i = 1; i <= numReplicas; ++i) output << "  - " << verificationKeys[i] << "\n";
  output << "\n";

  output << prefix << "_cryptosystem_private_key: " << getPrivateKey((uint16_t)(replicaId + 1)) << "\n\n";
}

Cryptosystem* Cryptosystem::fromConfiguration(std::istream& input,
                                              const std::string& prefix,
                                              const uint16_t& signerIndex,
                                              std::string& type,
                                              std::string& subtype,
                                              std::string& thrPrivateKey,
                                              std::string& thrPublicKey,
                                              std::vector<std::string>& thrVerificationKeys) {
  using namespace concord::util;
  type = yaml::readValue<std::string>(input, prefix + "_cryptosystem_type");
  subtype = yaml::readValue<std::string>(input, prefix + "_cryptosystem_subtype_parameter");
  std::uint16_t numSigners = yaml::readValue<std::uint16_t>(input, prefix + "_cryptosystem_num_signers");
  std::string publicKey = "uninitialized";
  uint16_t threshold = 1;
  if (type == THRESHOLD_BLS_SCHEME || type == MULTISIG_EDDSA_SCHEME)
    threshold = yaml::readValue<std::uint16_t>(input, prefix + "_cryptosystem_threshold");
  else if (type == MULTISIG_BLS_SCHEME)
    threshold = numSigners;
  thrPublicKey = yaml::readValue<std::string>(input, prefix + "_cryptosystem_public_key");
  thrVerificationKeys = yaml::readCollection<std::string>(input, prefix + "_cryptosystem_verification_keys");
  if (thrVerificationKeys.size() != numSigners)
    throw std::runtime_error("expected " + std::to_string(numSigners) + std::string(" verification keys, got: ") +
                             std::to_string(thrVerificationKeys.size()));

  thrPrivateKey = yaml::readValue<std::string>(input, prefix + "_cryptosystem_private_key");

  Cryptosystem* sys = new Cryptosystem(type, subtype, numSigners, threshold);

  thrVerificationKeys.insert(thrVerificationKeys.begin(), "");
  sys->loadKeys(thrPublicKey, thrVerificationKeys);
  sys->loadPrivateKey(signerIndex, thrPrivateKey);

  return sys;
}
