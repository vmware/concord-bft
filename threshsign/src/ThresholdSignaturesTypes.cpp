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

#include <regex>

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/bls/relic/BlsThresholdFactory.h"
#include "threshsign/bls/relic/PublicParametersFactory.h"
#include "yaml_utils.hpp"
#include "Logger.hpp"

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
    throw InvalidCryptosystemException(
        "Invalid cryptosystem selection:"
        " primary type: " +
        sysType + ", subtype: " + sysSubtype + ", with " + std::to_string(sysNumSigners) +
        " signers and threshold of " + std::to_string(sysThreshold) + ".");
  }
}

// Helper function to generateNewPseudorandomKeys.
IThresholdFactory* Cryptosystem::createThresholdFactory() {
  if (type_ == MULTISIG_BLS_SCHEME || forceMultisig_) {
    return new BLS::Relic::BlsThresholdFactory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype_.c_str()),
                                               true);
  } else if (type_ == THRESHOLD_BLS_SCHEME) {
    return new BLS::Relic::BlsThresholdFactory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype_.c_str()));
  } else {
    // This should never occur because Cryptosystem validates its parameters
    // in its constructor.
    throw InvalidCryptosystemException(
        "Using cryptosystem of unsupported"
        " type: " +
        type_ + ".");
  }
}

void Cryptosystem::generateNewPseudorandomKeys() {
  std::unique_ptr<IThresholdFactory> factory(createThresholdFactory());
  std::vector<IThresholdSigner*> signers;
  IThresholdVerifier* verifier;

  std::tie(signers, verifier) = factory->newRandomSigners(threshold_, numSigners_);
  if (forceMultisig_ || type_ == THRESHOLD_BLS_SCHEME) publicKey_ = verifier->getPublicKey().toString();

  verificationKeys_.clear();
  verificationKeys_.push_back("");  // Account for 1-indexing of signer IDs.
  for (uint16_t i = 1; i <= numSigners_; ++i) {
    verificationKeys_.push_back(verifier->getShareVerificationKey(static_cast<ShareID>(i)).toString());
  }

  privateKeys_.clear();
  privateKeys_.push_back("");  // Account for 1-indexing of signer IDs.
  for (uint16_t i = 1; i <= numSigners_; ++i) {
    privateKeys_.push_back(signers[i]->getShareSecretKey().toString());
  }

  for (auto signer : signers) {
    delete signer;
  }

  signerID_ = NID;

  delete verifier;
}

std::string Cryptosystem::getSystemPublicKey() const {
  if ((forceMultisig_ || type_ == THRESHOLD_BLS_SCHEME) && publicKey_.length() < 1) {
    throw UninitializedCryptosystemException(
        "A public key has not been"
        " generated or loaded for this cryptosystem.");
  }
  return publicKey_;
}

std::vector<std::string> Cryptosystem::getSystemVerificationKeys() const {
  std::vector<std::string> output;
  if (verificationKeys_.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw UninitializedCryptosystemException(
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
    throw UninitializedCryptosystemException(
        "Private keys have not been"
        " generated or loaded for this cryptosystem.");
  }
  // This should create a new copy of privateKeys since we are returning by
  // value.
  return privateKeys_;
}

std::string Cryptosystem::getPrivateKey(uint16_t signerIndex) const {
  if ((signerIndex < 1) || (signerIndex > numSigners_)) {
    throw std::out_of_range(
        "Signer index for requested private key out of"
        " range.");
  }

  if (privateKeys_.size() == static_cast<uint16_t>(numSigners_ + 1)) {
    return privateKeys_[signerIndex];
  } else if ((privateKeys_.size() == 1) && (signerID_ == signerIndex)) {
    return privateKeys_.front();
  }

  throw UninitializedCryptosystemException(
      "Private keys have not been"
      " generated or loaded for this cryptosystem.");
}

void Cryptosystem::loadKeys(const std::string& publicKey, const std::vector<std::string>& verificationKeys) {
  validatePublicKey(publicKey);
  if (verificationKeys.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw InvalidCryptosystemException(
        "Incorrect number of verification keys provided: " + std::to_string(verificationKeys.size()) + " (expected " +
        std::to_string(numSigners_ + 1) + ").");
  }
  for (size_t i = 1; i <= numSigners_; ++i) validateVerificationKey(verificationKeys[i]);

  this->verificationKeys_.clear();
  this->privateKeys_.clear();
  this->publicKey_ = publicKey;

  signerID_ = NID;

  // This should make a copy of the verificationKeys vector we received as a
  // parameter since this.verificationKeys is stored by value.
  this->verificationKeys_ = verificationKeys;
}

void Cryptosystem::loadPrivateKey(uint16_t signerIndex, const std::string& key) {
  if ((signerIndex < 1) & (signerIndex > numSigners_)) {
    throw std::out_of_range("Signer index for provided private key out of range.");
  }
  validatePrivateKey(key);

  signerID_ = signerIndex;
  privateKeys_.clear();
  privateKeys_.push_back(key);
}

IThresholdVerifier* Cryptosystem::createThresholdVerifier(uint16_t threshold) {
  if (publicKey_.length() < 1) {
    throw UninitializedCryptosystemException(
        "Attempting to create a threshold"
        " verifier for a cryptosystem with no public key loaded.");
  }
  if (verificationKeys_.size() != static_cast<uint16_t>(numSigners_ + 1)) {
    throw UninitializedCryptosystemException(
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
      throw UninitializedCryptosystemException(
          "Attempting to create a"
          " threshold signer for a cryptosystem with no private keys loaded.");
    } else {
      throw UninitializedCryptosystemException(
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

static const size_t expectedPublicKeyLength = 130;
static const size_t expectedVerificationKeyLength = 130;

void Cryptosystem::validatePublicKey(const std::string& key) const {
  if (forceMultisig_ || type_ == THRESHOLD_BLS_SCHEME)
    if (!((key.length() == expectedPublicKeyLength) && (std::regex_match(key, std::regex("[0-9A-Fa-f]+")))))
      throw InvalidCryptosystemException("invalid public key for this cryptosystem (type " + type_ + " and subtype " +
                                         subtype_ + "): " + key);
}

void Cryptosystem::validateVerificationKey(const std::string& key) const {
  if (!((key.length() == expectedVerificationKeyLength) && (std::regex_match(key, std::regex("[0-9A-Fa-f]+")))))
    throw InvalidCryptosystemException("invalid verification key for this cryptosystem (type " + type_ +
                                       " and subtype " + subtype_ + "): " + key);
}

void Cryptosystem::validatePrivateKey(const std::string& key) const {
  // We currently do not validate the length of the private key's string
  // representation because the length of its serialization varies slightly.

  if (!std::regex_match(key, std::regex("[0-9A-Fa-f]+")))
    throw InvalidCryptosystemException("invalid private key for cryptosystem (type " + type_ + " and subtype " +
                                       subtype_ + "): " + key);
}

bool Cryptosystem::isValidCryptosystemSelection(const std::string& type, const std::string& subtype) {
  if (type == MULTISIG_BLS_SCHEME) {
    try {
      BLS::Relic::BlsThresholdFactory factory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
      return true;
    } catch (std::exception& e) {
      LOG_FATAL(THRESHSIGN_LOG, e.what());
      return false;
    }
  } else if (type == THRESHOLD_BLS_SCHEME) {
    try {
      BLS::Relic::BlsThresholdFactory factory(BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
      return true;
    } catch (std::exception& e) {
      return false;
    }
  } else {
    return false;
  }
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

void Cryptosystem::getAvailableCryptosystemTypes(std::vector<std::pair<std::string, std::string>>& ret) {
  std::pair<std::string, std::string> p;

  p.first = MULTISIG_BLS_SCHEME;
  p.second = "an elliptical curve type, for example, BN-P254";
  ret.push_back(p);

  p.first = THRESHOLD_BLS_SCHEME;
  p.second = "an elliptical curve type, for example, BN-P254";
  ret.push_back(p);
}
void Cryptosystem::writeConfiguration(std::ostream& output, const std::string& prefix, const uint16_t& replicaId) {
  uint16_t numReplicas = getNumSigners();
  output << "\n# " << prefix << " threshold cryptosystem configuration.\n";
  output << prefix << "_cryptosystem_type: " << getType() << "\n";
  output << prefix << "_cryptosystem_subtype_parameter: " << getSubtype() << "\n";
  output << prefix << "_cryptosystem_num_signers: " << numReplicas << "\n";
  if (getType() == THRESHOLD_BLS_SCHEME) output << prefix << "_cryptosystem_threshold: " << getThreshold() << "\n";
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
  if (type == THRESHOLD_BLS_SCHEME)
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

  thrVerificationKeys.insert(thrVerificationKeys.begin(), "");  // ugly
  sys->loadKeys(thrPublicKey, thrVerificationKeys);
  sys->loadPrivateKey(signerIndex, thrPrivateKey);

  return sys;
}
