// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
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

Cryptosystem::Cryptosystem(const std::string& sysType,
                           const std::string& sysSubtype,
                           uint16_t sysNumSigners,
                           uint16_t sysThreshold) {
  if (!isValidCryptosystemSelection(
          sysType, sysSubtype, sysNumSigners, sysThreshold)) {
    throw InvalidCryptosystemException(
        "Invalid cryptosystem selection:"
        " primary type: " +
        sysType + ", subtype: " + sysSubtype + ", with " +
        std::to_string(sysNumSigners) + " signers and threshold of " +
        std::to_string(sysThreshold) + ".");
  }
  type = sysType;
  subtype = sysSubtype;
  numSigners = sysNumSigners;
  threshold = sysThreshold;
  signerID = NID;
}

// Helper function to generateNewPseudorandomKeys.
IThresholdFactory* Cryptosystem::createThresholdFactory() {
  if (type == MULTISIG_BLS_SCHEME) {
    return new BLS::Relic::BlsThresholdFactory(
        BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
  } else if (type == THRESHOLD_BLS_SCHEME) {
    return new BLS::Relic::BlsThresholdFactory(
        BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
  } else {
    // This should never occur because Cryptosystem validates its parameters
    // in its constructor.
    throw InvalidCryptosystemException(
        "Using cryptosystem of unsupported"
        " type: " +
        type + ".");
  }
}

void Cryptosystem::generateNewPseudorandomKeys() {
  std::unique_ptr<IThresholdFactory> factory(createThresholdFactory());
  std::vector<IThresholdSigner*> signers;
  IThresholdVerifier* verifier;

  std::tie(signers, verifier) =
      factory->newRandomSigners(threshold, numSigners);

  publicKey = verifier->getPublicKey().toString();

  verificationKeys.clear();
  verificationKeys.push_back("");  // Account for 1-indexing of signer IDs.
  for (uint16_t i = 1; i <= numSigners; ++i) {
    verificationKeys.push_back(
        verifier->getShareVerificationKey(static_cast<ShareID>(i)).toString());
  }

  privateKeys.clear();
  privateKeys.push_back("");  // Account for 1-indexing of signer IDs.
  for (uint16_t i = 1; i <= numSigners; ++i) {
    privateKeys.push_back(signers[i]->getShareSecretKey().toString());
  }

  for (auto signer : signers) {
    delete signer;
  }

  signerID = NID;

  delete verifier;
}

std::string Cryptosystem::getSystemPublicKey() const {
  if (publicKey.length() < 1) {
    throw UninitializedCryptosystemException(
        "A public key has not been"
        " generated or loaded for this cryptosystem.");
  }
  return publicKey;
}

std::vector<std::string> Cryptosystem::getSystemVerificationKeys() const {
  std::vector<std::string> output;
  if (verificationKeys.size() != static_cast<uint16_t>(numSigners + 1)) {
    throw UninitializedCryptosystemException(
        "Verification keys have not been"
        " generated or loaded for this cryptosystem.");
  }
  // This should create a new copy of verificationKeys since we are returning by
  // value.
  return verificationKeys;
}

std::vector<std::string> Cryptosystem::getSystemPrivateKeys() const {
  std::vector<std::string> output;
  if (privateKeys.size() != static_cast<uint16_t>(numSigners + 1)) {
    throw UninitializedCryptosystemException(
        "Private keys have not been"
        " generated or loaded for this cryptosystem.");
  }
  // This should create a new copy of privateKeys since we are returning by
  // value.
  return privateKeys;
}

std::string Cryptosystem::getPrivateKey(uint16_t signerIndex) const {
  if ((signerIndex < 1) || (signerIndex > numSigners)) {
    throw std::out_of_range(
        "Signer index for requested private key out of"
        " range.");
  }

  if (privateKeys.size() == static_cast<uint16_t>(numSigners + 1)) {
    return privateKeys[signerIndex];
  } else if ((privateKeys.size() == 1) && (signerID == signerIndex)) {
    return privateKeys.front();
  }

  throw UninitializedCryptosystemException(
      "Private keys have not been"
      " generated or loaded for this cryptosystem.");
}

void Cryptosystem::loadKeys(const std::string& publicKey,
                            const std::vector<std::string>& verificationKeys) {
  if (!isValidPublicKey(publicKey)) {
    throw InvalidCryptosystemException(
        "\"" + publicKey +
        "\" is not a valid"
        " public key for this cryptosystem (type " +
        type + " and subtype " + subtype + ").");
  }
  if (verificationKeys.size() != static_cast<uint16_t>(numSigners + 1)) {
    throw InvalidCryptosystemException(
        "Incorrect number of verification keys"
        " provided: " +
        std::to_string(verificationKeys.size()) + " (expected " +
        std::to_string(numSigners + 1) + ").");
  }
  for (size_t i = 1; i <= numSigners; ++i) {
    if (!isValidVerificationKey(verificationKeys[i])) {
      throw InvalidCryptosystemException(
          "\"" + verificationKeys[i] +
          "\" is"
          " not a valid verification key for this cryptosystem (type " +
          type + " and subtype " + subtype + ").");
    }
  }

  this->verificationKeys.clear();
  this->privateKeys.clear();
  this->publicKey = publicKey;

  signerID = NID;

  // This should make a copy of the verificationKeys vector we received as a
  // parameter since this.verificationKeys is stored by value.
  this->verificationKeys = verificationKeys;
}

void Cryptosystem::loadPrivateKey(uint16_t signerIndex,
                                  const std::string& key) {
  if ((signerIndex < 1) & (signerIndex > numSigners)) {
    throw std::out_of_range(
        "Signer index for provided private key out of"
        " range.");
  }
  if (!isValidPrivateKey(key)) {
    throw InvalidCryptosystemException("\"" + key +
                                       "\" is not a valid private"
                                       " key for this cryptosystem (type " +
                                       type + " and subtype " + subtype + ").");
  }

  signerID = signerIndex;
  privateKeys.clear();
  privateKeys.push_back(key);
}

IThresholdVerifier* Cryptosystem::createThresholdVerifier() {
  if (publicKey.length() < 1) {
    throw UninitializedCryptosystemException(
        "Attempting to create a threshold"
        " verifier for a cryptosystem with no public key loaded.");
  }
  if (verificationKeys.size() != static_cast<uint16_t>(numSigners + 1)) {
    throw UninitializedCryptosystemException(
        "Attempting to create a threshold"
        " verifier for a cryptosystem without verification keys loaded.");
  }

  IThresholdFactory* factory = createThresholdFactory();

  IThresholdVerifier* verifier = factory->newVerifier(
      threshold, numSigners, publicKey.c_str(), verificationKeys);

  delete factory;
  return verifier;
}

IThresholdSigner* Cryptosystem::createThresholdSigner() {
  if (privateKeys.size() != 1) {
    if (privateKeys.size() < 1) {
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

  IThresholdFactory* factory = createThresholdFactory();

  // Note we add 1 to the signer ID because IThresholdSigner seems to use a
  // convention in which signer IDs are 1-indexed.
  IThresholdSigner* signer =
      factory->newSigner(signerID, privateKeys.front().c_str());

  delete factory;
  return signer;
}

static const size_t expectedPublicKeyLength = 130;
static const size_t expectedVerificationKeyLength = 130;

bool Cryptosystem::isValidPublicKey(const std::string& key) const {
  return (key.length() == expectedPublicKeyLength) &&
         (std::regex_match(key, std::regex("[0-9A-Fa-f]+")));
}

bool Cryptosystem::isValidVerificationKey(const std::string& key) const {
  return (key.length() == expectedVerificationKeyLength) &&
         (std::regex_match(key, std::regex("[0-9A-Fa-f]+")));
}

bool Cryptosystem::isValidPrivateKey(const std::string& key) const {
  // We currently do not validate the length of the private key's string
  // representation because the length of its serialization varies slightly.

  return std::regex_match(key, std::regex("[0-9A-Fa-f]+"));
}

bool Cryptosystem::isValidCryptosystemSelection(const std::string& type,
                                                const std::string& subtype) {
  if (type == MULTISIG_BLS_SCHEME) {
    try {
      BLS::Relic::BlsThresholdFactory factory(
          BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
      return true;
    } catch (std::exception e) {
      return false;
    }
  } else if (type == THRESHOLD_BLS_SCHEME) {
    try {
      BLS::Relic::BlsThresholdFactory factory(
          BLS::Relic::PublicParametersFactory::getByCurveType(subtype.c_str()));
      return true;
    } catch (std::exception e) {
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

  // Note MULTISIG_BLS scheme is not a true threshold scheme and is only
  // allowable if the threshold equals the number of signers.
  if ((type == MULTISIG_BLS_SCHEME) && (threshold != numSigners)) {
    return false;
  } else {
    return isValidCryptosystemSelection(type, subtype);
  }
}

void Cryptosystem::getAvailableCryptosystemTypes(
    std::vector<std::pair<std::string, std::string>>& ret) {
  std::pair<std::string, std::string> p;

  p.first = MULTISIG_BLS_SCHEME;
  p.second = "an elliptical curve type, for example, BN-P254";
  ret.push_back(p);

  p.first = THRESHOLD_BLS_SCHEME;
  p.second = "an elliptical curve type, for example, BN-P254";
  ret.push_back(p);
}
