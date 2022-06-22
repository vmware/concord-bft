// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
#include <cstdint>
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/eddsa/EdDSAMultisigVerifier.h"

int EdDSASignatureAccumulator::add(const char *sigShareWithId, int len) {
  ConcordAssertEQ(len, static_cast<int>(sizeof(SingleEdDSASignature)));
  auto &singleSignature = *reinterpret_cast<const SingleEdDSASignature *>(sigShareWithId);

  if (singleSignature.id == 0 || singleSignature.id > verifier_.maxShareID()) {
    LOG_ERROR(EDDSA_MULTISIG_LOG, "Invalid signer id" << KVLOG(singleSignature.id, verifier_.maxShareID()));
    return static_cast<int>(signatures_.size());
    ;
  }

  if (hasShareVerificationEnabled()) {
    auto result = verifier_.verifySingleSignature(
        reinterpret_cast<const uint8_t *>(expectedMsgDigest_.data()), expectedMsgDigest_.size(), singleSignature);
    if (!result) {
      invalidShares_.insert(static_cast<ShareID>(singleSignature.id));
    }
    LOG_DEBUG(EDDSA_MULTISIG_LOG, "Share id: " << singleSignature.id << "Invalid");
  }

  auto result = signatures_.insert({singleSignature.id, singleSignature});
  if (result.second) {
    LOG_DEBUG(EDDSA_MULTISIG_LOG, "Added " << KVLOG(this, singleSignature.id, signatures_.size()));
  }
  return static_cast<int>(signatures_.size());
}
void EdDSASignatureAccumulator::setExpectedDigest(const unsigned char *msg, int len) {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(len));
  expectedMsgDigest_ = std::string(reinterpret_cast<const char *>(msg), static_cast<size_t>(len));
}

size_t EdDSASignatureAccumulator::getFullSignedData(char *outThreshSig, int threshSigLen) {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(threshSigLen, signatures_.size()));
  ConcordAssertGE(static_cast<uint64_t>(threshSigLen),
                  static_cast<unsigned long>(signatures_.size()) * sizeof(SingleEdDSASignature));
  size_t offset = 0;
  for (auto &[id, sig] : signatures_) {
    UNUSED(id);
    std::memcpy(outThreshSig + offset, &sig, sizeof(SingleEdDSASignature));
    offset += sizeof(SingleEdDSASignature);
  }
  return offset;
}

bool EdDSASignatureAccumulator::hasShareVerificationEnabled() const { return verification_; }
int EdDSASignatureAccumulator::getNumValidShares() const {
  return static_cast<int>(signatures_.size() - invalidShares_.size());
}
std::set<ShareID> EdDSASignatureAccumulator::getInvalidShareIds() const { return invalidShares_; }

EdDSASignatureAccumulator::EdDSASignatureAccumulator(bool verification, const EdDSAMultisigVerifier &verifier)
    : verification_(verification), verifier_(verifier) {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(this));
}

EdDSAMultisigVerifier::EdDSAMultisigVerifier(const std::vector<SingleVerifier> &verifiers,
                                             const size_t signersCount,
                                             const size_t threshold)
    : verifiers_(verifiers), signersCount_(signersCount), threshold_(threshold) {
  ConcordAssertEQ(verifiers.size(), signersCount + 1);
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(this, verifiers_.size(), threshold_));
}

IThresholdAccumulator *EdDSAMultisigVerifier::newAccumulator(bool withShareVerification) const {
  return new EdDSASignatureAccumulator(withShareVerification, *this);
}

bool EdDSAMultisigVerifier::verifySingleSignature(const uint8_t *msg,
                                                  size_t msgLen,
                                                  const SingleEdDSASignature &signature) const {
  return verifiers_[signature.id].verify(msg, msgLen, signature.signatureBytes.data(), signature.signatureBytes.size());
}

bool EdDSAMultisigVerifier::verify(const char *msg, int msgLen, const char *sig, int sigLen) const {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(this, signersCount_, threshold_, sigLen));
  auto msgLenUnsigned = static_cast<size_t>(msgLen);
  auto sigLenUnsigned = static_cast<unsigned long>(sigLen);
  ConcordAssert(sigLenUnsigned % sizeof(SingleEdDSASignature) == 0);
  const auto signatureCountInBuffer = sigLenUnsigned / sizeof(SingleEdDSASignature);

  if (signatureCountInBuffer < threshold_) {
    return false;
  }

  const SingleEdDSASignature *allSignatures = reinterpret_cast<const SingleEdDSASignature *>(sig);
  size_t validSignatureCount = 0;

  for (int i = 0; i < static_cast<int>(signatureCountInBuffer); i++) {
    auto &currentSignature = allSignatures[i];
    if (currentSignature.id == 0 || currentSignature.id >= verifiers_.size()) {
      LOG_ERROR(EDDSA_MULTISIG_LOG, "Invalid signer id" << KVLOG(currentSignature.id, verifiers_.size()));
      continue;
    }
    auto result = verifySingleSignature(reinterpret_cast<const uint8_t *>(msg), msgLenUnsigned, currentSignature);
    LOG_DEBUG(EDDSA_MULTISIG_LOG, "Verified id: " << KVLOG(currentSignature.id, result));

    validSignatureCount += result == true;
  }

  bool result = validSignatureCount >= threshold_;
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(validSignatureCount, threshold_));
  return result;
}

int EdDSAMultisigVerifier::requiredLengthForSignedData() const {
  return static_cast<int>(static_cast<unsigned long>(signersCount_) * sizeof(SingleEdDSASignature));
}
const IPublicKey &EdDSAMultisigVerifier::getPublicKey() const { return verifiers_[0].getPubKey(); }
const IShareVerificationKey &EdDSAMultisigVerifier::getShareVerificationKey(ShareID signer) const {
  return verifiers_[static_cast<size_t>(signer)].getPubKey();
}
size_t EdDSAMultisigVerifier::maxShareID() const { return signersCount_ + 1; }
