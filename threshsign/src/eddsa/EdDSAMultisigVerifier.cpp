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
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/eddsa/EdDSAMultisigVerifier.h"
#include <numeric>

int EdDSASignatureAccumulator::add(const char *sigShareWithId, int len) {
  ConcordAssertEQ(len, static_cast<int>(sizeof(SingleEdDSASignature)));
  auto &singleSignature = *reinterpret_cast<const SingleEdDSASignature *>(sigShareWithId);

  if (shareVerification_ && !verifier_.verifySingleSignature(msgDigest_, singleSignature)) {
    return static_cast<int>(signatures_.size());
  }

  auto result = signatures_.insert({singleSignature.id, singleSignature});
  if (result.second) {
    LOG_DEBUG(EDDSA_MULTISIG_LOG, "Added " << KVLOG(this, singleSignature.id, signatures_.size()));
  }
  return static_cast<int>(signatures_.size());
}
void EdDSASignatureAccumulator::setExpectedDigest(const unsigned char *msg, int len) {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(len));
  msgDigest_ = std::string(reinterpret_cast<const char *>(msg), static_cast<size_t>(len));
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

bool EdDSASignatureAccumulator::hasShareVerificationEnabled() const { return shareVerification_; }
int EdDSASignatureAccumulator::getNumValidShares() const { return 0; }
std::set<ShareID> EdDSASignatureAccumulator::getInvalidShareIds() const { return std::set<ShareID>(); }

EdDSASignatureAccumulator::EdDSASignatureAccumulator(const EdDSAMultisigVerifier &verifier, bool shareVerification)
    : verifier_(verifier), shareVerification_(shareVerification) {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(this));
}

EdDSAMultisigVerifier::EdDSAMultisigVerifier(const std::vector<SSLEdDSAPublicKey> &publicKeys,
                                             const size_t signersCount,
                                             const size_t threshold)
    : publicKeys_(), signersCount_(signersCount + 1), threshold_(threshold) {
  if (signersCount_ == publicKeys.size()) {
    publicKeys_ = publicKeys;
  }
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(this, publicKeys_.size()));
}

IThresholdAccumulator *EdDSAMultisigVerifier::newAccumulator(bool withShareVerification) const {
  return new EdDSASignatureAccumulator(*this, withShareVerification);
}

bool EdDSAMultisigVerifier::verifySingleSignature(const std::string_view msg,
                                                  const SingleEdDSASignature &signature) const {
  if (signature.id == 0) {
    return false;
  }

  auto result = publicKeys_[signature.id].verify(reinterpret_cast<const uint8_t *>(msg.data()),
                                                 msg.size(),
                                                 signature.signatureBytes.data(),
                                                 signature.signatureBytes.size());
  LOG_DEBUG(EDDSA_MULTISIG_LOG, "Verified id: " << KVLOG(signature.id, result));
  return result;
}

bool EdDSAMultisigVerifier::verify(const char *msg, int msgLen, const char *sig, int sigLen) const {
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(this, publicKeys_.size(), signersCount_, threshold_, sigLen));
  auto msgLenUnsigned = static_cast<size_t>(msgLen);
  auto sigLenUnsigned = static_cast<unsigned long>(sigLen);
  ConcordAssert(sigLenUnsigned % sizeof(SingleEdDSASignature) == 0);
  const auto signatureCountInBuffer = sigLenUnsigned / sizeof(SingleEdDSASignature);

  if (signatureCountInBuffer < threshold_) {
    return false;
  }

  const SingleEdDSASignature *allSignatures = reinterpret_cast<const SingleEdDSASignature *>(sig);
  size_t validSignatureCount = 0;

  for (int i = 0; i < (int)signatureCountInBuffer; i++) {
    auto result = verifySingleSignature(std::string_view(msg, msgLenUnsigned), allSignatures[i]);
    validSignatureCount += result == true;
  }

  bool result = validSignatureCount >= threshold_;
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(validSignatureCount, threshold_));
  return result;
}

int EdDSAMultisigVerifier::requiredLengthForSignedData() const {
  return static_cast<int>(static_cast<unsigned long>(signersCount_) * sizeof(SingleEdDSASignature));
}
const IPublicKey &EdDSAMultisigVerifier::getPublicKey() const { return publicKeys_[0]; }
const IShareVerificationKey &EdDSAMultisigVerifier::getShareVerificationKey(ShareID signer) const {
  return publicKeys_[static_cast<size_t>(signer)];
}
