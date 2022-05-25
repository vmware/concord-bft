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
#include "../Utils.h"
#include <numeric>

int EdDSASignatureAccumulator::add(const char *sigShareWithId, int len) {
  ConcordAssertEQ(len, sizeof(SingleEdDSASignature));
  auto signaturePtr = reinterpret_cast<const SingleEdDSASignature *>(sigShareWithId);
  auto result = signatures_.insert({signaturePtr->id, *signaturePtr});
  if (result.second) {
    LOG_INFO(EDDSA_MULTISIG_LOG,
             "this: " << this << " Added signature from replicaID: " << signaturePtr->id << " "
                      << KVLOG(signatures_.size()));
  }
  return static_cast<int>(signatures_.size());
}
void EdDSASignatureAccumulator::setExpectedDigest(const unsigned char *msg, int len) {
  LOG_INFO(EDDSA_MULTISIG_LOG, " setExpectedDigest: " << len);
  msgDigest_ = std::string((const char *)msg, (size_t)len);
}

size_t EdDSASignatureAccumulator::getFullSignedData(char *outThreshSig, int threshSigLen) {
  LOG_INFO(EDDSA_MULTISIG_LOG,
           "this: " << (uint64_t)this << ", threshSigLen: " << threshSigLen
                    << " ,signatures_.size():" << signatures_.size());
  ConcordAssertGE((uint64_t)threshSigLen,
                  static_cast<unsigned long>(signatures_.size()) * sizeof(SingleEdDSASignature));
  size_t offset = 0;
  for (auto &[id, sig] : signatures_) {
    UNUSED(id);
    std::memcpy(outThreshSig + offset, &sig, sizeof(SingleEdDSASignature));
    offset += sizeof(SingleEdDSASignature);
  }
  return offset;
}

bool EdDSASignatureAccumulator::hasShareVerificationEnabled() const { return false; }
int EdDSASignatureAccumulator::getNumValidShares() const { return 0; }
std::set<ShareID> EdDSASignatureAccumulator::getInvalidShareIds() const { return std::set<ShareID>(); }

EdDSASignatureAccumulator::EdDSASignatureAccumulator(const EdDSAMultisigVerifier &verifier) /*:
    verifier_(verifier)*/
{
  LOG_INFO(EDDSA_MULTISIG_LOG, "created new accumulator, addr: " << this);
  UNUSED(verifier);
}

EdDSAMultisigVerifier::EdDSAMultisigVerifier(const std::vector<SSLEdDSAPublicKey> &publicKeys,
                                             const size_t signersCount,
                                             const size_t threshold)
    : publicKeys_(), signersCount_(signersCount + 1), threshold_(threshold) {
  if (signersCount_ == publicKeys.size()) {
    publicKeys_ = publicKeys;
  }
  LOG_INFO(EDDSA_MULTISIG_LOG,
           "new verifier address: " << (uint64_t)this << "publicKeys_.size(): " << publicKeys_.size());
}

IThresholdAccumulator *EdDSAMultisigVerifier::newAccumulator(bool withShareVerification) const {
  UNUSED(withShareVerification);
  return new EdDSASignatureAccumulator(*this);
}

bool EdDSAMultisigVerifier::verify(const char *msg, int msgLen, const char *sig, int sigLen) const {
  LOG_INFO(EDDSA_MULTISIG_LOG, KVLOG(this, publicKeys_.size(), signersCount_, threshold_, sigLen));
  if (sigLen == 0) {
    return false;
  }
  auto msgLenUnsigned = static_cast<size_t>(msgLen);
  ConcordAssert(static_cast<unsigned long>(sigLen) % sizeof(SingleEdDSASignature) == 0);

  size_t validSignatureCount = 0;
  for (int i = 0; i < (int)publicKeys_.size(); i++) {
    auto currentSignature = reinterpret_cast<const SingleEdDSASignature *>(
        &sig[(static_cast<unsigned long>(i)) * sizeof(SingleEdDSASignature)]);
    if (currentSignature->id == 0) {
      continue;
    }
    auto result = publicKeys_[(size_t)currentSignature->id].verify(reinterpret_cast<const uint8_t *>(msg),
                                                                   msgLenUnsigned,
                                                                   currentSignature->signatureBytes.data(),
                                                                   currentSignature->signatureBytes.size());
    LOG_INFO(EDDSA_MULTISIG_LOG, "Verified id: " << KVLOG(currentSignature->id, result));
    validSignatureCount++;
  }

  bool result = validSignatureCount == threshold_;
  LOG_INFO(EDDSA_MULTISIG_LOG, KVLOG(validSignatureCount, threshold_));
  return result;
}

int EdDSAMultisigVerifier::requiredLengthForSignedData() const {
  return static_cast<int>(static_cast<unsigned long>(signersCount_) * sizeof(SingleEdDSASignature));
}
const IPublicKey &EdDSAMultisigVerifier::getPublicKey() const { return publicKeys_[0]; }
const IShareVerificationKey &EdDSAMultisigVerifier::getShareVerificationKey(ShareID signer) const {
  return publicKeys_[static_cast<size_t>(signer)];
}
