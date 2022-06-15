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
#include "threshsign/eddsa/EdDSAThreshsignKeys.h"
#include "threshsign/eddsa/EdDSAMultisigSigner.h"
#include "threshsign/eddsa/SingleEdDSASignature.h"

EdDSAMultisigSigner::EdDSAMultisigSigner(const EdDSAThreshsignPrivateKey &privateKey, const uint32_t id)
    : EdDSASigner<EdDSAThreshsignPrivateKey>{privateKey}, publicKey_{}, id_{id} {}

int EdDSAMultisigSigner::requiredLengthForSignedData() const { return sizeof(SingleEdDSASignature); }

void EdDSAMultisigSigner::signData(const char *hash, int hashLen, char *outSig, int outSigLen) {
  ConcordAssertGE(outSigLen, requiredLengthForSignedData());
  LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(id_));
  SingleEdDSASignature result;
  auto outSigBytesLen = result.signatureBytes.size();
  sign(reinterpret_cast<const uint8_t *>(hash),
       static_cast<size_t>(hashLen),
       result.signatureBytes.data(),
       outSigBytesLen);
  ConcordAssertEQ(outSigBytesLen, result.signatureBytes.size());
  result.id = id_;
  std::memcpy(outSig, &result, sizeof(SingleEdDSASignature));
}
const IShareSecretKey &EdDSAMultisigSigner::getShareSecretKey() const { return getPrivKey(); }
const IShareVerificationKey &EdDSAMultisigSigner::getShareVerificationKey() const { return publicKey_; }