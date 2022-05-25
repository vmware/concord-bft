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
#include "threshsign/eddsa/SSLEdDSAPrivateKey.h"
#include "threshsign/eddsa/EdDSAMultisigSigner.h"
#include "threshsign/eddsa/SingleEdDSASignature.h"

EdDSAMultisigSigner::EdDSAMultisigSigner(const SSLEdDSAPrivateKey &privateKey, const uint32_t id)
    : privateKey_{privateKey}, publicKey_{{0}}, id_{id} {}

int EdDSAMultisigSigner::requiredLengthForSignedData() const { return sizeof(SingleEdDSASignature); }
void EdDSAMultisigSigner::signData(const char *hash, int hashLen, char *outSig, int outSigLen) {
  ConcordAssertEQ(outSigLen, requiredLengthForSignedData());
  SingleEdDSASignature result;
  ConcordAssertGE((size_t)outSigLen, result.signatureBytes.size());
  auto outSigBytesLen = (size_t)result.signatureBytes.size();
  privateKey_.sign(
      reinterpret_cast<const uint8_t *>(hash), (size_t)hashLen, result.signatureBytes.data(), outSigBytesLen);
  ConcordAssertEQ(outSigBytesLen, result.signatureBytes.size());
  result.id = id_;
  std::memcpy(outSig, &result, sizeof(SingleEdDSASignature));
}
const IShareSecretKey &EdDSAMultisigSigner::getShareSecretKey() const { return privateKey_; }
const IShareVerificationKey &EdDSAMultisigSigner::getShareVerificationKey() const { return publicKey_; }