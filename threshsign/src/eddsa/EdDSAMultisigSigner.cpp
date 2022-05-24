//
// Created by yflum on 26/04/2022.
//

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