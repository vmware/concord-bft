//
// Created by yflum on 26/04/2022.
//
#pragma once

#include "../IThresholdSigner.h"
#include "SSLEdDSAPrivateKey.h"
#include "SSLEdDSAPublicKey.h"

class EdDSAMultisigSigner : public IThresholdSigner {
 public:
  EdDSAMultisigSigner(const SSLEdDSAPrivateKey &privateKey, const uint32_t id);
  int requiredLengthForSignedData() const override;

  void signData(const char *hash, int hashLen, char *outSig, int outSigLen) override;
  const IShareSecretKey &getShareSecretKey() const override;
  const IShareVerificationKey &getShareVerificationKey() const override;
  ~EdDSAMultisigSigner() override = default;

 private:
  SSLEdDSAPrivateKey privateKey_;
  SSLEdDSAPublicKey publicKey_;
  uint32_t id_;
};
