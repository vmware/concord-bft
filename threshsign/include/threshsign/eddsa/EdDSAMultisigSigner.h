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
