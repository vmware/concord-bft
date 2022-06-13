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

#include "threshsign/IThresholdSigner.h"
#include "EdDSAThreshsignKeys.h"
#include "crypto/eddsa/EdDSASigner.hpp"

class EdDSAMultisigSigner : public IThresholdSigner, public EdDSASigner<EdDSAThreshsignPrivateKey> {
 public:
  EdDSAMultisigSigner(const EdDSAThreshsignPrivateKey &privateKey, const uint32_t id);
  int requiredLengthForSignedData() const override;

  void signData(const char *hash, int hashLen, char *outSig, int outSigLen) override;
  const IShareSecretKey &getShareSecretKey() const override;
  const IShareVerificationKey &getShareVerificationKey() const override;
  ~EdDSAMultisigSigner() override = default;

 private:
  class StubVerificationKey : public IShareVerificationKey {
    virtual std::string toString() const { return ""; }
  };

  /// This is a stub so that getShareVerificationKey can have a return value
  const StubVerificationKey publicKey_;
  uint32_t id_;
};
