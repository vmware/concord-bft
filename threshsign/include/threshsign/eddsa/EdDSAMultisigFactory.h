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

#include "threshsign/IThresholdFactory.h"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "EdDSAThreshsignKeys.h"

class EdDSAMultisigFactory : public IThresholdFactory {
 public:
  EdDSAMultisigFactory();
  IThresholdVerifier *newVerifier(ShareID reqSigners,
                                  ShareID totalSigners,
                                  const char *publicKeyStr,
                                  const std::vector<std::string> &verifKeysHex) const override;
  IThresholdSigner *newSigner(ShareID id, const char *secretKeyStr) const override;
  IThresholdFactory::SignersVerifierTuple newRandomSigners(NumSharesType reqSigners,
                                                           NumSharesType numSigners) const override;
  std::pair<std::unique_ptr<IShareSecretKey>, std::unique_ptr<IShareVerificationKey>> newKeyPair() const override;
  ~EdDSAMultisigFactory() override = default;
};
