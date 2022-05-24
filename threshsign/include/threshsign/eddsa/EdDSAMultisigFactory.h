//
// Created by yflum on 26/04/2022.
//
#pragma once

#include "../IThresholdFactory.h"
#include "../IThresholdSigner.h"
#include "../IThresholdVerifier.h"

class EdDSAMultisigFactory : public IThresholdFactory {
 public:
  IThresholdVerifier *newVerifier(ShareID reqSigners,
                                  ShareID totalSigners,
                                  const char *publicKeyStr,
                                  const std::vector<std::string> &verifKeysHex) const override;
  IThresholdSigner *newSigner(ShareID id, const char *secretKeyStr) const override;
  std::tuple<std::vector<IThresholdSigner *>, IThresholdVerifier *> newRandomSigners(
      NumSharesType reqSigners, NumSharesType numSigners) const override;
  std::pair<std::unique_ptr<IShareSecretKey>, std::unique_ptr<IShareVerificationKey>> newKeyPair() const override;
  ~EdDSAMultisigFactory() override = default;
};
