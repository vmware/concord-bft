// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "SigManager.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

SigManager::SigManager(ReplicaId myId,
                       int16_t numberOfReplicasAndClients,
                       PrivateKeyDesc mySigPrivateKey,
                       std::set<PublicKeyDesc> replicasSigPublicKeys)
    : myId_{myId} {
  // Assert(replicasSigPublicKeys.size() == numberOfReplicasAndClients); TODO(GG): change - here we don't care about
  // client signatures

  mySigner_ = new RSASigner(mySigPrivateKey.c_str());

  for (const PublicKeyDesc& p : replicasSigPublicKeys) {
    Assert(replicasVerifiers_.count(p.first) == 0);

    RSAVerifier* verifier = new RSAVerifier(p.second.c_str());
    replicasVerifiers_[p.first] = verifier;

    Assert(p.first != myId || mySigner_->signatureLength() == verifier->signatureLength());
  }
}

SigManager::~SigManager() {
  delete mySigner_;
  for (std::pair<ReplicaId, RSAVerifier*> v : replicasVerifiers_) delete v.second;
}

uint16_t SigManager::getSigLength(ReplicaId replicaId) const {
  if (replicaId == myId_) {
    return (uint16_t)mySigner_->signatureLength();
  } else {
    auto pos = replicasVerifiers_.find(replicaId);
    Assert(pos != replicasVerifiers_.end());

    RSAVerifier* verifier = pos->second;

    return (uint16_t)verifier->signatureLength();
  }
}

bool SigManager::verifySig(
    ReplicaId replicaId, const char* data, size_t dataLength, const char* sig, uint16_t sigLength) const {
  auto pos = replicasVerifiers_.find(replicaId);
  Assert(pos != replicasVerifiers_.end());

  RSAVerifier* verifier = pos->second;

  bool res = verifier->verify(data, dataLength, sig, sigLength);

  return res;
}

void SigManager::sign(const char* data, size_t dataLength, char* outSig, uint16_t outSigLength) const {
  size_t actualSigSize = 0;
  mySigner_->sign(data, dataLength, outSig, outSigLength, actualSigSize);
  Assert(outSigLength == actualSigSize);
}

uint16_t SigManager::getMySigLength() const { return (uint16_t)mySigner_->signatureLength(); }

}  // namespace impl
}  // namespace bftEngine
