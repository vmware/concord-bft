// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
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
    : _myId{myId} {
  // Assert(replicasSigPublicKeys.size() == numberOfReplicasAndClients);
  // TODO(GG): change - here we don't care about client signatures

  _mySigner = new RSASigner(mySigPrivateKey.c_str());

  for (const PublicKeyDesc& p : replicasSigPublicKeys) {
    Assert(_replicasVerifiers.count(p.first) == 0);

    RSAVerifier* verifier = new RSAVerifier(p.second.c_str());
    _replicasVerifiers[p.first] = verifier;

    Assert(p.first != myId ||
           _mySigner->signatureLength() == verifier->signatureLength());
  }
}

SigManager::~SigManager() {
  delete _mySigner;
  for (std::pair<ReplicaId, RSAVerifier*> v : _replicasVerifiers)
    delete v.second;
}

uint16_t SigManager::getSigLength(ReplicaId replicaId) const {
  if (replicaId == _myId) {
    return (uint16_t)_mySigner->signatureLength();
  } else {
    auto pos = _replicasVerifiers.find(replicaId);
    Assert(pos != _replicasVerifiers.end());

    RSAVerifier* verifier = pos->second;

    return (uint16_t)verifier->signatureLength();
  }
}

bool SigManager::verifySig(ReplicaId replicaId,
                           const char* data,
                           size_t dataLength,
                           const char* sig,
                           uint16_t sigLength) const {
  auto pos = _replicasVerifiers.find(replicaId);
  Assert(pos != _replicasVerifiers.end());

  RSAVerifier* verifier = pos->second;

  bool res = verifier->verify(data, dataLength, sig, sigLength);

  return res;
}

void SigManager::sign(const char* data,
                      size_t dataLength,
                      char* outSig,
                      uint16_t outSigLength) const {
  size_t actualSigSize = 0;
  _mySigner->sign(data, dataLength, outSig, outSigLength, actualSigSize);
  Assert(outSigLength == actualSigSize);
}

uint16_t SigManager::getMySigLength() const {
  return (uint16_t)_mySigner->signatureLength();
}

}  // namespace impl
}  // namespace bftEngine
