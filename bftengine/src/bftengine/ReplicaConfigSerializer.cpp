// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of sub-components with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ReplicaConfigSerializer.hpp"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "SysConsts.hpp"
#include "KeyManager.h"

using namespace std;
using namespace concord::serialize;

namespace bftEngine {
namespace impl {

uint32_t ReplicaConfigSerializer::maxSize(uint32_t numOfReplicas) {
  return (sizeof(config_->fVal) + sizeof(config_->cVal) + sizeof(config_->replicaId) +
          sizeof(config_->numOfClientProxies) + sizeof(config_->numOfExternalClients) +
          sizeof(config_->statusReportTimerMillisec) + sizeof(config_->concurrencyLevel) +
          sizeof(config_->viewChangeProtocolEnabled) + sizeof(config_->viewChangeTimerMillisec) +
          sizeof(config_->autoPrimaryRotationEnabled) + sizeof(config_->autoPrimaryRotationTimerMillisec) +
          sizeof(config_->preExecutionFeatureEnabled) + sizeof(config_->preExecReqStatusCheckTimerMillisec) +
          sizeof(config_->preExecConcurrencyLevel) + MaxSizeOfPrivateKey + numOfReplicas * MaxSizeOfPublicKey +
          IThresholdSigner::maxSize() * 3 + IThresholdVerifier::maxSize() * 3 + sizeof(config_->batchingPolicy) +
          sizeof(config_->maxInitialBatchSize) + sizeof(config_->batchingFactorCoefficient) +
          sizeof(config_->maxExternalMessageSize) + sizeof(config_->maxReplyMessageSize) +
          sizeof(config_->maxNumOfReservedPages) + sizeof(config_->sizeOfReservedPage) +
          sizeof(config_->debugPersistentStorageEnabled) + sizeof(config_->metricsDumpIntervalSeconds) +
          sizeof(config_->keyExchangeOnStart) + KeyManager::FileSecureStore::maxSize());
}

ReplicaConfigSerializer::ReplicaConfigSerializer(ReplicaConfig *config) {
  config_.reset(new ReplicaConfig);
  if (config) *config_ = *config;
}

ReplicaConfigSerializer::ReplicaConfigSerializer() { config_.reset(new ReplicaConfig); }

void ReplicaConfigSerializer::setConfig(const ReplicaConfig &config) {
  if (config_) {
    config_.reset(new ReplicaConfig);
  }
  *config_ = config;
}

/************** Serialization **************/

void ReplicaConfigSerializer::serializeDataMembers(ostream &outStream) const {
  // Serialize isReadOnly
  outStream.write((char *)&config_->isReadOnly, sizeof(config_->isReadOnly));

  // Serialize numReplicas
  outStream.write((char *)&config_->numReplicas, sizeof(config_->numReplicas));

  // Serialize numRoReplicas
  outStream.write((char *)&config_->numRoReplicas, sizeof(config_->numRoReplicas));

  // Serialize fVal
  outStream.write((char *)&config_->fVal, sizeof(config_->fVal));

  // Serialize cVal
  outStream.write((char *)&config_->cVal, sizeof(config_->cVal));

  // Serialize replicaId
  outStream.write((char *)&config_->replicaId, sizeof(config_->replicaId));

  // Serialize numOfClientProxies
  outStream.write((char *)&config_->numOfClientProxies, sizeof(config_->numOfClientProxies));

  // Serialize numOfExternalClients
  outStream.write((char *)&config_->numOfExternalClients, sizeof(config_->numOfExternalClients));

  // Serialize statusReportTimerMillisec
  outStream.write((char *)&config_->statusReportTimerMillisec, sizeof(config_->statusReportTimerMillisec));

  // Serialize concurrencyLevel
  outStream.write((char *)&config_->concurrencyLevel, sizeof(config_->concurrencyLevel));

  // Serialize viewChangeProtocolEnabled
  outStream.write((char *)&config_->viewChangeProtocolEnabled, sizeof(config_->viewChangeProtocolEnabled));

  // Serialize viewChangeTimerMillisec
  outStream.write((char *)&config_->viewChangeTimerMillisec, sizeof(config_->viewChangeTimerMillisec));

  // Serialize autoPrimaryRotationEnabled
  outStream.write((char *)&config_->autoPrimaryRotationEnabled, sizeof(config_->autoPrimaryRotationEnabled));

  // Serialize autoPrimaryRotationTimerMillisec
  outStream.write((char *)&config_->autoPrimaryRotationTimerMillisec,
                  sizeof(config_->autoPrimaryRotationTimerMillisec));

  // Serialize batchingPolicy
  outStream.write((char *)&config_->batchingPolicy, sizeof(config_->batchingPolicy));

  // Serialize maxInitialBatchSize
  outStream.write((char *)&config_->maxInitialBatchSize, sizeof(config_->maxInitialBatchSize));

  // Serialize batchingFactorCoefficient
  outStream.write((char *)&config_->batchingFactorCoefficient, sizeof(config_->batchingFactorCoefficient));

  // Serialize preExecutionFeatureEnabled
  outStream.write((char *)&config_->preExecutionFeatureEnabled, sizeof(config_->preExecutionFeatureEnabled));

  // Serialize preExecReqStatusCheckTimerMillisec
  outStream.write((char *)&config_->preExecReqStatusCheckTimerMillisec,
                  sizeof(config_->preExecReqStatusCheckTimerMillisec));

  // Serialize preExecConcurrencyLevel
  outStream.write((char *)&config_->preExecConcurrencyLevel, sizeof(config_->preExecConcurrencyLevel));

  // Serialize public keys
  auto numOfPublicKeys = (int64_t)config_->publicKeysOfReplicas.size();
  outStream.write((char *)&numOfPublicKeys, sizeof(numOfPublicKeys));
  for (auto elem : config_->publicKeysOfReplicas) {
    outStream.write((char *)&elem.first, sizeof(elem.first));
    serializeKey(elem.second, outStream);
  }

  // Serialize replicaPrivateKey
  serializeKey(config_->replicaPrivateKey, outStream);

  // Serialize threshold crypto system
  concord::serialize::Serializable::serialize(outStream, config_->thresholdSystemType_);
  concord::serialize::Serializable::serialize(outStream, config_->thresholdSystemSubType_);
  serializeKey(config_->thresholdPrivateKey_, outStream);  // TODO [TK] serialize to a secret key store
  serializeKey(config_->thresholdPublicKey_, outStream);
  concord::serialize::Serializable::serialize(outStream, config_->thresholdVerificationKeys_);

  outStream.write((char *)&config_->maxExternalMessageSize, sizeof(config_->maxExternalMessageSize));
  outStream.write((char *)&config_->maxReplyMessageSize, sizeof(config_->maxReplyMessageSize));
  outStream.write((char *)&config_->maxNumOfReservedPages, sizeof(config_->maxNumOfReservedPages));
  outStream.write((char *)&config_->sizeOfReservedPage, sizeof(config_->sizeOfReservedPage));

  // Serialize debugPersistentStorageEnabled
  outStream.write((char *)&config_->debugPersistentStorageEnabled, sizeof(config_->debugPersistentStorageEnabled));

  // Serialize metricsDumpIntervalSeconds
  outStream.write((char *)&config_->metricsDumpIntervalSeconds, sizeof(config_->metricsDumpIntervalSeconds));

  outStream.write((char *)&config_->keyExchangeOnStart, sizeof(config_->keyExchangeOnStart));
  outStream.write(config_->keyViewFilePath.data(), sizeof(config_->keyViewFilePath.size()));
}

void ReplicaConfigSerializer::serializePointer(Serializable *ptrToClass, ostream &outStream) const {
  uint8_t ptrToClassSpecified = ptrToClass ? 1 : 0;
  serialize(outStream, ptrToClassSpecified);
  if (ptrToClass) ptrToClass->serialize(outStream);
}

void ReplicaConfigSerializer::serializeKey(const string &key, ostream &outStream) const {
  concord::serialize::Serializable::serialize(outStream, key);
}

bool ReplicaConfigSerializer::operator==(const ReplicaConfigSerializer &other) const {
  bool result =
      ((other.config_->isReadOnly == config_->isReadOnly) && (other.config_->numReplicas == config_->numReplicas) &&
       (other.config_->numRoReplicas == config_->numRoReplicas) && (other.config_->fVal == config_->fVal) &&
       (other.config_->cVal == config_->cVal) && (other.config_->replicaId == config_->replicaId) &&
       (other.config_->numOfClientProxies == config_->numOfClientProxies) &&
       (other.config_->numOfExternalClients == config_->numOfExternalClients) &&
       (other.config_->statusReportTimerMillisec == config_->statusReportTimerMillisec) &&
       (other.config_->concurrencyLevel == config_->concurrencyLevel) &&
       (other.config_->viewChangeProtocolEnabled == config_->viewChangeProtocolEnabled) &&
       (other.config_->viewChangeTimerMillisec == config_->viewChangeTimerMillisec) &&
       (other.config_->autoPrimaryRotationEnabled == config_->autoPrimaryRotationEnabled) &&
       (other.config_->autoPrimaryRotationTimerMillisec == config_->autoPrimaryRotationTimerMillisec) &&
       (other.config_->preExecutionFeatureEnabled == config_->preExecutionFeatureEnabled) &&
       (other.config_->preExecReqStatusCheckTimerMillisec == config_->preExecReqStatusCheckTimerMillisec) &&
       (other.config_->preExecConcurrencyLevel == config_->preExecConcurrencyLevel) &&
       (other.config_->replicaPrivateKey == config_->replicaPrivateKey) &&
       (other.config_->publicKeysOfReplicas == config_->publicKeysOfReplicas) &&
       (other.config_->debugPersistentStorageEnabled == config_->debugPersistentStorageEnabled) &&
       (other.config_->maxExternalMessageSize == config_->maxExternalMessageSize) &&
       (other.config_->maxReplyMessageSize == config_->maxReplyMessageSize) &&
       (other.config_->maxNumOfReservedPages == config_->maxNumOfReservedPages) &&
       (other.config_->sizeOfReservedPage == config_->sizeOfReservedPage) &&
       (other.config_->metricsDumpIntervalSeconds == config_->metricsDumpIntervalSeconds) &&
       (other.config_->keyExchangeOnStart == config_->keyExchangeOnStart) &&
       (other.config_->keyViewFilePath == config_->keyViewFilePath));
  return result;
}

/************** Deserialization **************/
void ReplicaConfigSerializer::deserializeDataMembers(istream &inStream) {
  ReplicaConfig &config = *config_.get();

  // Deserialize isReadOnly
  inStream.read((char *)&config.isReadOnly, sizeof(config.isReadOnly));

  // Deserialize numReplicas
  inStream.read((char *)&config.numReplicas, sizeof(config.numReplicas));

  // Deserialize numRoReplicas
  inStream.read((char *)&config.numRoReplicas, sizeof(config.numRoReplicas));

  // Deserialize fVal
  inStream.read((char *)&config.fVal, sizeof(config.fVal));

  // Deserialize cVal
  inStream.read((char *)&config.cVal, sizeof(config.cVal));

  // Deserialize replicaId
  inStream.read((char *)&config.replicaId, sizeof(config.replicaId));

  // Deserialize numOfClientProxies
  inStream.read((char *)&config.numOfClientProxies, sizeof(config.numOfClientProxies));

  // Deserialize numOfExternalClients
  inStream.read((char *)&config.numOfExternalClients, sizeof(config.numOfExternalClients));

  // Deserialize statusReportTimerMillisec
  inStream.read((char *)&config.statusReportTimerMillisec, sizeof(config.statusReportTimerMillisec));

  // Deserialize concurrencyLevel
  inStream.read((char *)&config.concurrencyLevel, sizeof(config.concurrencyLevel));

  // Deserialize viewChangeProtocolEnabled
  inStream.read((char *)&config.viewChangeProtocolEnabled, sizeof(config.viewChangeProtocolEnabled));

  // Deserialize viewChangeTimerMillisec
  inStream.read((char *)&config.viewChangeTimerMillisec, sizeof(config.viewChangeTimerMillisec));

  // Deserialize autoPrimaryRotationEnabled
  inStream.read((char *)&config.autoPrimaryRotationEnabled, sizeof(config.autoPrimaryRotationEnabled));

  // Deserialize autoPrimaryRotationTimerMillisec
  inStream.read((char *)&config.autoPrimaryRotationTimerMillisec, sizeof(config.autoPrimaryRotationTimerMillisec));

  // Deserialize batchingPolicy
  inStream.read((char *)&config.batchingPolicy, sizeof(config.batchingPolicy));

  // Deserialize maxInitialBatchSize
  inStream.read((char *)&config.maxInitialBatchSize, sizeof(config.maxInitialBatchSize));

  // Deserialize batchingFactorCoefficient
  inStream.read((char *)&config.batchingFactorCoefficient, sizeof(config.batchingFactorCoefficient));

  // Deserialize preExecutionFeatureEnabled
  inStream.read((char *)&config.preExecutionFeatureEnabled, sizeof(config.preExecutionFeatureEnabled));

  // Deserialize preExecReqStatusCheckTimerMillisec
  inStream.read((char *)&config.preExecReqStatusCheckTimerMillisec, sizeof(config.preExecReqStatusCheckTimerMillisec));

  // Deserialize preExecConcurrencyLevel
  inStream.read((char *)&config.preExecConcurrencyLevel, sizeof(config.preExecConcurrencyLevel));

  // Deserialize public keys
  int64_t numOfPublicKeys = 0;
  inStream.read((char *)&numOfPublicKeys, sizeof(numOfPublicKeys));
  for (int i = 0; i < numOfPublicKeys; ++i) {
    uint16_t id = 0;
    inStream.read((char *)&id, sizeof(id));
    string key = deserializeKey(inStream);
    config.publicKeysOfReplicas.insert(pair<uint16_t, string>(id, key));
  }

  // Deserialize replicaPrivateKey
  config.replicaPrivateKey = deserializeKey(inStream);

  // Deserialize threshold crypto system
  concord::serialize::Serializable::deserialize(inStream, config_->thresholdSystemType_);
  concord::serialize::Serializable::deserialize(inStream, config_->thresholdSystemSubType_);
  config_->thresholdPrivateKey_ = deserializeKey(inStream);  // TODO [TK] deserialize from a secret key store
  config_->thresholdPublicKey_ = deserializeKey(inStream);
  concord::serialize::Serializable::deserialize(inStream, config_->thresholdVerificationKeys_);

  inStream.read((char *)&config.maxExternalMessageSize, sizeof(config.maxExternalMessageSize));
  inStream.read((char *)&config.maxReplyMessageSize, sizeof(config.maxReplyMessageSize));
  inStream.read((char *)&config.maxNumOfReservedPages, sizeof(config.maxNumOfReservedPages));
  inStream.read((char *)&config.sizeOfReservedPage, sizeof(config.sizeOfReservedPage));

  inStream.read((char *)&config.debugPersistentStorageEnabled, sizeof(config.debugPersistentStorageEnabled));

  // Deserialize metricsDumpIntervalSeconds
  inStream.read((char *)&config.metricsDumpIntervalSeconds, sizeof(config.metricsDumpIntervalSeconds));

  inStream.read((char *)&config.keyExchangeOnStart, sizeof(config.keyExchangeOnStart));
  inStream.read(config.keyViewFilePath.data(), config.keyViewFilePath.size());
}

SerializablePtr ReplicaConfigSerializer::deserializePointer(std::istream &inStream) {
  uint8_t ptrToClassSpecified;
  deserialize(inStream, ptrToClassSpecified);
  if (ptrToClassSpecified) {
    Serializable *s = nullptr;
    deserialize(inStream, s);
    return SerializablePtr(s);
  }
  return SerializablePtr();
}

string ReplicaConfigSerializer::deserializeKey(istream &inStream) const {
  std::string result;
  concord::serialize::Serializable::deserialize(inStream, result);
  return result;
}

}  // namespace impl
}  // namespace bftEngine
