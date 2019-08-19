//Concord
//
//Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
//This product may include a number of sub-components with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ReplicaConfigSerializer.hpp"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "SysConsts.hpp"

using namespace std;
using namespace serialize;

namespace bftEngine {
namespace impl {

uint32_t ReplicaConfigSerializer::maxSize(uint32_t numOfReplicas) {
  return (sizeof(config_->fVal) + sizeof(config_->cVal) +
      sizeof(config_->replicaId) +
      sizeof(config_->numOfClientProxies) +
      sizeof(config_->statusReportTimerMillisec) +
      sizeof(config_->concurrencyLevel) +
      sizeof(config_->autoViewChangeEnabled) +
      sizeof(config_->viewChangeTimerMillisec) + MaxSizeOfPrivateKey +
      numOfReplicas * MaxSizeOfPublicKey +
      IThresholdSigner::maxSize() * 3 +
      IThresholdVerifier::maxSize() * 3 +
      sizeof(config_->maxExternalMessageSize) +
      sizeof(config_->maxReplyMessageSize) +
      sizeof(config_->maxNumOfReservedPages) +
      sizeof(config_->sizeOfReservedPage) +
      sizeof(config_->debugPersistentStorageEnabled));
}

void ReplicaConfigSerializer::registerClass() {
  registerObject("ReplicaConfig", SerializablePtr(new ReplicaConfigSerializer));
}

ReplicaConfigSerializer::ReplicaConfigSerializer(ReplicaConfig *config) {
  config_.reset(new ReplicaConfig);
  if (config)
    *config_ = *config;
  registerClass();
}

ReplicaConfigSerializer::ReplicaConfigSerializer() {
  config_.reset(new ReplicaConfig);
}

void ReplicaConfigSerializer::setConfig(const ReplicaConfig &config) {
  if (config_) {
    config_.reset(new ReplicaConfig);
  }
  *config_ = config;
}

/************** Serialization **************/

void ReplicaConfigSerializer::serializeDataMembers(ostream &outStream) const {
  // Serialize fVal
  outStream.write((char *) &config_->fVal, sizeof(config_->fVal));

  // Serialize cVal
  outStream.write((char *) &config_->cVal, sizeof(config_->cVal));

  // Serialize replicaId
  outStream.write((char *) &config_->replicaId, sizeof(config_->replicaId));

  // Serialize numOfClientProxies
  outStream.write((char *) &config_->numOfClientProxies, sizeof(config_->numOfClientProxies));

  // Serialize statusReportTimerMillisec
  outStream.write((char *) &config_->statusReportTimerMillisec, sizeof(config_->statusReportTimerMillisec));

  // Serialize concurrencyLevel
  outStream.write((char *) &config_->concurrencyLevel, sizeof(config_->concurrencyLevel));

  // Serialize autoViewChangeEnabled
  outStream.write((char *) &config_->autoViewChangeEnabled, sizeof(config_->autoViewChangeEnabled));

  // Serialize viewChangeTimerMillisec
  outStream.write((char *) &config_->viewChangeTimerMillisec, sizeof(config_->viewChangeTimerMillisec));

  // Serialize public keys
  auto numOfPublicKeys = (int64_t) config_->publicKeysOfReplicas.size();
  outStream.write((char *) &numOfPublicKeys, sizeof(numOfPublicKeys));
  for (auto elem : config_->publicKeysOfReplicas) {
    outStream.write((char *) &elem.first, sizeof(elem.first));
    serializeKey(elem.second, outStream);
  }

  // Serialize replicaPrivateKey
  serializeKey(config_->replicaPrivateKey, outStream);

  serializePointer(config_->thresholdSignerForExecution, outStream);
  serializePointer(config_->thresholdVerifierForExecution, outStream);

  serializePointer(config_->thresholdSignerForSlowPathCommit, outStream);
  serializePointer(config_->thresholdVerifierForSlowPathCommit, outStream);

  serializePointer(config_->thresholdSignerForCommit, outStream);
  serializePointer(config_->thresholdVerifierForCommit, outStream);

  serializePointer(config_->thresholdSignerForOptimisticCommit, outStream);
  serializePointer(config_->thresholdVerifierForOptimisticCommit, outStream);

  outStream.write((char *) &config_->maxExternalMessageSize, sizeof(config_->maxExternalMessageSize));
  outStream.write((char *) &config_->maxReplyMessageSize, sizeof(config_->maxReplyMessageSize));
  outStream.write((char *) &config_->maxNumOfReservedPages, sizeof(config_->maxNumOfReservedPages));
  outStream.write((char *) &config_->sizeOfReservedPage, sizeof(config_->sizeOfReservedPage));

  // Serialize debugPersistentStorageEnabled
  outStream.write((char *) &config_->debugPersistentStorageEnabled, sizeof(config_->debugPersistentStorageEnabled));
}

void ReplicaConfigSerializer::serializePointer(Serializable *ptrToClass, ostream &outStream) const {
  uint8_t ptrToClassSpecified = ptrToClass ? 1 : 0;
  outStream.write((char *) &ptrToClassSpecified, sizeof(ptrToClassSpecified));
  if (ptrToClass)
    ptrToClass->serialize(outStream);
}

void ReplicaConfigSerializer::serializeKey(const string &key, ostream &outStream) const {
  auto keyLength = (int64_t) key.size();
  // Save a length of the string to the buffer to be able to deserialize it.
  outStream.write((char *) &keyLength, sizeof(keyLength));
  outStream.write(key.c_str(), keyLength);
}

bool ReplicaConfigSerializer::operator==(const ReplicaConfigSerializer &other)
const {
  bool result = ((other.config_->fVal == config_->fVal) &&
      (other.config_->cVal == config_->cVal) &&
      (other.config_->replicaId == config_->replicaId) &&
      (other.config_->numOfClientProxies == config_->numOfClientProxies) &&
      (other.config_->statusReportTimerMillisec == config_->statusReportTimerMillisec) &&
      (other.config_->concurrencyLevel == config_->concurrencyLevel) &&
      (other.config_->autoViewChangeEnabled == config_->autoViewChangeEnabled) &&
      (other.config_->viewChangeTimerMillisec == config_->viewChangeTimerMillisec) &&
      (other.config_->replicaPrivateKey == config_->replicaPrivateKey) &&
      (other.config_->publicKeysOfReplicas == config_->publicKeysOfReplicas) &&
      (other.config_->debugPersistentStorageEnabled == config_->debugPersistentStorageEnabled) &&
      (other.config_->maxExternalMessageSize == config_->maxExternalMessageSize) &&
      (other.config_->maxReplyMessageSize == config_->maxReplyMessageSize) &&
      (other.config_->maxNumOfReservedPages == config_->maxNumOfReservedPages) &&
      (other.config_->sizeOfReservedPage == config_->sizeOfReservedPage));
  return result;
}

/************** Deserialization **************/

SerializablePtr ReplicaConfigSerializer::create(istream &inStream) {
  // Deserialize class version
  verifyClassVersion(classVersion_, inStream);
  return SerializablePtr(new ReplicaConfigSerializer);

}

void ReplicaConfigSerializer::deserializeDataMembers(istream &inStream) {
  ReplicaConfig &config = *config_.get();

  // Deserialize fVal
  inStream.read((char *) &config.fVal, sizeof(config.fVal));

  // Deserialize cVal
  inStream.read((char *) &config.cVal, sizeof(config.cVal));

  // Deserialize replicaId
  inStream.read((char *) &config.replicaId, sizeof(config.replicaId));

  // Deserialize numOfClientProxies
  inStream.read((char *) &config.numOfClientProxies, sizeof(config.numOfClientProxies));

  // Deserialize statusReportTimerMillisec
  inStream.read((char *) &config.statusReportTimerMillisec, sizeof(config.statusReportTimerMillisec));

  // Deserialize concurrencyLevel
  inStream.read((char *) &config.concurrencyLevel, sizeof(config.concurrencyLevel));

  // Deserialize autoViewChangeEnabled
  inStream.read((char *) &config.autoViewChangeEnabled, sizeof(config.autoViewChangeEnabled));

  // Deserialize viewChangeTimerMillisec
  inStream.read((char *) &config.viewChangeTimerMillisec, sizeof(config.viewChangeTimerMillisec));

  // Deserialize public keys
  int64_t numOfPublicKeys = 0;
  inStream.read((char *) &numOfPublicKeys, sizeof(numOfPublicKeys));
  for (int i = 0; i < numOfPublicKeys; ++i) {
    uint16_t id = 0;
    inStream.read((char *) &id, sizeof(id));
    string key = deserializeKey(inStream);
    config.publicKeysOfReplicas.insert(pair<uint16_t, string>(id, key));
  }

  // Serialize replicaPrivateKey
  config.replicaPrivateKey = deserializeKey(inStream);

  createSignersAndVerifiers(inStream, config);

  inStream.read((char *) &config.maxExternalMessageSize, sizeof(config.maxExternalMessageSize));
  inStream.read((char *) &config.maxReplyMessageSize, sizeof(config.maxReplyMessageSize));
  inStream.read((char *) &config.maxNumOfReservedPages, sizeof(config.maxNumOfReservedPages));
  inStream.read((char *) &config.sizeOfReservedPage, sizeof(config.sizeOfReservedPage));

  inStream.read((char *) &config.debugPersistentStorageEnabled, sizeof(config.debugPersistentStorageEnabled));

}

SerializablePtr ReplicaConfigSerializer::deserializePointer(std::istream &inStream) {
  uint8_t ptrToClassSpecified = 0;
  inStream.read((char *) &ptrToClassSpecified, sizeof(ptrToClassSpecified));
  if (ptrToClassSpecified)
    return deserialize(inStream);
  return SerializablePtr();
}

void ReplicaConfigSerializer::createSignersAndVerifiers(istream &inStream, ReplicaConfig &newObject) {
  thresholdSignerForExecution_ = deserializePointer(inStream);
  thresholdVerifierForExecution_ = deserializePointer(inStream);
  thresholdSignerForSlowPathCommit_ = deserializePointer(inStream);
  thresholdVerifierForSlowPathCommit_ = deserializePointer(inStream);
  thresholdSignerForCommit_ = deserializePointer(inStream);
  thresholdVerifierForCommit_ = deserializePointer(inStream);
  thresholdSignerForOptimisticCommit_ = deserializePointer(inStream);
  thresholdVerifierForOptimisticCommit_ = deserializePointer(inStream);

  newObject.thresholdSignerForExecution = dynamic_cast<IThresholdSigner *>(thresholdSignerForExecution_.get());
  newObject.thresholdVerifierForExecution = dynamic_cast<IThresholdVerifier *>(thresholdVerifierForExecution_.get());

  newObject.thresholdSignerForSlowPathCommit =
      dynamic_cast<IThresholdSigner *>(thresholdSignerForSlowPathCommit_.get());
  newObject.thresholdVerifierForSlowPathCommit =
      dynamic_cast<IThresholdVerifier *>(thresholdVerifierForSlowPathCommit_.get());

  newObject.thresholdSignerForCommit = dynamic_cast<IThresholdSigner *>(thresholdSignerForCommit_.get());
  newObject.thresholdVerifierForCommit = dynamic_cast<IThresholdVerifier *>(thresholdVerifierForCommit_.get());

  newObject.thresholdSignerForOptimisticCommit =
      dynamic_cast<IThresholdSigner *>(thresholdSignerForOptimisticCommit_.get());
  newObject.thresholdVerifierForOptimisticCommit =
      dynamic_cast<IThresholdVerifier *>(thresholdVerifierForOptimisticCommit_.get());
}

string ReplicaConfigSerializer::deserializeKey(istream &inStream) const {
  int64_t keyLength = 0;
  inStream.read((char *) &keyLength, sizeof(keyLength));
  UniquePtrToChar key(new char[keyLength]);
  inStream.read(key.get(), keyLength);
  string result(key.get(), keyLength);
  return result;
}

}
}
