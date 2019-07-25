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
using namespace concordSerializable;

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
      IThresholdVerifier::maxSize() * 3);
}

void ReplicaConfigSerializer::registerClass() {
  SerializableObjectsDB::registerObject("ReplicaConfig", SharedPtrToClass(new ReplicaConfigSerializer));
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

  config_->thresholdSignerForExecution->serialize(outStream);
  config_->thresholdVerifierForExecution->serialize(outStream);

  config_->thresholdSignerForSlowPathCommit->serialize(outStream);
  config_->thresholdVerifierForSlowPathCommit->serialize(outStream);

  config_->thresholdSignerForCommit->serialize(outStream);
  config_->thresholdVerifierForCommit->serialize(outStream);

  config_->thresholdSignerForOptimisticCommit->serialize(outStream);
  config_->thresholdVerifierForOptimisticCommit->serialize(outStream);
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
      (other.config_->publicKeysOfReplicas == config_->publicKeysOfReplicas));
  return result;
}

/************** Deserialization **************/

SharedPtrToClass ReplicaConfigSerializer::create(istream &inStream) {
  SharedPtrToClass replicaConfigSerializer(new ReplicaConfigSerializer());
  ReplicaConfig &config = *((ReplicaConfigSerializer *) replicaConfigSerializer.get())->config_;

  // Deserialize class version
  verifyClassVersion(((ReplicaConfigSerializer *) replicaConfigSerializer.get())->classVersion_, inStream);

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
  return replicaConfigSerializer;
}

void ReplicaConfigSerializer::createSignersAndVerifiers(istream &inStream, ReplicaConfig &newObject) {
  thresholdSignerForExecution_ = deserialize(inStream);
  thresholdVerifierForExecution_ = deserialize(inStream);
  thresholdSignerForSlowPathCommit_ = deserialize(inStream);
  thresholdVerifierForSlowPathCommit_ = deserialize(inStream);
  thresholdSignerForCommit_ = deserialize(inStream);
  thresholdVerifierForCommit_ = deserialize(inStream);
  thresholdSignerForOptimisticCommit_ = deserialize(inStream);
  thresholdVerifierForOptimisticCommit_ = deserialize(inStream);

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
