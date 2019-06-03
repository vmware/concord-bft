//Concord
//
//Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
//This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "ReplicaConfig.hpp"
#include "Serializable.h"

namespace bftEngine {
// As ReplicaConfig is an interface struct, we need this additional wrapper
// class for its serialization/deserialization functionality.
class ReplicaConfigSerializer : public Serializable {
 public:
  explicit ReplicaConfigSerializer(const ReplicaConfig &config);
  ~ReplicaConfigSerializer() override;

  ReplicaConfig *getConfig() const { return config_; }
  void setConfig(const ReplicaConfig& config);

  bool operator==(const ReplicaConfigSerializer &other) const;

  // Serialization/deserialization
  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization.
  ReplicaConfigSerializer() { config_ = new ReplicaConfig; };

  static uint32_t maxSize(uint32_t numOfReplicas) {
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

 protected:
  void serializeDataMembers(std::ostream &outStream) const override;
  std::string getName() const override { return className_; };
  std::string getVersion() const override { return classVersion_; };

 private:
  void serializeKey(const std::string &key, std::ostream &outStream) const;
  std::string deserializeKey(std::istream &inStream) const;
  void createSignersAndVerifiers(std::istream &inStream);

  static void registerClass();

 private:
  ReplicaConfig *config_ = nullptr;

  const std::string className_ = "ReplicaConfig";
  const std::string classVersion_ = "1";
  static bool registered_;
};

}
