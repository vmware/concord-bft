//Concord
//
//Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
//This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "ReplicaConfig.hpp"
#include "threshsign/Serializable.h"

namespace bftEngine {
// As ReplicaConfig is an interface struct, we need this additional wrapper
// class for its serialization/deserialization functionality.
class ReplicaConfigSerializer : public Serializable {
 public:
  explicit ReplicaConfigSerializer(const ReplicaConfig& config) {
    config_ = new ReplicaConfig;
    *config_ = config;
  }
  ~ReplicaConfigSerializer() override { delete config_; }

  ReplicaConfig* getConfig() { return config_; }

  bool operator==(const ReplicaConfigSerializer &other) const;

  // Serialization/deserialization
  // Two functions below should be implemented by all derived classes.
  void serialize(UniquePtrToChar &outBuf, int64_t &outBufSize) const;
  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization.
  ReplicaConfigSerializer() = default;

  static uint32_t maxSize() {
    return (sizeof(config_->fVal) + sizeof(config_->cVal) +
        sizeof(config_->replicaId) +
        sizeof(config_->numOfClientProxies) +
        sizeof(config_->statusReportTimerMillisec) +
        sizeof(config_->concurrencyLevel) +
        sizeof(config_->autoViewChangeEnabled) +
        sizeof(config_->viewChangeTimerMillisec) + MaxSizeOfPrivateKey +
        MaxNumberOfReplicas * MaxSizeOfPublicKey +
        IThresholdSigner::maxSize() * 3 +
        IThresholdVerifier::maxSize() * 3);
  }

 private:
  void serializeDataMembers(std::ostream &outStream) const;
  void serializeKey(const std::string &key, std::ostream &outStream) const;
  std::string deserializeKey(std::istream &inStream) const;
  void createSignersAndVerifiers(std::istream &inStream);

  static void registerClass();

 private:
  ReplicaConfig *config_ = nullptr;

  static const std::string className_;
  static const uint32_t classVersion_;
  static bool registered_;
};

}
