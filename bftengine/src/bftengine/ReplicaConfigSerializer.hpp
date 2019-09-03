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
namespace impl {

// As ReplicaConfig is an interface struct, we need this additional wrapper
// class for its serialization/deserialization functionality.
// Any ReplicaConfig changes require synchronization with this class and an
// update of ReplicaConfigSerializer::classVersion_.
class ReplicaConfigSerializer: public concord::serialize::SerializableFactory<ReplicaConfigSerializer>  {
 public:
  explicit ReplicaConfigSerializer(ReplicaConfig *config);
  ReplicaConfigSerializer();
  ~ReplicaConfigSerializer() override = default;

  ReplicaConfig *getConfig() const { return config_.get(); }
  void setConfig(const ReplicaConfig &config);

  bool operator==(const ReplicaConfigSerializer &other) const;

  // Serialization/deserialization
  static uint32_t maxSize(uint32_t numOfReplicas);
 protected:

  virtual void serializeDataMembers  (std::ostream&) const override;
  virtual void deserializeDataMembers(std::istream&)       override;
  const std::string getVersion() const override { return "1"; };

 private:
  void serializeKey(const std::string &key, std::ostream &outStream) const;
  std::string deserializeKey(std::istream &inStream) const;
  void createSignersAndVerifiers(std::istream &inStream, ReplicaConfig &newObject);
  void serializePointer(concord::serialize::Serializable *ptrToClass, std::ostream &outStream) const;
  static concord::serialize::SerializablePtr deserializePointer(std::istream &inStream);

 private:
  std::unique_ptr<ReplicaConfig> config_;

  // Place holders for shared pointers to serializable classes
  concord::serialize::SerializablePtr thresholdSignerForExecution_;
  concord::serialize::SerializablePtr thresholdVerifierForExecution_;
  concord::serialize::SerializablePtr thresholdSignerForSlowPathCommit_;
  concord::serialize::SerializablePtr thresholdVerifierForSlowPathCommit_;
  concord::serialize::SerializablePtr thresholdSignerForCommit_;
  concord::serialize::SerializablePtr thresholdVerifierForCommit_;
  concord::serialize::SerializablePtr thresholdSignerForOptimisticCommit_;
  concord::serialize::SerializablePtr thresholdVerifierForOptimisticCommit_;

};

}
}
