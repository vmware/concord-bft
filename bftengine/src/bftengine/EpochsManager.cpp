// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "EpochsManager.hpp"

namespace bftEngine {

void EpochManager::updateEpochForReplica(uint32_t replica_id, uint64_t epoch_id) {
  epochs_data_.epochs_[replica_id] = epoch_id;
  // update the data and save it on the reserved pages
  std::ostringstream outStream;
  concord::serialize::Serializable::serialize(outStream, epochs_data_);
  auto data = outStream.str();
  saveReservedPage(0, data.size(), data.data());
}
uint64_t EpochManager::getEpochForReplica(uint32_t replica_id) { return epochs_data_.epochs_[replica_id]; }
const EpochManager::EpochsData& EpochManager::getEpochData() { return epochs_data_; }
void EpochManager::EpochsData::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, n_);
  for (uint32_t i = 0; i < n_; i++) {
    serialize(outStream, epochs_.at(i));
  }
}
void EpochManager::EpochsData::deserializeDataMembers(std::istream& inStream) {
  uint32_t n = n_;
  deserialize(inStream, n);
  for (uint32_t i = 0; i < n_; i++) {
    deserialize(inStream, epochs_[i]);
  }
  for (uint32_t i = n; i < n_; i++) {
    epochs_.emplace(i, 0);
  }
}
}  // namespace bftEngine