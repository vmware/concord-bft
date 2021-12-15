// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#include "ReplicaSpecificInfoManager.hpp"
#include "Serializable.h"
#include <cstring>

namespace bftEngine {
namespace impl {
std::vector<uint8_t> RsiItem::serialize() {
  // The structure of the RSI in the persistent storage is: [(uint64_t) index | (uint64_t) requestSeqNum | (uint64_t)
  // dataSize | char* data]
  std::vector<uint8_t> serialized_data;
  serialized_data.resize(data_.size() + RSI_DATA_PREFIX_SIZE);
  memcpy(serialized_data.data(), &index_, RSI_INDEX_SIZE);
  memcpy(serialized_data.data() + RSI_INDEX_SIZE, &req_seq_num_, RSI_SN_SIZE);
  auto data_size = data_.size();
  memcpy(serialized_data.data() + RSI_INDEX_SIZE + RSI_SN_SIZE, &data_size, RSI_DATA_SIZE);
  memcpy(serialized_data.data() + RSI_DATA_PREFIX_SIZE, data_.data(), data_size);
  return serialized_data;
}
RsiItem RsiItem::deserialize(const std::vector<uint8_t>& data) {
  RsiItem item;
  std::memcpy(&item.index_, data.data(), RSI_INDEX_SIZE);
  std::memcpy(&item.req_seq_num_, data.data() + RSI_INDEX_SIZE, RSI_SN_SIZE);
  size_t data_size = 0;
  std::memcpy(&data_size, data.data() + RSI_INDEX_SIZE + RSI_SN_SIZE, RSI_DATA_SIZE);
  std::memcpy(item.data_.data(), data.data() + RSI_DATA_PREFIX_SIZE, data_size);
  return item;
}

RsiItem RsiDataManager::getRsiForClient(uint32_t client_id, uint64_t req_sn) {
  for (const auto& rsi_item : rsiCache_[client_id]) {
    if (rsi_item.sequence_number() == req_sn) return rsi_item;
  }
  return RsiItem();
}
void RsiDataManager::setRsiForClient(uint32_t client_id, uint64_t req_sn, const std::string& data) {
  // First, compute the correct index for this data
  uint32_t nextClientIndex = clientsIndex_[client_id];
  clientsIndex_[client_id]++;
  uint32_t storageIndex = client_id * max_client_batch_size_ + (nextClientIndex % max_client_batch_size_);
  RsiItem rsi_item(nextClientIndex, req_sn, data);
  ps_->beginWriteTran();
  ps_->setReplicaSpecificInfo(storageIndex, rsi_item.serialize());
  ps_->endWriteTran();
  if (rsiCache_[client_id].size() >= max_client_batch_size_) {
    rsiCache_[client_id].pop_front();
  }
  // Add the new record to the cache
  rsiCache_[client_id].emplace_back(rsi_item);
}
void RsiDataManager::init() {
  for (uint32_t clientId = 0; clientId < num_of_principles_; clientId++) {
    clientsIndex_[clientId] = 0;
    std::vector<RsiItem> clientRsiData;
    for (uint32_t offset = 0; offset < max_client_batch_size_; offset++) {
      std::vector<uint8_t> data = ps_->getReplicaSpecificInfo(clientId * max_client_batch_size_ + offset);
      if (data.empty()) continue;
      clientRsiData.emplace_back(RsiItem::deserialize(data));
    }
    std::sort(clientRsiData.begin(), clientRsiData.end(), [](auto& data1, auto& data2) {
      return data1.index() < data2.index();
    });
    for (const auto& rsiItem : clientRsiData) {
      rsiCache_[clientId].emplace_back(rsiItem);
    }
    if (!clientRsiData.empty()) clientsIndex_[clientId] = clientRsiData.back().index();
  }
}
}  // namespace impl
}  // namespace bftEngine
