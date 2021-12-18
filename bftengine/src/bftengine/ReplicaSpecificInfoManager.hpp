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

#pragma once
#include "PersistentStorage.hpp"
#include <string>
#include <vector>
#include <memory>
namespace bftEngine {
namespace impl {
class RsiItem {
 public:
  static const uint32_t RSI_INDEX_SIZE = sizeof(uint64_t);
  static const uint32_t RSI_SN_SIZE = sizeof(uint64_t);
  static const size_t RSI_DATA_SIZE = sizeof(size_t);
  static const uint32_t RSI_DATA_PREFIX_SIZE = RSI_INDEX_SIZE + RSI_SN_SIZE + RSI_DATA_SIZE;
  RsiItem() = default;
  RsiItem(uint64_t index, uint64_t req_seq_num, const std::string& data)
      : data_{data}, index_{index}, req_seq_num_{req_seq_num} {};
  std::vector<uint8_t> serialize();
  const std::string& data() const { return data_; }
  uint64_t sequence_number() const { return req_seq_num_; }
  uint64_t index() const { return index_; }

  static RsiItem deserialize(const std::vector<uint8_t>& data);

 private:
  std::string data_;
  uint64_t index_;
  uint64_t req_seq_num_;
};

class RsiDataManager {
 public:
  RsiDataManager(uint32_t num_of_principles, uint32_t max_client_batch_size, std::shared_ptr<PersistentStorage> ps)
      : num_of_principles_{num_of_principles}, max_client_batch_size_{max_client_batch_size}, ps_{ps} {
    init();
  }
  RsiItem getRsiForClient(uint32_t client_id, uint64_t req_sn);
  void setRsiForClient(uint32_t client_id, uint64_t req_sn, const std::string& data);

 private:
  void init();

  uint32_t num_of_principles_;
  uint32_t max_client_batch_size_;
  std::shared_ptr<PersistentStorage> ps_;
  // A map of clientId -> deque<requestSeqNum,rsi>
  std::unordered_map<uint32_t, std::deque<RsiItem>> rsiCache_;
  // A map that indicates what is current index of each client
  std::unordered_map<uint32_t, uint64_t> clientsIndex_;
};
}  // namespace impl
}  // namespace bftEngine