// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <map>
#include <vector>

#include "Logger.hpp"
#include "kv_replica_msgs.cmf.hpp"
#include "MessageParser.hpp"
#include "IMessageGenerator.hpp"

namespace concord::osexample {

using BlockId = std::uint64_t;
using KeyBlockIdToValueMap = std::map<std::pair<std::vector<uint8_t>, uint64_t>, std::vector<uint8_t>>;

struct KVBlock {
  uint64_t id = 0;
  std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> items;
};

class KVMessageGenerator : public IMessageGenerator {
 public:
  KVMessageGenerator(std::shared_ptr<MessageConfig> msgConfig) : msgConfig_(msgConfig) {}
  virtual ~KVMessageGenerator() = default;

  bft::client::Msg createWriteRequest() override final;
  bft::client::Msg createReadRequest() override final;

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("osexample::KVMessageGenerator"));
    return logger_;
  }

  void addExpectedWriteReply(bool foundConflict);
  void addExpectedReadReply(size_t numberOfKeysToRead, kv::messages::KVReadRequest& readReq);
  bool lookForConflicts(uint64_t readVersion, const std::vector<std::vector<uint8_t>>& readKeysArray);
  void addNewBlock(const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& writesKVArray);

 private:
  std::shared_ptr<MessageConfig> msgConfig_;
  std::list<kv::messages::KVRequest> requests_;
  std::list<kv::messages::KVReply> replies_;
  std::map<BlockId, KVBlock> internalBlockchain_;
  KeyBlockIdToValueMap allKeysToValueMap_;
  BlockId prevLastBlockId_ = 0;
  BlockId lastBlockId_ = 0;
};
}  // end namespace concord::osexample
