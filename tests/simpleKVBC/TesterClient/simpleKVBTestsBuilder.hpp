// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
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

#include <list>
#include <map>

#include "log/logger.hpp"
#include "KVBCInterfaces.h"
#include "skvbc_messages.cmf.hpp"

namespace concord::kvbc::test {

struct SimpleBlock {
  uint64_t id = 0;
  std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> items;
};

typedef std::map<std::pair<std::vector<uint8_t>, uint64_t>, std::vector<uint8_t>> KeyBlockIdToValueMap;

class TestsBuilder {
 public:
  TestsBuilder(BlockId lastBlockId) : prevLastBlockId_(lastBlockId), lastBlockId_(lastBlockId) {
    LOG_INFO(logger_, "TestsBuilder: initialBlockId_=" << lastBlockId_);
  }

  ~TestsBuilder() { LOG_INFO(logger_, "TestsBuilder: The last DB block is " << lastBlockId_); }

  void createRandomTest(size_t numOfRequests, size_t seed);
  std::list<skvbc::messages::SKVBCRequest>& getRequests() { return requests_; }
  std::list<skvbc::messages::SKVBCReply>& getReplies() { return replies_; }

 private:
  void create(size_t numOfRequests, size_t seed);
  void createAndInsertRandomConditionalWrite();
  void createAndInsertRandomRead();
  void createAndInsertGetLastBlock();
  void addExpectedWriteReply(bool foundConflict);
  bool lookForConflicts(uint64_t readVersion, const std::vector<std::vector<uint8_t>>& readKeysArray);
  void addNewBlock(const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& writesKVArray);
  // void retrieveExistingBlocksFromKVB();

 private:
  logging::Logger logger_ = logging::getLogger("concord.kvbc.tests.simple_kvbc");
  std::list<skvbc::messages::SKVBCRequest> requests_;
  std::list<skvbc::messages::SKVBCReply> replies_;
  std::map<uint64_t, SimpleBlock> internalBlockchain_;
  KeyBlockIdToValueMap allKeysToValueMap_;
  BlockId prevLastBlockId_ = 0;
  BlockId lastBlockId_ = 0;
};

}  // namespace concord::kvbc::test
