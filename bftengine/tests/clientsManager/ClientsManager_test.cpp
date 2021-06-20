// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ClientsManager.hpp"
#include "gtest/gtest.h"
#include <chrono>
#include <thread>

using namespace std;
using namespace bftEngine;
concordMetrics::Component metrics{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())};

TEST(ClientsManager, reservedPagesPerClient) {
  uint32_t sizeOfReservedPage = 1024;
  uint32_t maxReplysize = 1000;
  auto numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 1);
  maxReplysize = 3000;
  numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 3);
  maxReplysize = 1024;
  numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 1);
}

TEST(ClientsManager, constructor) {
  std::set<bftEngine::impl::NodeIdType> clset{1, 4, 50, 7};
  bftEngine::impl::ClientsManager cm{metrics, clset};
  ASSERT_EQ(50, cm.getHighestIdOfNonInternalClient());
  auto i = 0;
  for (auto id : clset) {
    ASSERT_EQ(i, cm.getIndexOfClient(id));
    ASSERT_EQ(false, cm.isInternal(id));
    ++i;
  }
}

// Test that interanl clients are added to Client manager data structures
// and are identifed as internal clients
TEST(ClientsManager, initInternalClientInfo) {
  std::set<bftEngine::impl::NodeIdType> clset{1, 4, 50, 7};
  bftEngine::impl::ClientsManager cm{metrics, clset};
  auto firstIntClId = cm.getHighestIdOfNonInternalClient() + 1;
  auto FirstIntIdx = cm.getIndexOfClient(cm.getHighestIdOfNonInternalClient()) + 1;
  auto numRep = 7;
  cm.initInternalClientInfo(numRep);
  for (int i = 0; i < numRep; i++) {
    ASSERT_EQ(true, cm.isValidClient(firstIntClId + i));
    ASSERT_EQ(true, cm.isInternal(firstIntClId + 1));
    ASSERT_EQ(FirstIntIdx + i, cm.getIndexOfClient(firstIntClId + i));
  }
  // One over the end
  ASSERT_EQ(false, cm.isValidClient(firstIntClId + numRep));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}