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
#include "ReservedPagesMock.hpp"

using namespace std;
using namespace bftEngine;
concordMetrics::Component metrics{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())};
bftEngine::test::ReservedPagesMock<bftEngine::impl::ClientsManager> res_pages_mock_;

TEST(ClientsManager, reservedPagesPerClient) {
  uint32_t sizeOfReservedPage = 1024;
  uint32_t maxReplysize = 1000;
  auto numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 2);
  maxReplysize = 3000;
  numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 4);
  maxReplysize = 1024;
  numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 2);
}

TEST(ClientsManager, constructor) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  bftEngine::impl::ClientsManager cm{{1, 4, 50, 7}, {3, 2, 60}, {5, 11, 55}, metrics};
  for (auto id : {5, 11, 55}) ASSERT_EQ(true, cm.isInternal(id));
  for (auto id : {1, 4, 50, 3, 2, 60}) ASSERT_EQ(false, cm.isInternal(id));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
