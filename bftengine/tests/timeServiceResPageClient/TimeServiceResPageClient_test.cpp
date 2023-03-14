// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "TimeServiceResPageClient.hpp"
#include "gtest/gtest.h"
#include <chrono>

using namespace bftEngine;
using namespace bftEngine::impl;

struct ReservedPagesMock : public IReservedPages {
  mutable bool is_first_load = true;
  std::string page_ = std::string(sizeof(ConsensusTickRep), 0);
  ReservedPagesMock() { ReservedPagesClientBase::setReservedPages(this); }
  ~ReservedPagesMock() { ReservedPagesClientBase::setReservedPages(nullptr); }
  virtual uint32_t numberOfReservedPages() const { return 1; };
  virtual uint32_t sizeOfReservedPage() const { return page_.size(); };
  virtual bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const {
    if (is_first_load) {
      is_first_load = false;
      return false;
    }
    (void)reservedPageId;
    memcpy(outReservedPage, page_.c_str(), copyLength);
    return true;
  };
  virtual void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) {
    (void)reservedPageId;
    page_ = std::string(inReservedPage, inReservedPage + copyLength);
  };
  virtual void zeroReservedPage(uint32_t reservedPageId) {
    (void)reservedPageId;
    page_ = std::string(sizeof(ConsensusTickRep), 0);
  };
};

TEST(TimeServiceResPageClient, InitWithEmptyReservedPages) {
  ReservedPagesMock m;
  auto client = TimeServiceResPageClient{};
  EXPECT_EQ(client.getLastTimestamp(), ConsensusTime::min());

  auto timestamp = std::chrono::duration_cast<ConsensusTime>(std::chrono::system_clock::now().time_since_epoch());
  client.setLastTimestamp(timestamp);
  EXPECT_EQ(timestamp, client.getLastTimestamp());

  // Check that the client saves to RP automatically
  ConsensusTickRep raw = 0;
  m.loadReservedPage(0, sizeof(ConsensusTickRep), reinterpret_cast<char*>(&raw));
  auto from_rp = ConsensusTime{raw};
  EXPECT_EQ(timestamp, from_rp);

  timestamp += std::chrono::milliseconds{10};
  client.setLastTimestamp(timestamp);
  EXPECT_EQ(timestamp, client.getLastTimestamp());
}

TEST(TimeServiceResPageClient, setTimeStampFromTicks) {
  ReservedPagesMock m;
  auto client = TimeServiceResPageClient{};
  EXPECT_EQ(client.getLastTimestamp(), ConsensusTime::min());

  auto ticks = std::chrono::duration_cast<ConsensusTime>(std::chrono::system_clock::now().time_since_epoch()).count();
  client.setTimestampFromTicks(ticks);
  EXPECT_EQ(ticks, client.getLastTimestamp().count());
}

TEST(TimeServiceResPageClient, InitWithExistingReservedPages) {
  ReservedPagesMock m;
  auto raw_value = int64_t{100};
  m.saveReservedPage(0, sizeof(ConsensusTickRep), reinterpret_cast<char*>(&raw_value));
  m.is_first_load = false;

  auto client = TimeServiceResPageClient{};
  EXPECT_EQ(client.getLastTimestamp(), ConsensusTime{raw_value});

  auto timestamp = std::chrono::duration_cast<ConsensusTime>(std::chrono::system_clock::now().time_since_epoch());
  client.setLastTimestamp(timestamp);
  EXPECT_EQ(timestamp, client.getLastTimestamp());

  timestamp += std::chrono::milliseconds{10};
  client.setLastTimestamp(timestamp);
  EXPECT_EQ(timestamp, client.getLastTimestamp());
}

TEST(TimeServiceResPageClient, ReloadAfterStateTransfer) {
  ReservedPagesMock m;
  auto client = TimeServiceResPageClient{};
  EXPECT_EQ(client.getLastTimestamp(), ConsensusTime::min());

  auto timestamp = std::chrono::duration_cast<ConsensusTime>(std::chrono::system_clock::now().time_since_epoch());
  client.setLastTimestamp(timestamp);
  EXPECT_EQ(timestamp, client.getLastTimestamp());

  auto value_from_st = timestamp + ConsensusTime{10000};
  auto raw_value = value_from_st.count();
  m.saveReservedPage(0, sizeof(ConsensusTickRep), reinterpret_cast<char*>(&raw_value));

  // Changing reserved pages does not affect the client
  EXPECT_EQ(timestamp, client.getLastTimestamp());

  // Load udpates internal state
  client.load();
  EXPECT_EQ(value_from_st, client.getLastTimestamp());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
