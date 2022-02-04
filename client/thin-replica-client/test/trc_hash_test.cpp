// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"

#include "client/thin-replica-client/trc_hash.hpp"
#include "client/concordclient/remote_update_data.hpp"
#include "kvbc_app_filter/kvbc_app_filter.h"

using com::vmware::concord::thin_replica::Data;
using std::make_pair;
using std::string;
using std::to_string;
using client::concordclient::hashUpdate;
using concord::client::concordclient::RemoteData;
using concord::client::concordclient::Update;

const string kSampleUpdateExpectedHash({'\x02', '\x3D', '\x0D', '\x8B', '\xC6', '\x54', '\x07', '\xD7',
                                        '\x33', '\x94', '\x99', '\xFE', '\x9F', '\x6E', '\x8E', '\xB3',
                                        '\x1A', '\x76', '\xB3', '\x70', '\xE6', '\xDA', '\x69', '\x9F',
                                        '\x64', '\x52', '\xDD', '\xA5', '\x5B', '\xD3', '\xF6', '\x62'});

namespace {

TEST(trc_hash, hash_update) {
  Update legacy_event;
  legacy_event.block_id = 1337;
  for (int i = 0; i < 3; ++i) {
    legacy_event.kv_pairs.push_back(make_pair(to_string(i), to_string(i)));
  }
  RemoteData update = legacy_event;
  EXPECT_EQ(hashUpdate(update), kSampleUpdateExpectedHash);
}

TEST(trc_hash, hash_data) {
  Data update;
  update.mutable_events()->set_block_id(1337);
  for (int i = 0; i < 3; ++i) {
    auto data = update.mutable_events()->add_data();
    data->set_key(to_string(i));
    data->set_value(to_string(i));
  }

  EXPECT_EQ(hashUpdate(update), kSampleUpdateExpectedHash);
}

TEST(trc_hash, trs_trc_legacy) {
  // Data created on the TRS
  concord::kvbc::KvbFilteredUpdate kvb_update;
  kvb_update.block_id = 1337;
  kvb_update.kv_pairs.push_back(std::make_pair("Hello", "World"));
  kvb_update.kv_pairs.push_back(std::make_pair("Legacy", "Event"));

  // Data received on the TRC
  Data data_update;
  data_update.mutable_events()->set_block_id(kvb_update.block_id);
  for (const auto& kv : kvb_update.kv_pairs) {
    auto* data = data_update.mutable_events()->add_data();
    data->set_key(kv.first);
    data->set_value(kv.second);
  }

  EXPECT_EQ(concord::kvbc::KvbAppFilter::hashUpdate(kvb_update), hashUpdate(data_update));
}

TEST(trc_hash, trs_trc_event_group) {
  // Data created on the TRS
  concord::kvbc::KvbFilteredEventGroupUpdate kvb_update;
  kvb_update.event_group_id = 1337;
  kvb_update.event_group.events.push_back({"Hello", {}});
  kvb_update.event_group.events.push_back({"World", {}});

  // Data received on the TRC
  Data data_update;
  data_update.mutable_event_group()->set_id(kvb_update.event_group_id);
  for (const auto& event : kvb_update.event_group.events) {
    *data_update.mutable_event_group()->add_events() = event.data;
  }

  EXPECT_EQ(concord::kvbc::KvbAppFilter::hashEventGroupUpdate(kvb_update), hashUpdate(data_update));
}

}  // anonymous namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
