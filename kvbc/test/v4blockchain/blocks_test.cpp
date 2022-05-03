// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "v4blockchain/detail/blocks.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <random>
#include "storage/test/storage_test_common.h"

using namespace concord::kvbc;
using namespace ::testing;

namespace {

TEST(v4_block, creation) {
  {
    v4blockchain::detail::Block block;
    const auto& buffer = block.getBuffer();
    ASSERT_EQ(buffer.size(), v4blockchain::detail::Block::HEADER_SIZE);
    const auto& v = block.getVersion();
    ASSERT_EQ(v, v4blockchain::detail::Block::BLOCK_VERSION);
  }

  {
    auto size = uint64_t{100};
    auto block = v4blockchain::detail::Block{size};
    const auto& buffer = block.getBuffer();
    ASSERT_EQ(buffer.size(), v4blockchain::detail::Block::HEADER_SIZE);
    ASSERT_EQ(buffer.capacity(), size);
    const auto& v = block.getVersion();
    ASSERT_EQ(v, v4blockchain::detail::Block::BLOCK_VERSION);
  }
}

TEST(v4_block, new_block_digest) {
  // New blocks should have empty digest
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }

  {
    v4blockchain::detail::Block block;
    const auto& dig = block.parentDigest();
    ASSERT_EQ(dig, empty_digest);
  }

  {
    auto size = uint64_t{100};
    auto block = v4blockchain::detail::Block{size};
    const auto& dig = block.parentDigest();
    ASSERT_EQ(dig, empty_digest);
  }
}

TEST(v4_block, add_digest) {
  // New blocks should have empty digest
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }
  concord::util::digest::BlockDigest digest;
  int i = 0;
  for (auto& d : digest) {
    d = i++;
  }

  {
    v4blockchain::detail::Block block;
    block.addDigest(digest);
    const auto& dig = block.parentDigest();
    ASSERT_EQ(dig, digest);
  }

  {
    auto size = uint64_t{100};
    auto block = v4blockchain::detail::Block{size};
    const auto& emdig = block.parentDigest();
    ASSERT_EQ(emdig, empty_digest);
    block.addDigest(digest);
    const auto& dig = block.parentDigest();
    ASSERT_EQ(dig, digest);
  }
}

TEST(v4_block, add_updates) {
  // New blocks should have empty digest
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }
  {
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));
    v4blockchain::detail::Block block;
    const auto& buffer = block.getBuffer();
    ASSERT_EQ(buffer.size(), v4blockchain::detail::Block::HEADER_SIZE);
    block.addUpdates(updates);

    {
      const auto& buffer = block.getBuffer();
      ASSERT_GT(buffer.size(), v4blockchain::detail::Block::HEADER_SIZE);
      auto reconstruct_updates = block.getUpdates();
      auto input = reconstruct_updates.categoryUpdates();
      ASSERT_EQ(input.kv.count(versioned_cat), 1);
      auto reconstruct_ver_updates = std::get<categorization::VersionedInput>(input.kv[versioned_cat]);
      ASSERT_EQ(reconstruct_ver_updates.kv[key].data, val);
      ASSERT_EQ(reconstruct_ver_updates.kv[key].stale_on_update, false);
    }
  }

  {
    uint64_t size = 158;
    v4blockchain::detail::Block block{size};
    const auto& buffer = block.getBuffer();
    ASSERT_EQ(buffer.size(), v4blockchain::detail::Block::HEADER_SIZE);
    ASSERT_EQ(buffer.capacity(), size);

    {
      auto imm_cat = std::string("immuatables");
      auto key = std::string("immkey");
      auto val = std::string("immval");
      auto updates = categorization::Updates{};
      auto imm_updates = categorization::ImmutableUpdates{};
      imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
      updates.add(imm_cat, std::move(imm_updates));
      const auto& buffer = block.getBuffer();
      block.addUpdates(updates);

      ASSERT_GT(buffer.size(), v4blockchain::detail::Block::HEADER_SIZE);
      auto reconstruct_updates = block.getUpdates();
      auto input = reconstruct_updates.categoryUpdates();
      ASSERT_EQ(input.kv.count("versioned_cat"), 0);
      ASSERT_EQ(input.kv.count(imm_cat), 1);
      auto reconstruct_imm_updates = std::get<categorization::ImmutableInput>(input.kv[imm_cat]);
      ASSERT_EQ(reconstruct_imm_updates.kv[key].data, val);
      std::vector<std::string> v = {"1", "2", "33"};
      ASSERT_EQ(reconstruct_imm_updates.kv[key].tags, v);
    }
  }
}

TEST(v4_block, calculate_digest) {
  // New blocks should have empty digest
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }
  // Genesis block
  concord::util::digest::BlockDigest genesis_digest;
  {
    v4blockchain::detail::Block block;
    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto versioned_cat = std::string("versioned");
    auto verkey = std::string("verkey");
    auto verval = std::string("verval");
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("verkey", "verval");
    updates.add(versioned_cat, std::move(ver_updates));

    block.addUpdates(updates);

    auto reconstruct_updates = block.getUpdates();
    auto input = reconstruct_updates.categoryUpdates();
    ASSERT_EQ(input.kv.count(versioned_cat), 1);
    ASSERT_EQ(input.kv.count(imm_cat), 1);
    auto reconstruct_imm_updates = std::get<categorization::ImmutableInput>(input.kv[imm_cat]);
    ASSERT_EQ(reconstruct_imm_updates.kv[immkey].data, immval);
    std::vector<std::string> v = {"1", "2", "33"};
    ASSERT_EQ(reconstruct_imm_updates.kv[immkey].tags, v);

    auto reconstruct_ver_updates = std::get<categorization::VersionedInput>(input.kv[versioned_cat]);
    ASSERT_EQ(reconstruct_ver_updates.kv[verkey].data, verval);
    ASSERT_EQ(reconstruct_ver_updates.kv[verkey].stale_on_update, false);

    const auto& dig = block.parentDigest();
    ASSERT_EQ(dig, empty_digest);

    genesis_digest = block.calculateDigest(1);
  }

  {
    auto size = uint64_t{100};
    auto block = v4blockchain::detail::Block{size};
    const auto& emdig = block.parentDigest();
    ASSERT_EQ(emdig, empty_digest);

    auto merkle_cat = std::string("merkle_cat");
    auto merkey = std::string("merkey");
    auto merval = std::string("merval");
    auto updates = categorization::Updates{};
    auto merkle_updates = categorization::BlockMerkleUpdates{};

    merkle_updates.addUpdate("merkey", "merval");
    merkle_updates.addDelete("merdel");

    block.addDigest(genesis_digest);
    updates.add(merkle_cat, std::move(merkle_updates));
    block.addUpdates(updates);

    auto reconstruct_updates = block.getUpdates();
    auto input = reconstruct_updates.categoryUpdates();
    auto reconstruct_mer_updates = std::get<categorization::BlockMerkleInput>(input.kv[merkle_cat]);
    ASSERT_EQ(reconstruct_mer_updates.kv[merkey], merval);
    ASSERT_EQ(reconstruct_mer_updates.deletes[0], "merdel");
    ASSERT_EQ(reconstruct_mer_updates.deletes.size(), 1);

    const auto& dig = block.parentDigest();
    ASSERT_EQ(dig, genesis_digest);
    ASSERT_NE(dig, empty_digest);
  }
}

TEST(v4_block, buffer_size_for_updates) {
  // New blocks should have empty digest
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }

  auto block_data = std::string("block");

  v4blockchain::detail::Block block;
  ASSERT_EQ(block.getBuffer().size(), v4blockchain::detail::Block::HEADER_SIZE);
  auto imm_cat = std::string("immuatables");
  auto immkey = std::string("immkey");
  auto immval = std::string("immval");
  auto updates = categorization::Updates{};
  auto imm_updates = categorization::ImmutableUpdates{};
  imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
  updates.add(imm_cat, std::move(imm_updates));

  auto versioned_cat = std::string("versioned");
  auto verkey = std::string("verkey");
  auto verval = std::string("verval");
  auto ver_updates = categorization::VersionedUpdates{};
  ver_updates.addUpdate("verkey", "verval");
  updates.add(versioned_cat, std::move(ver_updates));

  block.addUpdates(updates);

  std::vector<uint8_t> updates_buffer;
  concord::kvbc::categorization::serialize(updates_buffer, updates.categoryUpdates());
  ASSERT_EQ(block.getBuffer().size(), v4blockchain::detail::Block::HEADER_SIZE + updates_buffer.size());

  const auto& dig = block.parentDigest();
  ASSERT_EQ(dig, empty_digest);

  auto dig1 = v4blockchain::detail::Block::calculateDigest(1, block_data.c_str(), block_data.size());
  block.addDigest(dig1);
  ASSERT_EQ(block.getBuffer().size(), v4blockchain::detail::Block::HEADER_SIZE + updates_buffer.size());

  auto reconstruct_updates = block.getUpdates();
  auto input = reconstruct_updates.categoryUpdates();
  ASSERT_EQ(input.kv.count(versioned_cat), 1);
  ASSERT_EQ(input.kv.count(imm_cat), 1);
  auto reconstruct_imm_updates = std::get<categorization::ImmutableInput>(input.kv[imm_cat]);
  ASSERT_EQ(reconstruct_imm_updates.kv[immkey].data, immval);
  std::vector<std::string> v = {"1", "2", "33"};
  ASSERT_EQ(reconstruct_imm_updates.kv[immkey].tags, v);

  auto reconstruct_ver_updates = std::get<categorization::VersionedInput>(input.kv[versioned_cat]);
  ASSERT_EQ(reconstruct_ver_updates.kv[verkey].data, verval);
  ASSERT_EQ(reconstruct_ver_updates.kv[verkey].stale_on_update, false);

  const auto& dig2 = block.parentDigest();
  ASSERT_EQ(dig1, dig2);
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
