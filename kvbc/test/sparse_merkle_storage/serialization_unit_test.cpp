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

#include "kvbc_storage_test_common.h"

#include "endianness.hpp"
#include "kv_types.hpp"
#include "merkle_tree_db_adapter.h"
#include "merkle_tree_key_manipulator.h"
#include "merkle_tree_serialization.h"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "storage/merkle_tree_key_manipulator.h"

#include <stdint.h>

#include <string>
#include <type_traits>
#include <utility>

namespace {

using namespace ::concord::kvbc::v2MerkleTree;
using namespace ::concord::kvbc::v2MerkleTree::detail;
using namespace ::concord::storage::v2MerkleTree::detail;

using ::concordUtils::Sliver;

using ::concord::kvbc::BlockId;
using ::concord::kvbc::Key;
using ::concord::kvbc::OrderedKeysSet;
using ::concord::kvbc::SetOfKeyValuePairs;
using ::concord::kvbc::sparse_merkle::BatchedInternalNode;
using ::concord::kvbc::sparse_merkle::InternalChild;
using ::concord::kvbc::sparse_merkle::InternalNodeKey;
using ::concord::kvbc::sparse_merkle::LeafChild;
using ::concord::kvbc::sparse_merkle::LeafKey;
using ::concord::kvbc::sparse_merkle::LeafNode;
using ::concord::kvbc::sparse_merkle::NibblePath;
using ::concord::kvbc::sparse_merkle::Version;

using ::concord::storage::ObjectId;

Sliver toSliver(std::string b) { return Sliver{std::move(b)}; }

template <typename E>
std::string serializeEnum(E e) {
  static_assert(std::is_enum_v<E>);
  return std::string{static_cast<char>(e)};
}

template <typename T>
std::string serializeIntegral(T v) {
  v = concordUtils::hostToNet(v);
  const auto data = reinterpret_cast<const char *>(&v);
  return std::string{data, sizeof(v)};
}

const auto defaultVersion = Version{42};
const auto defaultHash = getHash(defaultData);
const auto defaultHashStrBuf = std::string{reinterpret_cast<const char *>(defaultHash.data()), defaultHash.size()};
const auto defaultObjectId = ObjectId{42};
const auto defaultPageId = uint32_t{42};
const auto defaultChkpt = uint64_t{42};
const auto defaultDigest = getBlockDigest(defaultData + defaultData);

const auto metadataKeyManip = ::concord::storage::v2MerkleTree::MetadataKeyManipulator{};
const auto stKeyManip = ::concord::storage::v2MerkleTree::STKeyManipulator{};

static_assert(sizeof(EDBKeyType) == 1);
static_assert(sizeof(EKeySubtype) == 1);
static_assert(sizeof(EBFTSubtype) == 1);
static_assert(sizeof(BlockId) == 8);
static_assert(sizeof(ObjectId) == 4);

// Expected key structure with respective bit sizes:
// [EDBKeyType::Block: 8, blockId: 64]
TEST(key_manipulator, block_key) {
  const auto key = DBKeyManipulator::genBlockDbKey(defaultBlockId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Block) + serializeIntegral(defaultBlockId));
  ASSERT_EQ(key.length(), 1 + sizeof(defaultBlockId));
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::NonProvableKey: 8, key: user-defined, blockId: 64]
TEST(key_manipulator, non_provable_key) {
  const auto nonProvKey = Sliver{"non-provable"};
  const auto key = DBKeyManipulator::genNonProvableDbKey(defaultBlockId, nonProvKey);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::NonProvable) +
                                 nonProvKey.toString() + serializeIntegral(defaultBlockId));
  ASSERT_EQ(key.length(),
            sizeof(EDBKeyType::Key) + sizeof(EKeySubtype::NonProvable) + nonProvKey.length() + sizeof(BlockId));
  ASSERT_TRUE(key == expected);
}

TEST(key_manipulator, extract_block_id_from_non_provable_key) {
  const auto nonProvKey = Sliver{"non-provable"};
  const auto key = DBKeyManipulator::genNonProvableDbKey(defaultBlockId, nonProvKey);
  const auto expected_block_id = DBKeyManipulator::extractBlockIdFromNonProvableKey(key);
  ASSERT_EQ(expected_block_id, defaultBlockId);
}

TEST(key_manipulator, extract_key_from_non_provable_key) {
  const auto nonProvKey = Sliver{"non-provable"};
  const auto key = DBKeyManipulator::genNonProvableDbKey(defaultBlockId, nonProvKey);
  const auto expected_key = DBKeyManipulator::extractKeyFromNonProvableKey(key);
  ASSERT_EQ(expected_key, nonProvKey);
}

TEST(key_manipulator, non_provable_stale_key) {
  const auto nonProvKey = Sliver{"non-provable"};
  const auto nonProvableKey = DBKeyManipulator::genNonProvableDbKey(defaultBlockId, nonProvKey);
  const auto key = DBKeyManipulator::genNonProvableStaleDbKey(nonProvableKey, defaultBlockId + 1);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::NonProvableStale) +
                                 serializeIntegral(defaultBlockId + 1) + nonProvableKey.toString());
  ASSERT_EQ(
      key.length(),
      sizeof(EDBKeyType::Key) + sizeof(EKeySubtype::NonProvableStale) + sizeof(BlockId) + nonProvableKey.length());
  ASSERT_EQ(expected, key);
}

TEST(key_manipulator, extract_block_id_from_non_provable_stale_key) {
  const auto nonProvKey = Sliver{"non-provable"};
  const auto staleSince = defaultBlockId + 1;
  const auto nonProvableKey = DBKeyManipulator::genNonProvableDbKey(defaultBlockId, nonProvKey);
  const auto key = DBKeyManipulator::genNonProvableStaleDbKey(nonProvableKey, staleSince);
  const auto actualBlockId = DBKeyManipulator::extractBlockIdFromNonProvableStaleKey(key);
  ASSERT_EQ(actualBlockId, staleSince);
}

TEST(key_manipulator, extract_key_from_non_provable_stale_key) {
  const auto nonProvKey = Sliver{"non-provable"};
  const auto staleSince = defaultBlockId + 1;
  const auto nonProvableKey = DBKeyManipulator::genNonProvableDbKey(defaultBlockId, nonProvKey);
  const auto key = DBKeyManipulator::genNonProvableStaleDbKey(nonProvableKey, staleSince);
  const auto actualStaleKey = DBKeyManipulator::extractKeyFromNonProvableStaleKey(key);
  ASSERT_EQ(actualStaleKey, nonProvableKey);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Leaf: 8, keyHash: 256, keyVersion: 64]
TEST(key_manipulator, data_key_leaf) {
  const auto leafKey = LeafKey{defaultHash, defaultVersion};
  const auto key = DBKeyManipulator::genDataDbKey(leafKey);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::Leaf) + defaultHashStrBuf +
                                 serializeIntegral(leafKey.version().value()));
  ASSERT_EQ(key.length(), 1 + 1 + 32 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Leaf: 8, keyHash: 256, keyVersion: 64]
TEST(key_manipulator, data_key_sliver) {
  const auto version = 42ul;
  const auto key = DBKeyManipulator::genDataDbKey(defaultSliver, version);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::Leaf) + defaultHashStrBuf +
                                 serializeIntegral(version));
  ASSERT_EQ(key.length(), 1 + 1 + 32 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Internal: 8, version: 64, numNibbles: 8, nibbles: 16]
TEST(key_manipulator, internal_key_even) {
  auto path = NibblePath{};
  // first byte of the nibble path is 0x12 (by appending 0x01 and 0x02)
  path.append(0x01);
  path.append(0x02);
  // second byte of the nibble path is 0x34 (by appending 0x03 and 0x04)
  path.append(0x03);
  path.append(0x04);
  const auto internalKey = InternalNodeKey{defaultVersion, path};
  const auto key = DBKeyManipulator::genInternalDbKey(internalKey);
  // Expect that two bytes of nibbles have been written.
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::Internal) +
                                 serializeIntegral(internalKey.version().value()) + serializeIntegral(std::uint8_t{4}) +
                                 serializeIntegral(std::uint8_t{0x12}) + serializeIntegral(std::uint8_t{0x34}));
  ASSERT_EQ(key.length(), 1 + 1 + 8 + 1 + 2);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Internal: 8, version: 64, numNibbles: 8, nibbles: 16]
TEST(key_manipulator, internal_key_odd) {
  auto path = NibblePath{};
  // first byte of the nibble path is 0x12 (by appending 0x01 and 0x02)
  path.append(0x01);
  path.append(0x02);
  // second byte of the nibble path is 0x30 (by appending 0x03)
  path.append(0x03);
  const auto internalKey = InternalNodeKey{defaultVersion, path};
  const auto key = DBKeyManipulator::genInternalDbKey(internalKey);
  // Expect that two bytes of nibbles have been written.
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::Internal) +
                                 serializeIntegral(internalKey.version().value()) + serializeIntegral(std::uint8_t{3}) +
                                 serializeIntegral(std::uint8_t{0x12}) + serializeIntegral(std::uint8_t{0x30}));
  ASSERT_EQ(key.length(), 1 + 1 + 8 + 1 + 2);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EBFTSubtype::Metadata: 8, objectId: 32]
TEST(key_manipulator, metadata_key) {
  const auto key = metadataKeyManip.generateMetadataKey(defaultObjectId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::Metadata) +
                                 serializeIntegral(defaultObjectId));
  ASSERT_EQ(key.length(), 1 + 1 + 4);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EBFTSubtype::ST: 8, objectId: 32]
TEST(key_manipulator, st_key) {
  const auto key = stKeyManip.generateStateTransferKey(defaultObjectId);
  const auto expected =
      toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::ST) + serializeIntegral(defaultObjectId));
  ASSERT_EQ(key.length(), 1 + 1 + 4);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EBFTSubtype::STPendingPage: 8, pageId: 32]
TEST(key_manipulator, st_pending_page_key) {
  const auto key = stKeyManip.generateSTPendingPageKey(defaultPageId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STPendingPage) +
                                 serializeIntegral(defaultPageId));
  ASSERT_EQ(key.length(), 1 + 1 + 4);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EBFTSubtype::STCheckpointDescriptor: 8, chkpt: 64]
TEST(key_manipulator, st_chkpt_desc_key) {
  const auto key = stKeyManip.generateSTCheckpointDescriptorKey(defaultChkpt);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STCheckpointDescriptor) +
                                 serializeIntegral(defaultChkpt));
  ASSERT_EQ(key.length(), 1 + 1 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EBFTSubtype::STReservedPageDynamic: 8, pageId: 32, chkpt: 64]
TEST(key_manipulator, st_res_page_dynamic_key) {
  const auto key = stKeyManip.generateSTReservedPageDynamicKey(defaultPageId, defaultChkpt);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STReservedPageDynamic) +
                                 serializeIntegral(defaultPageId) + serializeIntegral(defaultChkpt));
  ASSERT_EQ(key.length(), 1 + 1 + 4 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EBFTSubtype::STTempBlock: 8, blockId: 64]
TEST(key_manipulator, st_temp_block_key) {
  const auto key = DBKeyManipulator::generateSTTempBlockKey(defaultBlockId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STTempBlock) +
                                 serializeIntegral(defaultBlockId));
  ASSERT_EQ(key.length(), 1 + 1 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Stale: 8, staleSinceVersion: 64]
TEST(key_manipulator, stale_db_key_empty) {
  const auto key = DBKeyManipulator::genStaleDbKey(defaultVersion);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::ProvableStale) +
                                 serializeIntegral(defaultBlockId));
  ASSERT_EQ(key.length(), 1 + 1 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Stale: 8, staleSinceVersion: 64, EDBKeyType::Key: 8, EKeySubtype::internal: 8,
// internalKeyVersion: 64, numNibbles: 8, nibbles: 16]
TEST(key_manipulator, stale_db_key_internal) {
  auto path = NibblePath{};
  // first byte of the nibble path is 0x12 (by appending 0x01 and 0x02)
  path.append(0x01);
  path.append(0x02);
  // second byte of the nibble path is 0x34 (by appending 0x03 and 0x04)
  path.append(0x03);
  path.append(0x04);
  const auto internalKey = InternalNodeKey{defaultVersion, path};
  const auto key = DBKeyManipulator::genStaleDbKey(internalKey, defaultVersion);
  const auto expected =
      toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::ProvableStale) +
               serializeIntegral(defaultBlockId) + DBKeyManipulator::genInternalDbKey(internalKey).toString());
  ASSERT_EQ(key.length(), 1 + 1 + 8 + 1 + 1 + 8 + 1 + 2);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Key: 8, EKeySubtype::Stale: 8, staleSinceVersion: 64, EDBKeyType::Key: 8, EKeySubtype::Leaf: 8,
// keyHash: 256, keyVersion: 64]
TEST(key_manipulator, stale_db_key_leaf) {
  const auto leafKey = LeafKey{defaultHash, defaultVersion};
  const auto key = DBKeyManipulator::genStaleDbKey(leafKey, defaultVersion);
  const auto expected =
      toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::ProvableStale) +
               serializeIntegral(defaultBlockId) + DBKeyManipulator::genDataDbKey(leafKey).toString());
  ASSERT_EQ(key.length(), 1 + 1 + 8 + 1 + 1 + 32 + 8);
  ASSERT_TRUE(key == expected);
}

TEST(block, key_data_equality) {
  ASSERT_TRUE(block::detail::KeyData{true} == block::detail::KeyData{true});
  ASSERT_FALSE(block::detail::KeyData{true} == block::detail::KeyData{false});
}

TEST(block, block_node_equality) {
  // Non-key differences.
  {
    const auto node1 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    const auto node2 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    const auto node3 = block::detail::Node{defaultBlockId + 1, defaultDigest, defaultHash, defaultVersion};
    const auto node4 = block::detail::Node{defaultBlockId, getBlockDigest("random data"), defaultHash, defaultVersion};
    const auto node5 = block::detail::Node{defaultBlockId, defaultDigest, getHash("random data2"), defaultVersion};
    const auto node6 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion + 1};

    ASSERT_EQ(node1.blockId, node2.blockId);
    ASSERT_TRUE(node1.parentDigest == node2.parentDigest);
    ASSERT_TRUE(node1.stateHash == node2.stateHash);
    ASSERT_TRUE(node1.stateRootVersion == node2.stateRootVersion);
    ASSERT_TRUE(node1.keys == node2.keys);

    ASSERT_NE(node1.blockId, node3.blockId);
    ASSERT_TRUE(node1.parentDigest == node3.parentDigest);
    ASSERT_TRUE(node1.stateHash == node3.stateHash);
    ASSERT_TRUE(node1.stateRootVersion == node3.stateRootVersion);
    ASSERT_TRUE(node1.keys == node3.keys);

    ASSERT_EQ(node1.blockId, node4.blockId);
    ASSERT_FALSE(node1.parentDigest == node4.parentDigest);
    ASSERT_TRUE(node1.stateHash == node4.stateHash);
    ASSERT_TRUE(node1.stateRootVersion == node4.stateRootVersion);
    ASSERT_TRUE(node1.keys == node4.keys);

    ASSERT_EQ(node1.blockId, node5.blockId);
    ASSERT_TRUE(node1.parentDigest == node5.parentDigest);
    ASSERT_FALSE(node1.stateHash == node5.stateHash);
    ASSERT_TRUE(node1.stateRootVersion == node5.stateRootVersion);
    ASSERT_TRUE(node1.keys == node5.keys);

    ASSERT_EQ(node1.blockId, node6.blockId);
    ASSERT_TRUE(node1.parentDigest == node6.parentDigest);
    ASSERT_TRUE(node1.stateHash == node6.stateHash);
    ASSERT_FALSE(node1.stateRootVersion == node6.stateRootVersion);
    ASSERT_TRUE(node1.keys == node6.keys);

    ASSERT_TRUE(node1 == node2);
    ASSERT_FALSE(node1 == node3);
    ASSERT_FALSE(node1 == node4);
    ASSERT_FALSE(node1 == node5);
    ASSERT_FALSE(node1 == node6);
  }

  // Differences in keys only.
  {
    auto node1 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    auto node2 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    node2.keys.emplace(defaultSliver, block::detail::KeyData{false});
    auto node3 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    node3.keys.emplace(defaultSliver, block::detail::KeyData{true});
    auto node4 = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    node4.keys.emplace(getSliverOfSize(42), block::detail::KeyData{true});

    ASSERT_FALSE(node1 == node2);
    ASSERT_FALSE(node1 == node3);
    ASSERT_FALSE(node2 == node3);
    ASSERT_FALSE(node3 == node4);
    ASSERT_FALSE(node2 == node4);
  }
}

TEST(block, block_node_serialization) {
  // No keys.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // 1 non-deleted key.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    node.keys.emplace(defaultSliver, block::detail::KeyData{false});

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // 1 deleted key.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    node.keys.emplace(defaultSliver, block::detail::KeyData{true});

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // Empty key.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    node.keys.emplace(getSliverOfSize(0), block::detail::KeyData{true});
    node.keys.emplace(getSliverOfSize(1), block::detail::KeyData{true});

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // Multiple keys with different sizes and deleted flags.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    auto deleted = false;
    for (auto i = 1u; i <= maxNumKeys; ++i) {
      node.keys.emplace(getSliverOfSize(i), block::detail::KeyData{deleted});
      deleted = !deleted;
    }

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }
}

TEST(block, state_root_deserialization) {
  // No keys.
  {
    const auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    const auto nodeSliver = block::detail::createNode(node);
    ASSERT_EQ(deserializeStateRootVersion(nodeSliver), defaultVersion);
  }

  // Multiple keys with different sizes and deleted flags.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest, defaultHash, defaultVersion};
    auto deleted = false;
    for (auto i = 1u; i <= maxNumKeys; ++i) {
      node.keys.emplace(getSliverOfSize(i), block::detail::KeyData{deleted});
      deleted = !deleted;
    }
    const auto nodeSliver = block::detail::createNode(node);
    ASSERT_EQ(deserializeStateRootVersion(nodeSliver), defaultVersion);
  }
}

TEST(block, raw_block_merkle_data_equality) {
  // Default-constructed ones are equal.
  {
    const auto d1 = block::detail::RawBlockMerkleData{};
    const auto d2 = block::detail::RawBlockMerkleData{};
    ASSERT_TRUE(d1 == d2);
  }

  // Hash-only - equal.
  {
    const auto d1 = block::detail::RawBlockMerkleData{defaultHash};
    const auto d2 = block::detail::RawBlockMerkleData{defaultHash};
    ASSERT_TRUE(d1 == d2);
  }

  // Hash-only - not equal.
  {
    const auto d1 = block::detail::RawBlockMerkleData{getHash("1")};
    const auto d2 = block::detail::RawBlockMerkleData{getHash("2")};
    ASSERT_FALSE(d1 == d2);
  }

  // Equal hash and keys.
  {
    const auto keys = OrderedKeysSet{Key{"k1"}, Key{"k2"}};
    const auto d1 = block::detail::RawBlockMerkleData{defaultHash, keys};
    const auto d2 = block::detail::RawBlockMerkleData{defaultHash, keys};
    ASSERT_TRUE(d1 == d2);
  }

  // Same hash, different keys.
  {
    const auto keys1 = OrderedKeysSet{Key{"k1"}, Key{"k2"}};
    const auto keys2 = OrderedKeysSet{Key{"k1"}};
    const auto d1 = block::detail::RawBlockMerkleData{defaultHash, keys1};
    const auto d2 = block::detail::RawBlockMerkleData{defaultHash, keys2};
    ASSERT_FALSE(d1 == d2);
  }

  // Different hash, same keys.
  {
    const auto keys = OrderedKeysSet{Key{"k1"}, Key{"k2"}};
    const auto d1 = block::detail::RawBlockMerkleData{getHash("1"), keys};
    const auto d2 = block::detail::RawBlockMerkleData{getHash("2"), keys};
    ASSERT_FALSE(d1 == d2);
  }
}

TEST(block, raw_block_merkle_data_serialization) {
  // No keys.
  {
    const auto data = block::detail::RawBlockMerkleData{defaultHash};
    const auto dataSliver = Sliver{serialize(data)};
    ASSERT_EQ(deserialize<block::detail::RawBlockMerkleData>(dataSliver), data);
  }

  // Non-empty keys.
  {
    const auto data = block::detail::RawBlockMerkleData{defaultHash, OrderedKeysSet{Key{"k1"}, Key{"k2"}}};
    const auto dataSliver = Sliver{serialize(data)};
    ASSERT_EQ(deserialize<block::detail::RawBlockMerkleData>(dataSliver), data);
  }

  // With empty keys.
  {
    const auto data = block::detail::RawBlockMerkleData{defaultHash, OrderedKeysSet{Key{"k1"}, Key{}, Key{"k3"}}};
    const auto dataSliver = Sliver{serialize(data)};
    ASSERT_EQ(deserialize<block::detail::RawBlockMerkleData>(dataSliver), data);
  }

  // Different key sizes.
  {
    const auto data =
        block::detail::RawBlockMerkleData{defaultHash, OrderedKeysSet{Key{"k1"}, Key{"key12"}, Key{"333333"}}};
    const auto dataSliver = Sliver{serialize(data)};
    ASSERT_EQ(deserialize<block::detail::RawBlockMerkleData>(dataSliver), data);
  }
}

TEST(block, block_serialization) {
  SetOfKeyValuePairs updates;
  for (auto i = 1u; i <= maxNumKeys; ++i) {
    updates.emplace(getSliverOfSize(i), getSliverOfSize(i * 10));
  }

  const auto block = block::detail::create(updates, defaultDigest, defaultHash);
  const auto parsedUpdates = block::detail::getData(block);
  const auto parsedDigest = block::detail::getParentDigest(block);
  const auto parsedStateHash = block::detail::getStateHash(block);

  ASSERT_TRUE(updates == parsedUpdates);
  ASSERT_TRUE(parsedDigest == defaultDigest);
  ASSERT_TRUE(defaultHash == parsedStateHash);
}

// The serialization test doesn't take into account if the test nodes are semantically valid. Instead, it is only
// focused on serialization/deserialization.
TEST(batched_internal, serialization) {
  // No children.
  {
    const auto node = BatchedInternalNode{};
    const auto buf = Sliver{serialize(node)};
    ASSERT_TRUE(buf.empty());
    ASSERT_TRUE(deserialize<BatchedInternalNode>(buf) == node);
  }

  // Full container with LeafChild children.
  {
    auto children = BatchedInternalNode::Children{};
    for (auto &child : children) {
      child = LeafChild{getHash("LeafChild"), LeafKey{getHash("LeafKey"), defaultBlockId}};
    }

    const auto node = BatchedInternalNode{children};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // Full container with InternalChild children.
  {
    auto children = BatchedInternalNode::Children{};
    auto count = 0;
    for (auto &child : children) {
      child = InternalChild{getHash("InternalChild"), defaultBlockId + count};
      ++count;
    }

    const auto node = BatchedInternalNode{children};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // Full container with alternating children.
  {
    auto children = BatchedInternalNode::Children{};
    auto internal = false;
    auto count = 0;
    for (auto &child : children) {
      if (internal) {
        child = InternalChild{getHash("InternalChild" + std::to_string(count)), defaultBlockId + count};
      } else {
        child = LeafChild{getHash("LeafChild" + std::to_string(count)),
                          LeafKey{getHash("LeafKey" + std::to_string(count)), defaultBlockId + count}};
      }
      ++count;
    }

    const auto node = BatchedInternalNode{children};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // 1 LeafChild at the beginning.
  {
    auto children = BatchedInternalNode::Children{};
    children[0] = LeafChild{getHash("LeafChild"), LeafKey{getHash("LeafKey"), defaultBlockId}};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // 1 LeafChild at the end.
  {
    auto children = BatchedInternalNode::Children{};
    children[children.size() - 1] = LeafChild{getHash("LeafChild"), LeafKey{getHash("LeafKey"), defaultBlockId}};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // 1 InternalChild at the beginning.
  {
    auto children = BatchedInternalNode::Children{};
    children[0] = InternalChild{getHash("InternalChild"), defaultBlockId};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // 1 InternalChild at the end.
  {
    auto children = BatchedInternalNode::Children{};
    children[children.size() - 1] = InternalChild{getHash("InternalChild"), defaultBlockId};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // 1 LeafChild at the beginning and one InternalChild at the end.
  {
    auto children = BatchedInternalNode::Children{};
    children[0] = LeafChild{getHash("LeafChild"), LeafKey{getHash("LeafKey"), defaultBlockId}};
    children[children.size() - 1] = InternalChild{getHash("InternalChild"), defaultBlockId};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // 1 InternalChild at the beginning and one LeafChild at the end.
  {
    auto children = BatchedInternalNode::Children{};
    children[0] = InternalChild{getHash("InternalChild"), defaultBlockId};
    children[children.size() - 1] = LeafChild{getHash("LeafChild"), LeafKey{getHash("LeafKey"), defaultBlockId}};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // Children in the middle.
  {
    auto children = BatchedInternalNode::Children{};
    children[15] = InternalChild{getHash("InternalChild1"), defaultBlockId};
    children[16] = LeafChild{getHash("LeafChild1"), LeafKey{getHash("LeafKey1"), defaultBlockId}};
    children[17] = InternalChild{getHash("InternalChild2"), defaultBlockId};
    children[18] = LeafChild{getHash("LeafChild2"), LeafKey{getHash("LeafKey2"), defaultBlockId}};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }

  // Children at the beginning and end.
  {
    auto children = BatchedInternalNode::Children{};
    children[0] = InternalChild{getHash("InternalChild1"), defaultBlockId + 1};
    children[1] = LeafChild{getHash("LeafChild1"), LeafKey{getHash("LeafKey1"), defaultBlockId + 2}};
    children[2] = InternalChild{getHash("InternalChild2"), defaultBlockId + 3};
    children[3] = LeafChild{getHash("LeafChild2"), LeafKey{getHash("LeafKey2"), defaultBlockId + 4}};

    children[30] = InternalChild{getHash("InternalChild3"), defaultBlockId + 5};
    children[29] = LeafChild{getHash("LeafChild3"), LeafKey{getHash("LeafKey3"), defaultBlockId + 6}};
    children[28] = InternalChild{getHash("InternalChild4"), defaultBlockId + 7};
    children[27] = LeafChild{getHash("LeafChild4"), LeafKey{getHash("LeafKey4"), defaultBlockId + 8}};
    const auto node = BatchedInternalNode{};
    ASSERT_TRUE(deserialize<BatchedInternalNode>(serialize(node)) == node);
  }
}

TEST(database_leaf_value, equality) {
  // Default-constructed leaf values are equal
  {
    const auto v1 = detail::DatabaseLeafValue{};
    const auto v2 = detail::DatabaseLeafValue{};
    ASSERT_TRUE(v1 == v2);
  }

  // Non-deleted equal leaf values.
  {
    const auto v1 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}};
    const auto v2 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}};
    ASSERT_TRUE(v1 == v2);
  }

  // Non-deleted leaf values that differ in the block ID only.
  {
    const auto v1 = detail::DatabaseLeafValue{defaultBlockId + 1, LeafNode{defaultSliver}};
    const auto v2 = detail::DatabaseLeafValue{defaultBlockId + 2, LeafNode{defaultSliver}};
    ASSERT_FALSE(v1 == v2);
  }

  // Non-deleted leaf values that differ in the value only.
  {
    const auto v1 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{Sliver{"v1"}}};
    const auto v2 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{Sliver{"v2"}}};
    ASSERT_FALSE(v1 == v2);
  }

  // Deleted equal leaf values.
  {
    const auto v1 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}, defaultBlockId};
    const auto v2 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}, defaultBlockId};
    ASSERT_TRUE(v1 == v2);
  }

  // Deleted and non-deleted leaf values are not equal.
  {
    const auto v1 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}};
    const auto v2 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}, defaultBlockId};
    ASSERT_FALSE(v1 == v2);
  }

  // Deleted in different blocks are not equal.
  {
    const auto v1 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}, defaultBlockId + 1};
    const auto v2 = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}, defaultBlockId + 2};
    ASSERT_FALSE(v1 == v2);
  }
}

TEST(database_leaf_value, serialization) {
  // Non-deleted with a non-empty value.
  {
    const auto dbLeafVal = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}};
    ASSERT_TRUE(deserialize<detail::DatabaseLeafValue>(serialize(dbLeafVal)) == dbLeafVal);
  }

  // Non-deleted with an empty value.
  {
    const auto dbLeafVal = detail::DatabaseLeafValue{defaultBlockId, LeafNode{Sliver{}}};
    ASSERT_TRUE(deserialize<detail::DatabaseLeafValue>(serialize(dbLeafVal)) == dbLeafVal);
  }

  // Deleted with a non-empty value.
  {
    const auto dbLeafVal = detail::DatabaseLeafValue{defaultBlockId, LeafNode{defaultSliver}, defaultBlockId + 1};
    ASSERT_TRUE(deserialize<detail::DatabaseLeafValue>(serialize(dbLeafVal)) == dbLeafVal);
  }

  // Deleted with an empty value.
  {
    const auto dbLeafVal = detail::DatabaseLeafValue{defaultBlockId, LeafNode{Sliver{}}, defaultBlockId + 1};
    ASSERT_TRUE(deserialize<detail::DatabaseLeafValue>(serialize(dbLeafVal)) == dbLeafVal);
  }
}

}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
