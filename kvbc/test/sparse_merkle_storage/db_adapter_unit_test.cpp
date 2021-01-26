// Copyright 2020 VMware, all rights reserved
//
// Test merkle tree DB adapter and key manipulation code.

#include "gtest/gtest.h"

#include "kvbc_storage_test_common.h"

#include "block_digest.h"
#include "merkle_tree_block.h"
#include "merkle_tree_db_adapter.h"
#include "merkle_tree_key_manipulator.h"
#include "memorydb/client.h"
#include "rocksdb/client.h"
#include "sparse_merkle/base_types.h"
#include "storage/db_types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using namespace ::concord::kvbc::v2MerkleTree;
using namespace ::concord::kvbc::v2MerkleTree::detail;
using namespace ::concord::storage::v2MerkleTree::detail;

using ::concord::storage::IDBClient;
using ::concord::kvbc::BlockDigest;
using ::concord::kvbc::NotFoundException;
using ::concord::kvbc::sparse_merkle::Hash;
using ::concord::kvbc::sparse_merkle::InternalNodeKey;
using ::concord::kvbc::sparse_merkle::Version;

using ::concord::kvbc::BlockId;
using ::concord::kvbc::OrderedKeysSet;
using ::concord::kvbc::SetOfKeyValuePairs;
using ::concord::kvbc::ValuesVector;
using ::concordUtils::Sliver;

// Make sure key updates and deletes partially overlap - see getDeterministicBlockDeletes() .
SetOfKeyValuePairs getDeterministicBlockUpdates(std::uint32_t count) {
  auto updates = SetOfKeyValuePairs{};
  for (auto i = 0u; i < count; ++i) {
    updates[getSliverOfSize((i + 1) * 2)] = getSliverOfSize((i + 1) * 3);
  }
  return updates;
}

OrderedKeysSet getDeterministicBlockDeletes(std::uint32_t count) {
  auto deletes = OrderedKeysSet{};
  for (auto i = 0u; i < count; ++i) {
    deletes.insert(getSliverOfSize((i + 1) * 4));
  }
  return deletes;
}

SetOfKeyValuePairs getOffsetUpdates(std::uint32_t base, std::uint32_t offset) {
  return SetOfKeyValuePairs{
      std::make_pair(Sliver{std::to_string(base)}, Sliver{"val" + std::to_string(base)}),
      std::make_pair(Sliver{std::to_string(offset + base)}, Sliver{"val" + std::to_string(offset + base)})};
}

enum class ReferenceBlockchainType {
  WithEmptyBlocks,
  NoEmptyBlocks,
  WithEmptyBlocksAndKeyDeletes,
};

ValuesVector createReferenceBlockchain(const std::shared_ptr<IDBClient> &db,
                                       std::size_t length,
                                       ReferenceBlockchainType type) {
  auto adapter = DBAdapter{db};
  ValuesVector blockchain;

  auto emptyToggle = true;
  for (auto i = 1u; i <= length; ++i) {
    auto deletes = OrderedKeysSet{};
    if (type == ReferenceBlockchainType::WithEmptyBlocksAndKeyDeletes) {
      deletes = getDeterministicBlockDeletes(i * 2);
    }

    if ((type == ReferenceBlockchainType::WithEmptyBlocks ||
         type == ReferenceBlockchainType::WithEmptyBlocksAndKeyDeletes) &&
        emptyToggle) {
      adapter.addBlock(SetOfKeyValuePairs{}, deletes);
    } else {
      adapter.addBlock(getDeterministicBlockUpdates(i * 2), deletes);
    }
    emptyToggle = !emptyToggle;

    const auto rawBlock = adapter.getRawBlock(i);
    blockchain.push_back(rawBlock);
  }

  return blockchain;
}

bool hasProvableStaleIndexKeysSince(const std::shared_ptr<IDBClient> &db, const Version &version) {
  auto iter = db->getIteratorGuard();
  const auto key = iter->seekAtLeast(DBKeyManipulator::genStaleDbKey(version)).first;
  if (!key.empty() && DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key &&
      DBKeyManipulator::getKeySubtype(key) == EKeySubtype::ProvableStale &&
      DBKeyManipulator::extractVersionFromProvableStaleKey(key) == version) {
    return true;
  }
  return false;
}

bool hasInternalKeysForVersion(const std::shared_ptr<IDBClient> &db, const Version &version) {
  auto iter = db->getIteratorGuard();
  const auto key = iter->seekAtLeast(DBKeyManipulator::genInternalDbKey(InternalNodeKey::root(version))).first;
  if (!key.empty() && DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key &&
      DBKeyManipulator::getKeySubtype(key) == EKeySubtype::Internal &&
      DBKeyManipulator::extractVersionFromInternalKey(key) == version) {
    return true;
  }
  return false;
}

bool hasNonProvableStaleIndexKeysSince(const std::shared_ptr<IDBClient> &db, BlockId blockId) {
  auto iter = db->getIteratorGuard();
  const auto key = iter->seekAtLeast(DBKeyManipulator::genNonProvableStaleDbKey(Sliver{}, blockId)).first;
  if (!key.empty() && DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key &&
      DBKeyManipulator::getKeySubtype(key) == EKeySubtype::NonProvableStale &&
      DBKeyManipulator::extractBlockIdFromNonProvableStaleKey(key) == blockId) {
    return true;
  }
  return false;
}

const auto zeroDigest = BlockDigest{};

struct IDbAdapterTest {
  virtual std::shared_ptr<IDBClient> newEmptyDb() const = 0;
  virtual std::string type() const = 0;
  virtual ValuesVector referenceBlockchain(const std::shared_ptr<IDBClient> &db, std::size_t length) const = 0;
  virtual ~IDbAdapterTest() noexcept = default;
};

template <typename Database, ReferenceBlockchainType refBlockchainType = ReferenceBlockchainType::NoEmptyBlocks>
struct DbAdapterTest : public IDbAdapterTest {
  std::shared_ptr<IDBClient> newEmptyDb() const override {
    Database::cleanup();
    return Database::create();
  }

  std::string type() const override {
    auto blocksType = std::string{};
    switch (refBlockchainType) {
      case ReferenceBlockchainType::WithEmptyBlocks:
        blocksType = "withEmptyBlocks";
        break;
      case ReferenceBlockchainType::NoEmptyBlocks:
        blocksType = "noEmptyBlocks";
        break;
      case ReferenceBlockchainType::WithEmptyBlocksAndKeyDeletes:
        blocksType = "withEmptyBlocksAndKeyDeletes";
        break;
    }
    return Database::type() + '_' + blocksType;
  }

  ValuesVector referenceBlockchain(const std::shared_ptr<IDBClient> &db, std::size_t length) const override {
    return createReferenceBlockchain(db, length, refBlockchainType);
  }
};

using db_adapter_custom_blockchain = ParametrizedTest<std::shared_ptr<IDbAdapterTest>>;
using db_adapter_ref_blockchain = ParametrizedTest<std::shared_ptr<IDbAdapterTest>>;

// Test the last reachable block functionality.
TEST_P(db_adapter_custom_blockchain, get_last_reachable_block) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto updates = SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)};
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_EQ(adapter.getLastReachableBlockId(), 3);
}

// Test the return value from the addBlock() method.
TEST_P(db_adapter_custom_blockchain, add_block_return) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto updates = SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)};
  const auto numBlocks = 4u;
  for (auto i = 0u; i < numBlocks; ++i) {
    ASSERT_EQ(adapter.addBlock(updates), i + 1);
  }
}

TEST_P(db_adapter_custom_blockchain, add_block_with_provable_and_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet non_provable_key_set = {skey1, skey2};
  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, non_provable_key_set};
  ASSERT_THROW(adapter.getValue(skey1, 4), NotFoundException);

  const auto provableKey = defaultSliver;
  const auto provableValue = defaultSliver;
  const auto nonProvablevalue = Sliver{"nonProvableValue"};
  const auto updates =
      SetOfKeyValuePairs{{provableKey, provableValue}, {skey1, nonProvablevalue}, {skey2, nonProvablevalue}};
  const auto numBlocks = 4u;
  for (auto i = 0u; i < numBlocks; ++i) {
    ASSERT_EQ(adapter.addBlock(updates), i + 1);
  }

  for (const auto &k : non_provable_key_set) {
    const auto [value, blockId] = adapter.getValue(k, numBlocks);
    ASSERT_EQ(value, nonProvablevalue);
    ASSERT_EQ(blockId, numBlocks);
  }

  const auto [value, blockId] = adapter.getValue(provableKey, numBlocks);
  ASSERT_EQ(value, provableValue);
  ASSERT_EQ(blockId, numBlocks);
}

TEST_P(db_adapter_custom_blockchain, add_block_only_with_non_provable_keys) {
  const Sliver skey1{"skey1"};
  const Sliver skey2{"skey2"};
  const Sliver skey3{"skey3"};

  const Sliver svalue1{"svalue1"};
  const Sliver svalue2{"svalue2"};
  const Sliver svalue3{"svalue3"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet non_provable_key_set = {skey1, skey2, skey3};
  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, non_provable_key_set};

  const auto initialTreeVersion = adapter.getStateVersion();
  const auto initialHash = adapter.getStateHash();

  ASSERT_THROW(adapter.getValue(skey1, 4), NotFoundException);

  const std::vector<SetOfKeyValuePairs> updates = {{{skey1, svalue1}}, {{skey2, svalue2}}, {{skey3, svalue3}}};
  for (size_t i = 0u; i < updates.size(); ++i) {
    ASSERT_EQ(adapter.addBlock(updates[i]), i + 1);
  }

  // Make sure getValue throws when block id is incorrect
  for (size_t i = 0u; i < updates.size(); ++i) {
    ASSERT_THROW(adapter.getValue(updates[i].begin()->first, i), NotFoundException);
  }

  // Make sure getValue returns correct value when the exact block id is provided
  for (size_t i = 0u; i < updates.size(); ++i) {
    const auto &[value, blockId] = adapter.getValue(updates[i].begin()->first, i + 1);
    ASSERT_EQ(blockId, i + 1);
    ASSERT_EQ(value, updates[i].begin()->second);
  }

  // Make sure getValue returns correct value when last reachable block id is provided
  for (size_t i = 0u; i < updates.size(); ++i) {
    const auto &[value, blockId] = adapter.getValue(updates[i].begin()->first, adapter.getLastReachableBlockId());
    ASSERT_EQ(blockId, i + 1);
    ASSERT_EQ(value, updates[i].begin()->second);
  }

  ASSERT_THROW(adapter.getValue(Sliver{"non-exisiting-key"}, adapter.getLastReachableBlockId()), NotFoundException);

  // Make sure that blocks with non-provable keys does not affect the Merkle Tree state
  ASSERT_EQ(initialTreeVersion, adapter.getStateVersion());
  ASSERT_EQ(initialHash, adapter.getStateHash());

  // Make sure that adding blocks with provable keys affects the Merkle Tree state
  adapter.addBlock(SetOfKeyValuePairs{{Sliver{"provable-key"}, Sliver{"provable-value"}}});
  ASSERT_EQ(initialTreeVersion + 1, adapter.getStateVersion());
  ASSERT_NE(initialHash, adapter.getStateHash());
}

TEST_P(db_adapter_custom_blockchain, raw_block_from_db_before_non_provable_keys) {
  const Sliver skey1{"skey1"};
  const Sliver skey2{"skey2"};

  const Sliver svalue1{"svalue1"};
  const Sliver svalue2{"svalue2"};

  const Sliver key1{"key1"};
  const Sliver key2{"key2"};

  const Sliver value1{"value1"};
  const Sliver value2{"value2"};

  const auto updates = SetOfKeyValuePairs{{skey1, svalue1}, {skey2, svalue2}, {key1, value1}, {key2, value2}};
  auto db = GetParam()->newEmptyDb();
  {
    auto adapter = DBAdapter{db, true};
    adapter.addBlock(updates);
  }
  {
    auto adapter = DBAdapter{db, true, {skey1, skey2}};
    ASSERT_EQ(block::detail::getData(adapter.getRawBlock(1)), updates);
  }
}

TEST_P(db_adapter_custom_blockchain, raw_block_with_non_provable_and_provable_keys) {
  const Sliver skey1{"skey1"};
  const Sliver skey2{"skey2"};
  const Sliver skey3{"skey3"};

  const Sliver svalue1{"svalue1"};
  const Sliver svalue2{"svalue2"};
  const Sliver svalue3{"svalue3"};

  const Sliver key1{"key1"};
  const Sliver key2{"key2"};
  const Sliver key3{"key3"};

  const Sliver value1{"value1"};
  const Sliver value2{"value2"};
  const Sliver value3{"value3"};

  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, {skey1, skey2, skey3}};

  std::vector<SetOfKeyValuePairs> updates = {
      {{skey1, svalue1}}, {{skey2, svalue2}}, {{skey3, svalue3}}, {{key1, value1}}, {{key2, value2}}, {{key3, value3}}};
  for (const auto &u : updates) {
    adapter.addBlock(u);
  }

  for (size_t i = 0u; i < updates.size(); ++i) {
    ASSERT_EQ(block::detail::getData(adapter.getRawBlock(i + 1)), updates[i]);
  }
}

TEST_P(db_adapter_custom_blockchain, raw_block_only_with_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  const Sliver skey3{"key3"};

  const Sliver svalue1{"value1"};
  const Sliver svalue2{"value2"};
  const Sliver svalue3{"value3"};

  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, {skey1, skey2, skey3}};

  std::vector<SetOfKeyValuePairs> updates = {{{skey1, svalue1}}, {{skey2, svalue2}}, {{skey3, svalue3}}};
  for (const auto &u : updates) {
    adapter.addBlock(u);
  }

  for (size_t i = 0u; i < updates.size(); ++i) {
    ASSERT_EQ(block::detail::getData(adapter.getRawBlock(i + 1)), updates[i]);
  }
}

TEST_P(db_adapter_custom_blockchain, raw_block_only_with_provable_keys) {
  const Sliver key1{"key1"};
  const Sliver key2{"key2"};
  const Sliver key3{"key3"};

  const Sliver value1{"value1"};
  const Sliver value2{"value2"};
  const Sliver value3{"value3"};

  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, {Sliver{"non-provable-key"}}};

  std::vector<SetOfKeyValuePairs> updates = {{{key1, value1}}, {{key2, value2}}, {{key3, value3}}};
  for (const auto &u : updates) {
    adapter.addBlock(u);
  }

  for (size_t i = 0u; i < updates.size(); ++i) {
    ASSERT_EQ(block::detail::getData(adapter.getRawBlock(i + 1)), updates[i]);
  }
}

TEST_P(db_adapter_custom_blockchain, add_block_throws_if_deletes_contain_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet non_provable_key_set = {skey1, skey2};
  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, non_provable_key_set};

  const auto updates = SetOfKeyValuePairs{{skey1, defaultSliver}, {skey2, defaultSliver}};
  const auto deletes = OrderedKeysSet{skey1};
  ASSERT_THROW(adapter.addBlock(updates, deletes), std::runtime_error);
}

TEST_P(db_adapter_custom_blockchain, getValue_finds_overlapping_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet non_provable_key_set = {skey1, skey2};

  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, non_provable_key_set};
  const auto numBlocks = 10u;
  for (size_t i = 0u; i < numBlocks; ++i) {
    adapter.addBlock(SetOfKeyValuePairs{{skey1, Sliver{"value1" + std::to_string(i)}},
                                        {skey2, Sliver{"value2" + std::to_string(i)}}});
  }
  for (size_t i = 0u; i < numBlocks; ++i) {
    {
      const auto &[value, blockId] = adapter.getValue(skey1, i + 1);
      ASSERT_EQ(value, Sliver{"value1" + std::to_string(i)});
      ASSERT_EQ(blockId, i + 1);
    }
    {
      const auto &[value, blockId] = adapter.getValue(skey2, i + 1);
      ASSERT_EQ(value, Sliver{"value2" + std::to_string(i)});
      ASSERT_EQ(blockId, i + 1);
    }
  }

  // Make sure that when the last reachable block id is provided, the most updated value is returned
  {
    const auto &[value, blockId] = adapter.getValue(skey1, adapter.getLastReachableBlockId());
    ASSERT_EQ(blockId, numBlocks);
    ASSERT_EQ(value, Sliver{"value1" + std::to_string(numBlocks - 1)});
  }
  {
    const auto &[value, blockId] = adapter.getValue(skey2, adapter.getLastReachableBlockId());
    ASSERT_EQ(blockId, numBlocks);
    ASSERT_EQ(value, Sliver{"value2" + std::to_string(numBlocks - 1)});
  }
}

TEST_P(db_adapter_custom_blockchain, getValue_finds_single_versioned_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  const Sliver svalue1{"value1"};
  const Sliver svalue2{"value2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet non_provable_key_set = {skey1, skey2};

  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, non_provable_key_set};
  adapter.addBlock(SetOfKeyValuePairs{{skey1, svalue1}});
  adapter.addBlock(SetOfKeyValuePairs{{skey2, svalue2}});

  {
    const auto &[value, blockId] = adapter.getValue(skey1, 1);
    ASSERT_EQ(value, svalue1);
    ASSERT_EQ(blockId, 1);
  }
  {
    const auto &[value, blockId] = adapter.getValue(skey2, 2);
    ASSERT_EQ(value, svalue2);
    ASSERT_EQ(blockId, 2);
  }
  {
    const auto &[value, blockId] = adapter.getValue(skey1, 2);
    ASSERT_EQ(value, svalue1);
    ASSERT_EQ(blockId, 1);
  }
  ASSERT_THROW(adapter.getValue(skey2, 1), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, getValue_finds_non_provable_keys_in_bc_with_mixed_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  const Sliver svalue1{"value1"};
  const Sliver svalue2{"value2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet non_provable_key_set = {skey1, skey2};

  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, non_provable_key_set};
  const size_t blockNum = 5u;
  for (size_t i = 0u; i < blockNum; ++i) {
    const SetOfKeyValuePairs updates{{Sliver{std::to_string(i)}, Sliver{std::to_string(i)}}};
    adapter.addBlock(updates);
  }
  adapter.addBlock(SetOfKeyValuePairs{{skey1, svalue1}});
  adapter.addBlock(SetOfKeyValuePairs{{skey2, svalue2}});

  for (size_t i = 0u; i < blockNum; ++i) {
    const auto &[v, b] = adapter.getValue(Sliver{std::to_string(i)}, i + 1);
    ASSERT_EQ(v, Sliver{std::to_string(i)});
    ASSERT_EQ(b, i + 1);
  }
  {
    const auto &[v, b] = adapter.getValue(skey1, blockNum + 1);
    ASSERT_EQ(v, svalue1);
    ASSERT_EQ(b, blockNum + 1);
  }
  {
    const auto &[v, b] = adapter.getValue(skey2, blockNum + 2);
    ASSERT_EQ(v, svalue2);
    ASSERT_EQ(b, blockNum + 2);
  }
  // Test that getValue throws in case the block id is wrong
  ASSERT_THROW(adapter.getValue(skey1, 1), NotFoundException);
  ASSERT_THROW(adapter.getValue(skey2, 1), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, getValue_finds_non_provable_keys_added_before_the_feature_was_introduced) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};

  const Sliver svalue1{"value1"};
  const Sliver svalue2{"value2"};
  const auto updates = SetOfKeyValuePairs{{skey1, svalue1}, {skey2, svalue2}};
  auto db = GetParam()->newEmptyDb();
  {
    auto adapter = DBAdapter{db, true};
    adapter.addBlock(updates);
  }
  {
    auto adapter = DBAdapter{db, true, {skey1, skey2}};
    for (const auto &[expectedKey, expectedValue] : updates) {
      const auto &[actualValue, actualBlockId] = adapter.getValue(expectedKey, adapter.getLastReachableBlockId());
      ASSERT_EQ(actualValue, expectedValue);
      ASSERT_EQ(actualBlockId, adapter.getLastReachableBlockId());
    }
  }
}

// Test the hasBlock() method.
TEST_P(db_adapter_custom_blockchain, has_block) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Verify that blocks do not exist when the blockchain is empty.
  ASSERT_FALSE(adapter.hasBlock(1));
  ASSERT_FALSE(adapter.hasBlock(2));
  ASSERT_FALSE(adapter.hasBlock(8));
  ASSERT_FALSE(adapter.hasBlock(9));

  // Verify that adding last reachable blocks leads to existent blocks.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}), 1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}), 2);
  ASSERT_TRUE(adapter.hasBlock(1));
  ASSERT_TRUE(adapter.hasBlock(2));

  // Verify that adding state transfer blocks leads to existent blocks.
  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}, BlockDigest{}, Hash{}),
      8));
  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}, BlockDigest{}, Hash{}),
      9));
  ASSERT_TRUE(adapter.hasBlock(8));
  ASSERT_TRUE(adapter.hasBlock(9));
}

// Test the last reachable block functionality with empty blocks.
TEST_P(db_adapter_custom_blockchain, get_last_reachable_block_empty_blocks) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto updates = SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)};
  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
  ASSERT_EQ(adapter.getLastReachableBlockId(), 6);
}

TEST_P(db_adapter_custom_blockchain, get_key_by_ver_1_key) {
  const auto key = defaultSliver;
  const auto keyData = defaultData;
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto data1 = Sliver{"data1"};
  const auto data2 = Sliver{"data2"};
  const auto data3 = Sliver{"data3"};
  const auto updates1 = SetOfKeyValuePairs{std::make_pair(key, data1)};
  const auto updates2 = SetOfKeyValuePairs{std::make_pair(key, data2)};
  const auto updates3 = SetOfKeyValuePairs{std::make_pair(key, data3)};
  ASSERT_NO_THROW(adapter.addBlock(updates1));
  ASSERT_NO_THROW(adapter.addBlock(updates2));
  ASSERT_NO_THROW(adapter.addBlock(updates3));

  // Get a non-existent key with a hash that is after the existent key.
  {
    const auto after = Sliver{"dummy"};
    ASSERT_TRUE(getHash(keyData) < getHash(after));

    ASSERT_THROW(adapter.getValue(after, 1), NotFoundException);
  }

  // Get a non-existent key with a hash that is before the existent key.
  {
    const auto before = Sliver{"aa"};
    ASSERT_TRUE(getHash(before) < getHash(keyData));

    ASSERT_THROW(adapter.getValue(before, 1), NotFoundException);
  }

  // Get a key with a version smaller than the first version and expect an empty response.
  ASSERT_THROW(adapter.getValue(key, 0), NotFoundException);

  // Get the first one.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 1);
    ASSERT_TRUE(value == data1);
    ASSERT_EQ(actualVersion, 1);
  }

  // Get the second one.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 2);
    ASSERT_TRUE(value == data2);
    ASSERT_EQ(actualVersion, 2);
  }

  // Get the last one.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 3);
    ASSERT_TRUE(value == data3);
    ASSERT_EQ(actualVersion, 3);
  }

  // Get the key at a version bigger than the last one and expect the last one.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 42);
    ASSERT_TRUE(value == data3);
    ASSERT_EQ(actualVersion, 3);
  }
}

TEST_P(db_adapter_custom_blockchain, get_key_by_ver_1_key_empty_blocks) {
  const auto key = defaultSliver;
  const auto keyData = defaultData;
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto data2 = Sliver{"data2"};
  const auto updates2 = SetOfKeyValuePairs{std::make_pair(key, data2)};
  const auto data5 = Sliver{"data5"};
  const auto updates5 = SetOfKeyValuePairs{std::make_pair(key, data5)};

  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
  ASSERT_NO_THROW(adapter.addBlock(updates2));
  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
  ASSERT_NO_THROW(adapter.addBlock(updates5));
  ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));

  // Get a non-existent key with a hash that is after the existent key.
  {
    const auto after = Sliver{"dummy"};
    ASSERT_TRUE(getHash(keyData) < getHash(after));

    ASSERT_THROW(adapter.getValue(after, 1), NotFoundException);
  }

  // Get a non-existent key with a hash that is before the existent key.
  {
    const auto before = Sliver{"aa"};
    ASSERT_TRUE(getHash(before) < getHash(keyData));

    ASSERT_THROW(adapter.getValue(before, 1), NotFoundException);
  }

  // Get a key with a version smaller than the first version and expect an empty response.
  ASSERT_THROW(adapter.getValue(key, 0), NotFoundException);

  // Get the key at the first empty block and expect an empty response.
  ASSERT_THROW(adapter.getValue(key, 1), NotFoundException);

  // Get the key at the second block.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 2);
    ASSERT_TRUE(value == data2);
    ASSERT_EQ(actualVersion, 2);
  }

  // Get the key at the third empty block and expect at the second.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 3);
    ASSERT_TRUE(value == data2);
    ASSERT_EQ(actualVersion, 2);
  }

  // Get the key at the fourth empty block and expect at the second.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 4);
    ASSERT_TRUE(value == data2);
    ASSERT_EQ(actualVersion, 2);
  }

  // Get the key at the fifth block.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 5);
    ASSERT_TRUE(value == data5);
    ASSERT_EQ(actualVersion, 5);
  }

  // Get the key at the sixth empty block and expect at the fifth.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 6);
    ASSERT_TRUE(value == data5);
    ASSERT_EQ(actualVersion, 5);
  }

  // Get the key at a version bigger than the last one and expect at the last one.
  {
    const auto [value, actualVersion] = adapter.getValue(key, 42);
    ASSERT_TRUE(value == data5);
    ASSERT_EQ(actualVersion, 5);
  }
}

// Test the getKeyByReadVersion() method with multiple keys, including ones that are ordered before and after the keys
// in the system.
// Note: Leaf keys are ordered first on the key hash and then on the version. See db_types.h and
// merkle_tree_serialization.h for more information.
TEST_P(db_adapter_custom_blockchain, get_key_by_ver_multiple_keys) {
  const auto key = defaultSliver;
  const auto keyData = defaultData;
  const auto before = Sliver{"aa"};
  const auto after = Sliver{"dummy"};
  const auto random = Sliver{"random"};
  ASSERT_TRUE(getHash(keyData) < getHash(after));
  ASSERT_TRUE(getHash(before) < getHash(keyData));

  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto data1 = Sliver{"data1"};
  const auto data2 = Sliver{"data2"};
  const auto data3 = Sliver{"data3"};
  const auto updates1 = SetOfKeyValuePairs{std::make_pair(key, data1), std::make_pair(before, data2)};
  const auto updates2 =
      SetOfKeyValuePairs{std::make_pair(key, data2), std::make_pair(before, data1), std::make_pair(after, data3)};
  const auto updates3 = SetOfKeyValuePairs{std::make_pair(key, data3), std::make_pair(before, data2)};
  ASSERT_NO_THROW(adapter.addBlock(updates1));
  ASSERT_NO_THROW(adapter.addBlock(updates2));
  ASSERT_NO_THROW(adapter.addBlock(updates3));

  // Get keys with a version that is smaller than the first version and expect an empty response.
  {
    const auto version = 0;

    ASSERT_THROW(adapter.getValue(key, version), NotFoundException);

    ASSERT_THROW(adapter.getValue(before, version), NotFoundException);

    ASSERT_THROW(adapter.getValue(after, version), NotFoundException);

    ASSERT_THROW(adapter.getValue(random, version), NotFoundException);
  }

  // Get keys with a version that is bigger than the last version and expect the last one.
  {
    const auto version = 42;

    {
      const auto [value, actualVersion] = adapter.getValue(key, version);
      ASSERT_TRUE(value == data3);
      ASSERT_EQ(actualVersion, 3);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(before, version);
      ASSERT_TRUE(value == data2);
      ASSERT_EQ(actualVersion, 3);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(after, version);
      ASSERT_TRUE(value == data3);
      ASSERT_EQ(actualVersion, 2);
    }

    ASSERT_THROW(adapter.getValue(random, version), NotFoundException);
  }

  // Get keys at version 1.
  {
    const auto version = 1;

    {
      const auto [value, actualVersion] = adapter.getValue(key, version);
      ASSERT_TRUE(value == data1);
      ASSERT_EQ(actualVersion, 1);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(before, version);
      ASSERT_TRUE(value == data2);
      ASSERT_EQ(actualVersion, 1);
    }

    // The after key is not present at version 1.
    ASSERT_THROW(adapter.getValue(after, version), NotFoundException);

    // The random key is not present at version 1.
    ASSERT_THROW(adapter.getValue(random, version), NotFoundException);
  }

  // Get keys at version 2.
  {
    const auto version = 2;

    {
      const auto [value, actualVersion] = adapter.getValue(key, version);
      ASSERT_TRUE(value == data2);
      ASSERT_EQ(actualVersion, 2);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(before, version);
      ASSERT_TRUE(value == data1);
      ASSERT_EQ(actualVersion, 2);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(after, version);
      ASSERT_TRUE(value == data3);
      ASSERT_EQ(actualVersion, 2);
    }

    ASSERT_THROW(adapter.getValue(random, version), NotFoundException);
  }

  // Get keys at version 3.
  {
    const auto version = 3;

    {
      const auto [value, actualVersion] = adapter.getValue(key, version);
      ASSERT_TRUE(value == data3);
      ASSERT_EQ(actualVersion, 3);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(before, version);
      ASSERT_TRUE(value == data2);
      ASSERT_EQ(actualVersion, 3);
    }

    {
      const auto [value, actualVersion] = adapter.getValue(after, version);
      ASSERT_TRUE(value == data3);
      ASSERT_EQ(actualVersion, 2);
    }

    ASSERT_THROW(adapter.getValue(random, version), NotFoundException);
  }
}

TEST_P(db_adapter_custom_blockchain, add_and_get_block) {
  for (auto numKeys = 1u; numKeys <= maxNumKeys; ++numKeys) {
    auto adapter = DBAdapter{GetParam()->newEmptyDb()};

    SetOfKeyValuePairs updates1, updates2;
    for (auto i = 1u; i <= numKeys; ++i) {
      // Make the keys overlap between block1 and block2.
      updates1[getSliverOfSize(i)] = getSliverOfSize(i * 10);
      updates2[getSliverOfSize(i * 2)] = getSliverOfSize(i * 2 * 10);
    }

    // Add 2 blocks.
    {
      ASSERT_NO_THROW(adapter.addBlock(updates1));

      ASSERT_NO_THROW(adapter.addBlock(updates2));
    }

    // Try to get a non-existent block.
    ASSERT_THROW(adapter.getRawBlock(3), NotFoundException);

    // Get the first block.
    Sliver rawBlock1;
    {
      rawBlock1 = adapter.getRawBlock(1);
      ASSERT_TRUE(updates1 == block::detail::getData(rawBlock1));
    }

    // Get the second block.
    {
      const auto rawBlock2 = adapter.getRawBlock(2);
      ASSERT_TRUE(updates2 == block::detail::getData(rawBlock2));
      ASSERT_TRUE(adapter.getStateHash() == block::detail::getStateHash(rawBlock2));
      ASSERT_TRUE(block::detail::getParentDigest(rawBlock2) == blockDigest(1, rawBlock1));
    }
  }
}

TEST_P(db_adapter_ref_blockchain, add_multiple_deterministic_blocks) {
  const auto numBlocks = 16;
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->newEmptyDb(), numBlocks);
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  for (auto i = 1u; i <= numBlocks; ++i) {
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_NO_THROW(
        adapter.addBlock(block::detail::getData(referenceBlock), block::detail::getDeletedKeys(referenceBlock)));
    ASSERT_EQ(adapter.getLastReachableBlockId(), i);
  }

  for (auto i = 1u; i <= numBlocks; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    ASSERT_FALSE(rawBlock.empty());

    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(rawBlock == referenceBlock);

    // Expect a zero parent digest for block 1.
    if (i == 1) {
      ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == zeroDigest);
    } else {
      const auto rawParentBlock = adapter.getRawBlock(i - 1);
      ASSERT_FALSE(rawParentBlock.empty());
      ASSERT_TRUE(blockDigest(i - 1, rawParentBlock) == block::detail::getParentDigest(rawBlock));
    }

    ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
  }
}

TEST_P(db_adapter_custom_blockchain, no_blocks) {
  const auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  ASSERT_EQ(adapter.getLastReachableBlockId(), 0);
  ASSERT_EQ(adapter.getLatestBlockId(), 0);

  ASSERT_THROW(adapter.getRawBlock(defaultBlockId), NotFoundException);

  ASSERT_THROW(adapter.getValue(defaultSliver, defaultBlockId), NotFoundException);
}

TEST_P(db_adapter_ref_blockchain, state_transfer_reverse_order_with_blockchain_blocks) {
  const auto numBlockchainBlocks = 5;
  const auto numStBlocks = 7;
  const auto numTotalBlocks = numBlockchainBlocks + numStBlocks;
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->newEmptyDb(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_NO_THROW(
        adapter.addBlock(block::detail::getData(referenceBlock), block::detail::getDeletedKeys(referenceBlock)));
    ASSERT_EQ(adapter.getGenesisBlockId(), 1);
    ASSERT_EQ(adapter.getLatestBlockId(), i);
    ASSERT_EQ(adapter.getLastReachableBlockId(), i);
  }

  // Receive more blocks from state transfer and add them to the blockchain.
  for (auto i = numTotalBlocks; i > numBlockchainBlocks; --i) {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[i - 1], i));
    ASSERT_EQ(adapter.getLatestBlockId(), numTotalBlocks);
    ASSERT_EQ(adapter.getGenesisBlockId(), 1);
    if (i == numBlockchainBlocks + 1) {
      // We link the blockchain and state transfer chains at that point.
      ASSERT_EQ(adapter.getLastReachableBlockId(), numTotalBlocks);
      ASSERT_EQ(adapter.getLatestBlockId(), numTotalBlocks);
    } else {
      ASSERT_EQ(adapter.getLastReachableBlockId(), numBlockchainBlocks);
    }

    // Verify that initial blocks are accessible at all steps.
    for (auto j = 1; j <= numBlockchainBlocks; ++j) {
      const auto rawBlock = adapter.getRawBlock(j);
      const auto &referenceBlock = referenceBlockchain[j - 1];
      ASSERT_TRUE(rawBlock == referenceBlock);
      ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
      ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
      ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
      if (j > 1) {
        const auto &prevReferenceBlock = referenceBlockchain[j - 2];
        ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == blockDigest(j - 1, prevReferenceBlock));
      } else {
        ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == zeroDigest);
      }
    }
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= numTotalBlocks; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(rawBlock == referenceBlock);
    ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
    ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
    if (i > 1) {
      const auto &prevReferenceBlock = referenceBlockchain[i - 2];
      ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == blockDigest(i - 1, prevReferenceBlock));
    } else {
      ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == zeroDigest);
    }
  }
}

TEST_P(db_adapter_ref_blockchain, state_transfer_fetch_whole_blockchain_in_reverse_order) {
  const auto numBlocks = 7;
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->newEmptyDb(), numBlocks);

  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  for (auto i = numBlocks; i > 0; --i) {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[i - 1], i));
    ASSERT_EQ(adapter.getLatestBlockId(), numBlocks);
    if (i == 1) {
      // We link the blockchain and state transfer chains at that point.
      ASSERT_EQ(adapter.getLastReachableBlockId(), numBlocks);
    } else {
      ASSERT_EQ(adapter.getLastReachableBlockId(), 0);
    }
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= numBlocks; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(rawBlock == referenceBlock);
    ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
    ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
    if (i > 1) {
      const auto &prevReferenceBlock = referenceBlockchain[i - 2];
      ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == blockDigest(i - 1, prevReferenceBlock));
    } else {
      ASSERT_TRUE(block::detail::getParentDigest(rawBlock) == zeroDigest);
    }
  }
}

TEST_P(db_adapter_ref_blockchain, state_transfer_unordered_with_blockchain_blocks) {
  const auto numBlockchainBlocks = 5;
  const auto numStBlocks = 3;
  const auto numTotalBlocks = numBlockchainBlocks + numStBlocks;
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->newEmptyDb(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_NO_THROW(
        adapter.addBlock(block::detail::getData(referenceBlock), block::detail::getDeletedKeys(referenceBlock)));
    ASSERT_EQ(adapter.getLatestBlockId(), i);
    ASSERT_EQ(adapter.getLastReachableBlockId(), i);
  }

  // Add block 7.
  {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[6], 7));
    ASSERT_EQ(adapter.getLastReachableBlockId(), numBlockchainBlocks);
    ASSERT_EQ(adapter.getLatestBlockId(), 7);
    for (auto i = 1; i <= numBlockchainBlocks; ++i) {
      const auto rawBlock = adapter.getRawBlock(i);
      const auto &referenceBlock = referenceBlockchain[i - 1];
      ASSERT_TRUE(rawBlock == referenceBlock);
      ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
      ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
      ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
    }
  }

  // Add block 6.
  {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[5], 6));
    ASSERT_EQ(adapter.getLastReachableBlockId(), 7);
    ASSERT_EQ(adapter.getLatestBlockId(), 7);
    for (auto i = 1; i <= 7; ++i) {
      const auto rawBlock = adapter.getRawBlock(i);
      const auto &referenceBlock = referenceBlockchain[i - 1];
      ASSERT_TRUE(rawBlock == referenceBlock);
      ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
      ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
      ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
    }
  }

  // Add block 8.
  {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[7], 8));
    ASSERT_EQ(adapter.getLastReachableBlockId(), 8);
    ASSERT_EQ(adapter.getLatestBlockId(), 8);
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= numTotalBlocks; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(rawBlock == referenceBlock);
    ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
    ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
  }
}

TEST_P(db_adapter_ref_blockchain, partial_unordered_state_transfer) {
  const auto numBlockchainBlocks = 5;
  const auto numStBlocks = 3;
  const auto numTotalBlocks = numBlockchainBlocks + numStBlocks;
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->newEmptyDb(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_NO_THROW(
        adapter.addBlock(block::detail::getData(referenceBlock), block::detail::getDeletedKeys(referenceBlock)));
    ASSERT_EQ(adapter.getGenesisBlockId(), 1);
    ASSERT_EQ(adapter.getLatestBlockId(), i);
    ASSERT_EQ(adapter.getLastReachableBlockId(), i);
  }

  // Add block 8.
  {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[7], 8));
    ASSERT_EQ(adapter.getGenesisBlockId(), 1);
    ASSERT_EQ(adapter.getLastReachableBlockId(), numBlockchainBlocks);
    ASSERT_EQ(adapter.getLatestBlockId(), 8);
  }

  // Add block 6.
  {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[5], 6));
    ASSERT_EQ(adapter.getGenesisBlockId(), 1);
    ASSERT_EQ(adapter.getLastReachableBlockId(), 6);
    ASSERT_EQ(adapter.getLatestBlockId(), 8);
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= 6; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(rawBlock == referenceBlock);
    ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock) == block::detail::getDeletedKeys(referenceBlock));
    ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
  }
  const auto rawBlock8 = adapter.getRawBlock(8);
  const auto &referenceBlock8 = referenceBlockchain[8 - 1];
  ASSERT_TRUE(rawBlock8 == referenceBlock8);
  ASSERT_TRUE(block::detail::getData(rawBlock8) == block::detail::getData(referenceBlock8));
  ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock8) == block::detail::getDeletedKeys(referenceBlock8));
  ASSERT_TRUE(block::detail::getStateHash(rawBlock8) == block::detail::getStateHash(referenceBlock8));
}

TEST_P(db_adapter_custom_blockchain, delete_last_reachable_block) {
  const auto numBlocks = 10;
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add block updates with no overlapping keys between blocks.
  for (auto i = 1; i <= numBlocks - 1; ++i) {
    ASSERT_EQ(adapter.addBlock(getOffsetUpdates(i, numBlocks)), i);
  }

  // Save the DB updates for the last reachable block and add the block.
  const auto lastReachabeDbUpdates =
      adapter.lastReachableBlockDbUpdates(getOffsetUpdates(numBlocks, numBlocks), OrderedKeysSet{}, numBlocks);
  ASSERT_EQ(adapter.addBlock(getOffsetUpdates(numBlocks, numBlocks)), numBlocks);

  // Verify that all of the keys representing the last reachable block are present.
  for (const auto &kv : lastReachabeDbUpdates) {
    auto value = Sliver{};
    ASSERT_TRUE(adapter.getDb()->get(kv.first, value).isOK());
  }

  // Verify block IDs before deletion.
  ASSERT_EQ(adapter.getLastReachableBlockId(), numBlocks);
  ASSERT_EQ(adapter.getLatestBlockId(), numBlocks);

  // Delete the last reachable block.
  ASSERT_NO_THROW(adapter.deleteBlock(numBlocks));

  // Verify that all of the keys representing the last reachable block are deleted.
  for (const auto &kv : lastReachabeDbUpdates) {
    auto value = Sliver{};
    ASSERT_TRUE(adapter.getDb()->get(kv.first, value).isNotFound());
  }

  // Verify block IDs after deletion.
  ASSERT_EQ(adapter.getGenesisBlockId(), 1);
  ASSERT_EQ(adapter.getLastReachableBlockId(), numBlocks - 1);
  ASSERT_EQ(adapter.getLatestBlockId(), numBlocks - 1);

  // Make sure we cannot get the last block.
  ASSERT_FALSE(adapter.hasBlock(numBlocks));
  ASSERT_THROW(adapter.getRawBlock(numBlocks), NotFoundException);

  // Since there are no overlapping keys between blocks, we expect that all keys from the last update will be
  // non-existent.
  const auto lastBlockUpdates = getOffsetUpdates(numBlocks, numBlocks);
  for (const auto &kv : lastBlockUpdates) {
    ASSERT_THROW(adapter.getValue(kv.first, numBlocks), NotFoundException);
  }
}

TEST_P(db_adapter_custom_blockchain, delete_latest_block) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add a last reachable block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}), 1);

  // Add a temporary state transfer block.
  const auto stKey = Sliver{"stk"};
  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(stKey, defaultSliver)}, BlockDigest{}, Hash{}), 8));

  // Verify blocks before deletion.
  ASSERT_EQ(adapter.getLastReachableBlockId(), 1);
  ASSERT_EQ(adapter.getLatestBlockId(), 8);
  ASSERT_TRUE(adapter.hasBlock(1));
  ASSERT_TRUE(adapter.hasBlock(8));

  // Delete the latest block.
  ASSERT_NO_THROW(adapter.deleteBlock(8));

  // Verify blocks after deletion.
  ASSERT_EQ(adapter.getLastReachableBlockId(), 1);
  ASSERT_EQ(adapter.getLatestBlockId(), 1);
  ASSERT_TRUE(adapter.hasBlock(1));
  ASSERT_FALSE(adapter.hasBlock(8));
  ASSERT_THROW(adapter.getRawBlock(8), NotFoundException);
  ASSERT_THROW(adapter.getValue(stKey, 8), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, delete_non_existent_blocks) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add a last reachable block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}), 1);

  // Delete a non-existent block.
  ASSERT_NO_THROW(adapter.deleteBlock(3));

  // Make sure the last reachable block is available.
  ASSERT_TRUE(adapter.hasBlock(1));
  ASSERT_NO_THROW(adapter.getRawBlock(1));

  // Add a temporary state transfer block.
  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}, BlockDigest{}, Hash{}),
      8));

  // Delete a non-existent block before the latest ST block.
  ASSERT_NO_THROW(adapter.deleteBlock(3));

  // Delete a non-existent block after the latest ST block.
  ASSERT_NO_THROW(adapter.deleteBlock(11));

  // Make sure both blocks are available.
  ASSERT_TRUE(adapter.hasBlock(1));
  ASSERT_NO_THROW(adapter.getRawBlock(1));
  ASSERT_TRUE(adapter.hasBlock(8));
  ASSERT_NO_THROW(adapter.getRawBlock(8));
}

TEST_P(db_adapter_custom_blockchain, delete_on_an_empty_blockchain) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  ASSERT_NO_THROW(adapter.deleteBlock(0));
  ASSERT_NO_THROW(adapter.deleteBlock(1));
}

TEST_P(db_adapter_custom_blockchain, delete_block_0_on_non_empty_chain_is_not_an_error) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto key = Sliver{"k"};

  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v1"})}), 1);

  ASSERT_NO_THROW(adapter.deleteBlock(0));
}

TEST_P(db_adapter_custom_blockchain, delete_single_st_block) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}, BlockDigest{}, Hash{}),
      8));

  // Make sure that a single ST block can be deleted.
  ASSERT_NO_THROW(adapter.deleteBlock(8));
  ASSERT_FALSE(adapter.hasBlock(8));
  ASSERT_THROW(adapter.getRawBlock(8), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, get_genesis_block_id) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Empty blockchain.
  ASSERT_EQ(adapter.getGenesisBlockId(), 0);

  // Add a temporary state transfer block.
  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}, BlockDigest{}, Hash{}),
      8));

  // The blockchain is still empty, irrespective of the added state transfer block.
  ASSERT_EQ(adapter.getGenesisBlockId(), 0);

  // Add a last reachable block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}), 1);

  // Last reachable blocks are part of the blockchain and, therefore, getGenesisBlockId() takes them into account.
  ASSERT_EQ(adapter.getGenesisBlockId(), 1);
}

TEST_P(db_adapter_custom_blockchain, delete_genesis_block) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto key = Sliver{"k"};

  // Add blocks.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v1"})}), 1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v3"})}), 3);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 4);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v5"})}), 5);

  // Delete the genesis block.
  ASSERT_NO_THROW(adapter.deleteBlock(1));

  // Verify that the genesis block has been deleted.
  ASSERT_FALSE(adapter.hasBlock(1));
  ASSERT_THROW(adapter.getRawBlock(1), NotFoundException);
  ASSERT_EQ(adapter.getGenesisBlockId(), 2);
  ASSERT_EQ(adapter.getLastReachableBlockId(), 5);

  // Verify that keys from the deleted genesis block are available.
  const auto value = adapter.getValue(key, 2);
  ASSERT_EQ(value.second, 1);
  ASSERT_EQ(value.first, Sliver{"v1"});
}

TEST_P(db_adapter_custom_blockchain, block_ids_are_persisted_after_deletes) {
  auto db = GetParam()->newEmptyDb();
  auto adapter = std::make_unique<DBAdapter>(db);

  const auto key = Sliver{"k"};

  // Add blockchain blocks.
  ASSERT_EQ(adapter->addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v1"})}), 1);
  ASSERT_EQ(adapter->addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(adapter->addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v3"})}), 3);

  // Add ST temporary blocks.
  ASSERT_NO_THROW(adapter->addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v8"})}, BlockDigest{}, Hash{}), 8));
  ASSERT_NO_THROW(adapter->addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v9"})}, BlockDigest{}, Hash{}), 9));

  // Verify before deletion.
  ASSERT_EQ(adapter->getGenesisBlockId(), 1);
  ASSERT_EQ(adapter->getLastReachableBlockId(), 3);
  ASSERT_EQ(adapter->getLatestBlockId(), 9);

  // Delete genesis, last reachable and a ST temporary blocks.
  adapter->deleteBlock(1);  // genesis
  adapter->deleteBlock(3);  // last reachable
  adapter->deleteBlock(9);  // ST temp

  // Verify after deletion.
  ASSERT_EQ(adapter->getGenesisBlockId(), 2);
  ASSERT_EQ(adapter->getLastReachableBlockId(), 2);
  ASSERT_EQ(adapter->getLatestBlockId(), 8);

  // Create a new adapter instance, simulating a system restart, and verify.
  adapter.reset();
  adapter = std::make_unique<DBAdapter>(db);
  ASSERT_EQ(adapter->getGenesisBlockId(), 2);
  ASSERT_EQ(adapter->getLastReachableBlockId(), 2);
  ASSERT_EQ(adapter->getLatestBlockId(), 8);
}

TEST_P(db_adapter_custom_blockchain, get_value_from_deleted_block) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};
  const auto key = Sliver{"k"};

  // Add blocks.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v1"})}), 1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v2"})}), 2);

  // Delete the genesis block.
  ASSERT_NO_THROW(adapter.deleteBlock(1));

  // Verify that the key is not found at a deleted version.
  ASSERT_THROW(adapter.getValue(key, 1), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, delete_only_block_in_system) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  // Add a single block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(Sliver{"k"}, Sliver{"v1"})}), 1);

  // Expect an exception - cannot delete the only block in the system.
  ASSERT_THROW(adapter.deleteBlock(1), std::logic_error);

  // Verify that the block is still there.
  ASSERT_EQ(adapter.getGenesisBlockId(), 1);
  ASSERT_EQ(adapter.getLastReachableBlockId(), 1);
  ASSERT_TRUE(adapter.hasBlock(1));
}

TEST_P(db_adapter_custom_blockchain, delete_only_block_as_last_reachable) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key = Sliver{"k"};
  const auto valueBefore = Sliver{"vb"};
  const auto valueAfter = Sliver{"va"};
  const auto updatesBefore = SetOfKeyValuePairs{std::make_pair(key, valueBefore)};
  const auto updatesAfter = SetOfKeyValuePairs{std::make_pair(key, valueAfter)};

  // Doesn't throw on an empty blockchain.
  ASSERT_NO_THROW(adapter.deleteLastReachableBlock());

  // Add a single block.
  ASSERT_EQ(adapter.addBlock(updatesBefore), 1);
  ASSERT_EQ(adapter.getLastReachableBlockId(), 1);
  ASSERT_EQ(adapter.getLatestBlockId(), 1);
  ASSERT_EQ(adapter.getGenesisBlockId(), 1);

  // Delete the last reachable block.
  adapter.deleteLastReachableBlock();

  // Verify it has been deleted.
  ASSERT_THROW(adapter.getRawBlock(1), NotFoundException);
  ASSERT_THROW(adapter.getValue(key, 1), NotFoundException);
  ASSERT_EQ(adapter.getLastReachableBlockId(), 0);
  ASSERT_EQ(adapter.getLatestBlockId(), 0);
  ASSERT_EQ(adapter.getGenesisBlockId(), 0);

  // Verify we can add block 1 again. Use different values.
  ASSERT_EQ(adapter.addBlock(updatesAfter), 1);
  ASSERT_EQ(adapter.getLastReachableBlockId(), 1);
  ASSERT_EQ(adapter.getLatestBlockId(), 1);
  ASSERT_EQ(adapter.getGenesisBlockId(), 1);
  ASSERT_NO_THROW(adapter.getRawBlock(1));
  const auto [foundValue, version] = adapter.getValue(key, 1);
  ASSERT_EQ(foundValue, valueAfter);
  ASSERT_EQ(version, 1);
}

TEST_P(db_adapter_custom_blockchain, delete_only_block_with_id_gt_1_as_last_reachable) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key1 = Sliver{"k1"};
  const auto key2 = Sliver{"k2"};

  // Add 2 blocks
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key1, Sliver{"v1_1"})}), 1);
  ASSERT_EQ(
      adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key1, Sliver{"v1_2"}), std::make_pair(key2, Sliver{"v2_2"})}),
      2);

  // Delete block 1 as genesis.
  ASSERT_NO_THROW(adapter.deleteBlock(1));

  // Verify last/latest/genesis block ID is 2.
  ASSERT_EQ(adapter.getLastReachableBlockId(), 2);
  ASSERT_EQ(adapter.getLatestBlockId(), 2);
  ASSERT_EQ(adapter.getGenesisBlockId(), 2);

  // Delete the last block 2 as last reachable.
  ASSERT_NO_THROW(adapter.deleteLastReachableBlock());

  // Verify last/latest/genesis block ID is 0.
  ASSERT_EQ(adapter.getLastReachableBlockId(), 0);
  ASSERT_EQ(adapter.getLatestBlockId(), 0);
  ASSERT_EQ(adapter.getGenesisBlockId(), 0);

  // Make sure no keys are available at block 1 and block 2 as:
  // * block 2 updates key1 and then block 1 is pruned
  // * block 2 is deleted as a last reachable one and all its keys are deleted
  ASSERT_THROW(adapter.getValue(key1, 1), NotFoundException);
  ASSERT_THROW(adapter.getValue(key1, 2), NotFoundException);
  ASSERT_THROW(adapter.getValue(key2, 2), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, delete_multiple_genesis_blocks) {
  const auto numBlocks = 10;
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  for (auto i = 1; i <= numBlocks; ++i) {
    ASSERT_EQ(adapter.addBlock(getOffsetUpdates(i, numBlocks)), i);
  }

  for (auto i = 1; i <= numBlocks - 1; ++i) {
    const auto updates = getOffsetUpdates(i, numBlocks);
    ASSERT_NO_THROW(adapter.deleteBlock(i));
    ASSERT_FALSE(adapter.hasBlock(i));
    ASSERT_THROW(adapter.getRawBlock(i), NotFoundException);
    ASSERT_THROW(adapter.getValue(updates.begin()->first, i), NotFoundException);
    ASSERT_THROW(adapter.getValue(updates.begin()->second, i), NotFoundException);
    ASSERT_EQ(adapter.getLastReachableBlockId(), numBlocks);
    ASSERT_EQ(adapter.getLatestBlockId(), numBlocks);
    ASSERT_EQ(adapter.getGenesisBlockId(), i + 1);
  }

  const auto lastBlockUpdates = getOffsetUpdates(numBlocks, numBlocks);
  for (const auto &[updateKey, updateValue] : lastBlockUpdates) {
    const auto [actualValue, actualBlockVersion] = adapter.getValue(updateKey, numBlocks);
    ASSERT_TRUE(actualValue == updateValue);
    ASSERT_EQ(actualBlockVersion, numBlocks);
  }
  ASSERT_TRUE(adapter.getBlockData(adapter.getRawBlock(numBlocks)) == lastBlockUpdates);
}

TEST_P(db_adapter_custom_blockchain, delete_multiple_last_reachable_blocks) {
  const auto numBlocks = 10;
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  for (auto i = 1; i <= numBlocks; ++i) {
    ASSERT_EQ(adapter.addBlock(getOffsetUpdates(i, numBlocks)), i);
  }

  for (auto i = numBlocks; i > 1; --i) {
    const auto updates = getOffsetUpdates(i, numBlocks);
    ASSERT_NO_THROW(adapter.deleteBlock(i));
    ASSERT_FALSE(adapter.hasBlock(i));
    ASSERT_THROW(adapter.getRawBlock(i), NotFoundException);
    ASSERT_THROW(adapter.getValue(updates.begin()->first, i), NotFoundException);
    ASSERT_THROW(adapter.getValue(updates.begin()->second, i), NotFoundException);
    ASSERT_EQ(adapter.getLastReachableBlockId(), i - 1);
    ASSERT_EQ(adapter.getLatestBlockId(), i - 1);
    ASSERT_EQ(adapter.getGenesisBlockId(), 1);
  }

  const auto genesisBlockUpdates = getOffsetUpdates(1, numBlocks);
  for (const auto &[updateKey, updateValue] : genesisBlockUpdates) {
    const auto [actualValue, actualBlockVersion] = adapter.getValue(updateKey, 1);
    ASSERT_TRUE(actualValue == updateValue);
    ASSERT_EQ(actualBlockVersion, 1);
  }
  ASSERT_TRUE(adapter.getBlockData(adapter.getRawBlock(1)) == genesisBlockUpdates);
}

TEST_P(db_adapter_custom_blockchain, delete_genesis_and_verify_stale_index) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key = Sliver{"k"};

  // Add blocks with an overlapping key. Include an empty block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v1"})}), 1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v3"})}), 3);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v4"})}), 4);

  // Verify there are stale keys in the index since version 2 of the tree (block 2 doesn't update the tree version).
  ASSERT_TRUE(hasProvableStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete the genesis block.
  ASSERT_NO_THROW(adapter.deleteBlock(1));

  // Verify there are stale keys in the index since tree version 2.
  ASSERT_TRUE(hasProvableStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete the genesis block again.
  ASSERT_NO_THROW(adapter.deleteBlock(2));

  // Verify there are stale keys in the index since tree version 2.
  ASSERT_TRUE(hasProvableStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete the genesis block again.
  ASSERT_NO_THROW(adapter.deleteBlock(3));

  // Verify there are no stale keys in the index since tree version 2.
  ASSERT_FALSE(hasProvableStaleIndexKeysSince(adapter.getDb(), 2));
}

TEST_P(db_adapter_custom_blockchain, delete_genesis_and_verify_stale_key_deletion_with_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet nonProvableKeySet = {skey1, skey2};
  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, nonProvableKeySet};

  // Add blocks with overlapping keys. Include an empty block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{

                std::make_pair(skey1, Sliver{"sk1v1"}), std::make_pair(skey2, Sliver{"sk2v1"})}),
            1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{

                std::make_pair(skey1, Sliver{"sk1v2"}), std::make_pair(skey2, Sliver{"sk2v2"})}),
            3);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{

                std::make_pair(skey1, Sliver{"sk1v3"}), std::make_pair(skey2, Sliver{"sk2v3"})}),
            4);

  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey2)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey2)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(4, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(4, skey2)).isOK());

  // Make sure the are non-provable stale keys for blocks 3, 4 before deletion.
  ASSERT_TRUE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 3));
  ASSERT_TRUE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 4));

  // Make sure the are no non-provable stale keys for blocks 1, 2 before deletion.
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 1));
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete genesis blocks until the stale version 1 keys are to be deleted.
  ASSERT_NO_THROW(adapter.deleteBlock(1));
  ASSERT_NO_THROW(adapter.deleteBlock(2));
  ASSERT_NO_THROW(adapter.deleteBlock(3));

  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey1)).isNotFound());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey2)).isNotFound());

  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey2)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(4, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(4, skey2)).isOK());

  // Make sure non-provable stale keys for blocks that have been deleted
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 3));
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 2));
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 1));

  ASSERT_TRUE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 4));
}

TEST_P(db_adapter_custom_blockchain, provide_non_provable_keys_of_different_size) {
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet nonProvableKeySet = {Sliver{"k"}, Sliver{"kk"}};
  ASSERT_THROW(DBAdapter(GetParam()->newEmptyDb(), true, nonProvableKeySet), std::runtime_error);
}

TEST_P(db_adapter_custom_blockchain, delete_blocks_with_non_provable_keys) {
  const Sliver skey1{"key1"};
  const Sliver skey2{"key2"};
  concord::kvbc::v2MerkleTree::DBAdapter::NonProvableKeySet nonProvableKeySet = {skey1, skey2};
  auto adapter = DBAdapter{GetParam()->newEmptyDb(), true, nonProvableKeySet};

  ASSERT_EQ(adapter.addBlock(
                SetOfKeyValuePairs{std::make_pair(skey1, Sliver{"sk1v1"}), std::make_pair(skey2, Sliver{"sk2v1"})}),
            1);

  ASSERT_EQ(adapter.addBlock(
                SetOfKeyValuePairs{std::make_pair(skey1, Sliver{"sk1v2"}), std::make_pair(skey2, Sliver{"sk2v2"})}),
            2);

  ASSERT_EQ(adapter.addBlock(
                SetOfKeyValuePairs{std::make_pair(skey1, Sliver{"sk1v3"}), std::make_pair(skey2, Sliver{"sk2v3"})}),
            3);

  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 1));
  ASSERT_TRUE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 2));
  ASSERT_TRUE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 3));

  adapter.deleteBlock(3);
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 1));
  ASSERT_TRUE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 2));
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 3));

  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey2)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(2, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(2, skey2)).isOK());
  ASSERT_FALSE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey1)).isOK());
  ASSERT_FALSE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey2)).isOK());

  adapter.deleteBlock(2);
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 1));
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 2));
  ASSERT_FALSE(hasNonProvableStaleIndexKeysSince(adapter.getDb(), 3));

  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey1)).isOK());
  ASSERT_TRUE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(1, skey2)).isOK());
  ASSERT_FALSE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(2, skey1)).isOK());
  ASSERT_FALSE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(2, skey2)).isOK());
  ASSERT_FALSE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey1)).isOK());
  ASSERT_FALSE(adapter.getDb()->has(DBKeyManipulator::genNonProvableDbKey(3, skey2)).isOK());
}

TEST_P(db_adapter_custom_blockchain, delete_genesis_and_verify_stale_key_deletion) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key1 = Sliver{"k1"};
  const auto key2 = Sliver{"k2"};

  const auto ver1StaleLeafKey1 = DBKeyManipulator::genDataDbKey(key1, 1);
  const auto ver1StaleLeafKey2 = DBKeyManipulator::genDataDbKey(key2, 1);

  // Add blocks with overlapping keys. Include an empty block.
  ASSERT_EQ(
      adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key1, Sliver{"k1v1"}), std::make_pair(key2, Sliver{"k2v1"})}),
      1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(
      adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key1, Sliver{"k1v3"}), std::make_pair(key2, Sliver{"k2v3"})}),
      3);
  ASSERT_EQ(
      adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key1, Sliver{"k1v4"}), std::make_pair(key2, Sliver{"k2v4"})}),
      4);

  // Verify that the leaf key for key1 at version 1 is available.
  ASSERT_TRUE(adapter.getDb()->has(ver1StaleLeafKey1).isOK());

  // Verify that the leaf key for key2 at version 1 is available.
  ASSERT_TRUE(adapter.getDb()->has(ver1StaleLeafKey2).isOK());

  // Make sure the are internal keys for tree version 1 before deletion.
  ASSERT_TRUE(hasInternalKeysForVersion(adapter.getDb(), 1));

  // Delete genesis blocks until the stale version 1 keys are to be deleted.
  ASSERT_NO_THROW(adapter.deleteBlock(1));
  ASSERT_NO_THROW(adapter.deleteBlock(2));
  ASSERT_NO_THROW(adapter.deleteBlock(3));

  // Verify that the leaf key for key1 at version 1 is deleted.
  ASSERT_TRUE(adapter.getDb()->has(ver1StaleLeafKey1).isNotFound());

  // Verify that the leaf key for key2 at version 1 is deleted.
  ASSERT_TRUE(adapter.getDb()->has(ver1StaleLeafKey2).isNotFound());

  // Make sure internal keys for tree version 1 have been deleted
  ASSERT_FALSE(hasInternalKeysForVersion(adapter.getDb(), 1));
}

TEST_P(db_adapter_custom_blockchain, empty_values) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key1 = Sliver{"k1"};
  const auto key2 = Sliver{"k2"};
  const auto key3 = Sliver{"k3"};

  // Empty value in the middle.
  {
    const auto updates = SetOfKeyValuePairs{
        std::make_pair(key1, Sliver{"k1v1"}), std::make_pair(key2, Sliver{}), std::make_pair(key3, Sliver{"k3v3"})};
    ASSERT_EQ(adapter.addBlock(updates), 1);

    const auto key2Value = adapter.getValue(key2, 1);
    ASSERT_TRUE(key2Value.first.empty());
    ASSERT_EQ(key2Value.second, 1);

    const auto rawBlock = adapter.getRawBlock(1);
    ASSERT_EQ(block::detail::getData(rawBlock), updates);
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock).empty());
  }

  // Empty value at the start and at the end.
  {
    const auto updates = SetOfKeyValuePairs{
        std::make_pair(key1, Sliver{}), std::make_pair(key2, Sliver{"k2v2"}), std::make_pair(key3, Sliver{})};
    ASSERT_EQ(adapter.addBlock(updates), 2);

    {
      const auto [key1Value, key1Version] = adapter.getValue(key1, 2);
      ASSERT_TRUE(key1Value.empty());
      ASSERT_EQ(key1Version, 2);
    }

    {
      const auto [key3Value, key3Version] = adapter.getValue(key3, 2);
      ASSERT_TRUE(key3Value.empty());
      ASSERT_EQ(key3Version, 2);
    }

    const auto rawBlock = adapter.getRawBlock(2);
    ASSERT_EQ(block::detail::getData(rawBlock), updates);
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock).empty());
  }
}

TEST_P(db_adapter_custom_blockchain, delete_all_keys) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key1 = Sliver{"key1"};
  const auto value11 = Sliver{"val11"};
  const auto value12 = Sliver{"val12"};
  const auto key2 = Sliver{"key2"};
  const auto value21 = Sliver{"val21"};
  const auto value22 = Sliver{"val22"};

  const auto updates1 = SetOfKeyValuePairs{std::make_pair(key1, value11), std::make_pair(key2, value12)};
  const auto deletes = OrderedKeysSet{key1, key2};

  ASSERT_EQ(adapter.addBlock(updates1), 1);
  ASSERT_EQ(Version{1}, adapter.getStateVersion());

  const auto stateHashAtBlock1 = adapter.getStateHash();

  // Make sure that deleting keys will change the state version and hash.
  ASSERT_EQ(adapter.addBlock(deletes), 2);
  ASSERT_EQ(Version{2}, adapter.getStateVersion());
  ASSERT_NE(stateHashAtBlock1, adapter.getStateHash());

  // Make sure that there are internal keys for version 2 (i.e. an empty root).
  ASSERT_TRUE(hasInternalKeysForVersion(adapter.getDb(), 2));

  // Make sure the block reflects the deletes.
  const auto rawBlock = adapter.getRawBlock(2);
  ASSERT_TRUE(block::detail::getData(rawBlock).empty());
  ASSERT_EQ(block::detail::getDeletedKeys(rawBlock), deletes);

  // Make sure keys are not available at block version 2.
  ASSERT_THROW(adapter.getValue(key1, 2), NotFoundException);
  ASSERT_THROW(adapter.getValue(key2, 2), NotFoundException);

  // Add a block after having deleted all keys in the system.
  const auto updates2 = SetOfKeyValuePairs{std::make_pair(key1, value21), std::make_pair(key2, value22)};
  ASSERT_EQ(adapter.addBlock(updates2), 3);

  // Make sure keys are still not available at block version 2.
  ASSERT_THROW(adapter.getValue(key1, 2), NotFoundException);
  ASSERT_THROW(adapter.getValue(key2, 2), NotFoundException);

  // Make sure keys are available at block version 3.
  const auto [key1Value, key1Version] = adapter.getValue(key1, 3);
  ASSERT_EQ(key1Value, value21);
  ASSERT_EQ(key1Version, 3);
  const auto [key2Value, key2Version] = adapter.getValue(key2, 3);
  ASSERT_EQ(key2Value, value22);
  ASSERT_EQ(key2Version, 3);

  // Make sure state version is 3 and state hash is different from the one at block version 1.
  ASSERT_NE(stateHashAtBlock1, adapter.getStateHash());
  ASSERT_EQ(Version{3}, adapter.getStateVersion());

  // Make sure we have stale index keys since version 3, i.e. the empty root should be stale now.
  ASSERT_TRUE(hasProvableStaleIndexKeysSince(adapter.getDb(), 3));
}

TEST_P(db_adapter_custom_blockchain, add_delete_add_key) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key = Sliver{"key"};
  const auto value = Sliver{"val"};

  const auto updates = SetOfKeyValuePairs{std::make_pair(key, value)};
  const auto deletes = OrderedKeysSet{key};

  // Add, delete and add again.
  ASSERT_EQ(adapter.addBlock(updates), 1);
  const auto stateHashAtBlock1 = adapter.getStateHash();
  ASSERT_EQ(adapter.addBlock(deletes), 2);
  ASSERT_EQ(adapter.addBlock(updates), 3);

  // Make sure that the state hash at version 3 is the same as the one at verison 1.
  ASSERT_EQ(Version{3}, adapter.getStateVersion());
  ASSERT_EQ(stateHashAtBlock1, adapter.getStateHash());
}

TEST_P(db_adapter_custom_blockchain, delete_existing_keys) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key = Sliver{"key"};
  const auto value = Sliver{"val"};

  // Add a key in block 1 and delete it in block 4.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, value)}), 1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(Sliver{"some_other_key"}, Sliver{"other_value"})}), 3);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}, OrderedKeysSet{key}), 4);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 5);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(Sliver{"some_other_key2"}, Sliver{"other_value2"})}), 6);

  // Verify that the key is persent until deleted.
  for (auto i = 1; i < 4; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock).empty());

    const auto [keyValue, keyVersion] = adapter.getValue(key, i);
    ASSERT_EQ(keyValue, value);
    ASSERT_EQ(keyVersion, 1);
  }

  // Verify that the key is deleted at block 4.
  {
    const auto rawBlock4 = adapter.getRawBlock(4);
    ASSERT_TRUE(block::detail::getData(rawBlock4).empty());
    const auto deletedKeys4 = block::detail::getDeletedKeys(rawBlock4);
    ASSERT_EQ(deletedKeys4.size(), 1);
    ASSERT_EQ(*std::cbegin(deletedKeys4), key);

    ASSERT_THROW(adapter.getValue(key, 4), NotFoundException);
  }

  // Verify that the key is not present after being deleted.
  for (auto i = 5; i <= 6; ++i) {
    const auto rawBlock = adapter.getRawBlock(i);
    ASSERT_TRUE(block::detail::getDeletedKeys(rawBlock).empty());

    ASSERT_THROW(adapter.getValue(key, i), NotFoundException);
  }

  // Verify there are stale keys in the index since version 3 of the tree (block 2 doesn't update the tree version
  // and, therefore, the tree version at block 4 is 3).
  ASSERT_TRUE(hasProvableStaleIndexKeysSince(adapter.getDb(), 3));
}

TEST_P(db_adapter_custom_blockchain, delete_non_existing_keys) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto existingKey = Sliver{"existingKey"};
  const auto existingValue = Sliver{"existingValue"};
  const auto nonExistingKey = Sliver{"nonExistingKey"};

  // Add the existing key.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(existingKey, existingValue)}), 1);

  // Add an empty block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);

  // Add a block that deletes a non-existing key.
  ASSERT_EQ(adapter.addBlock(OrderedKeysSet{nonExistingKey}), 3);

  // Make sure the existing key can be fetched from version 3 and is found at version 1.
  {
    const auto [value, version] = adapter.getValue(existingKey, 3);
    ASSERT_EQ(value, existingValue);
    ASSERT_EQ(version, 1);
  }

  // Make sure blocks 2 and 3 are empty.
  {
    const auto block2 = adapter.getRawBlock(2);
    ASSERT_TRUE(block::detail::getData(block2).empty());
    ASSERT_TRUE(block::detail::getDeletedKeys(block2).empty());

    const auto block3 = adapter.getRawBlock(3);
    ASSERT_TRUE(block::detail::getData(block3).empty());
    ASSERT_TRUE(block::detail::getDeletedKeys(block3).empty());
  }

  // Verify that block 2 and block 3 haven't updated the state version as they are empty.
  ASSERT_EQ(adapter.getStateVersion(), 1);
}

TEST_P(db_adapter_custom_blockchain, update_and_delete_same_keys) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key1 = Sliver{"key1"};
  const auto value1 = Sliver{"val1"};
  const auto key2 = Sliver{"key2"};
  const auto value2 = Sliver{"val2"};

  const auto updates = SetOfKeyValuePairs{std::make_pair(key1, value1), std::make_pair(key2, value2)};
  const auto deletes = OrderedKeysSet{key1, key2};

  ASSERT_EQ(adapter.addBlock(updates, deletes), 1);

  {
    const auto [key1Value, key1Version] = adapter.getValue(key1, 1);
    ASSERT_EQ(key1Value, value1);
    ASSERT_EQ(key1Version, 1);
  }

  {
    const auto [key2Value, key2Version] = adapter.getValue(key2, 1);
    ASSERT_EQ(key2Value, value2);
    ASSERT_EQ(key2Version, 1);
  }
}

TEST_P(db_adapter_custom_blockchain, delete_blocks_with_deleted_keys) {
  auto adapter = DBAdapter{GetParam()->newEmptyDb()};

  const auto key1 = Sliver{"key1"};
  const auto value1 = Sliver{"val1"};
  const auto key2 = Sliver{"key2"};
  const auto value2 = Sliver{"val2"};

  // Add 2 keys in block 1.
  const auto updates = SetOfKeyValuePairs{std::make_pair(key1, value1), std::make_pair(key2, value2)};
  ASSERT_EQ(adapter.addBlock(updates), 1);

  // Delete the 2 keys in block 2.
  const auto deletes = OrderedKeysSet{key1, key2};
  ASSERT_EQ(adapter.addBlock(deletes), 2);

  // Add an empty block 3.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 3);

  // Delete block 1 and 2.
  adapter.deleteBlock(1);
  adapter.deleteBlock(2);

  // Block 3 exists.
  const auto block3 = adapter.getRawBlock(3);
  ASSERT_FALSE(block3.empty());
  ASSERT_TRUE(block::detail::getData(block3).empty());
  ASSERT_TRUE(block::detail::getDeletedKeys(block3).empty());

  // Cannot get the values at block 2 and 3.
  ASSERT_THROW(adapter.getValue(key1, 2), NotFoundException);
  ASSERT_THROW(adapter.getValue(key1, 3), NotFoundException);
  ASSERT_THROW(adapter.getValue(key2, 2), NotFoundException);
  ASSERT_THROW(adapter.getValue(key2, 3), NotFoundException);
}

#ifdef USE_ROCKSDB
const auto customBlockchainTestsParams =
    ::testing::Values(std::make_shared<DbAdapterTest<TestMemoryDb>>(), std::make_shared<DbAdapterTest<TestRocksDb>>());

const auto refBlockchainTestParams = ::testing::Values(
    std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::NoEmptyBlocks>>(),
    std::make_shared<DbAdapterTest<TestRocksDb, ReferenceBlockchainType::NoEmptyBlocks>>(),
    std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::WithEmptyBlocks>>(),
    std::make_shared<DbAdapterTest<TestRocksDb, ReferenceBlockchainType::WithEmptyBlocks>>(),
    std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::WithEmptyBlocksAndKeyDeletes>>(),
    std::make_shared<DbAdapterTest<TestRocksDb, ReferenceBlockchainType::WithEmptyBlocksAndKeyDeletes>>());
#else
const auto customBlockchainTestsParams = ::testing::Values(std::make_shared<DbAdapterTest<TestMemoryDb>>());

const auto refBlockchainTestParams = ::testing::Values(
    std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::NoEmptyBlocks>>(),
    std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::WithEmptyBlocks>>(),
    std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::WithEmptyBlocksAndKeyDeletes>>());
#endif

// Instantiate tests with memorydb and RocksDB clients and with custom (test-specific) blockchains.
INSTANTIATE_TEST_CASE_P(db_adapter_tests_custom_blockchain,
                        db_adapter_custom_blockchain,
                        customBlockchainTestsParams,
                        TypePrinter{});

// Instantiate tests with memorydb and RocksDB clients and with reference blockchains (with and without empty blocks).
INSTANTIATE_TEST_CASE_P(db_adapter_tests_ref_blockchain,
                        db_adapter_ref_blockchain,
                        refBlockchainTestParams,
                        TypePrinter{});

}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
