// Copyright 2020 VMware, all rights reserved
//
// Test merkle tree DB adapter and key manipulation code.

#include "gtest/gtest.h"

#include "storage_test_common.h"

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
using ::concord::kvbc::SetOfKeyValuePairs;
using ::concord::kvbc::ValuesVector;
using ::concordUtils::Sliver;
using ::concordUtils::Status;

SetOfKeyValuePairs getDeterministicBlockUpdates(std::uint32_t count) {
  auto updates = SetOfKeyValuePairs{};
  auto size = std::uint8_t{0};
  for (auto i = 0u; i < count; ++i) {
    updates[getSliverOfSize(size + 1)] = getSliverOfSize((size + 1) * 3);
    ++size;
  }
  return updates;
}

SetOfKeyValuePairs getOffsetUpdates(std::uint32_t base, std::uint32_t offset) {
  return SetOfKeyValuePairs{
      std::make_pair(Sliver{std::to_string(base)}, Sliver{"val" + std::to_string(base)}),
      std::make_pair(Sliver{std::to_string(offset + base)}, Sliver{"val" + std::to_string(offset + base)})};
}

enum class ReferenceBlockchainType {
  WithEmptyBlocks,
  NoEmptyBlocks,
};

ValuesVector createReferenceBlockchain(const std::shared_ptr<IDBClient> &db,
                                       std::size_t length,
                                       ReferenceBlockchainType type) {
  auto adapter = DBAdapter{db};
  ValuesVector blockchain;

  auto emptyToggle = true;
  for (auto i = 1u; i <= length; ++i) {
    if (type == ReferenceBlockchainType::WithEmptyBlocks && emptyToggle) {
      adapter.addBlock(SetOfKeyValuePairs{});
    } else {
      adapter.addBlock(getDeterministicBlockUpdates(i * 2));
    }
    emptyToggle = !emptyToggle;

    const auto rawBlock = adapter.getRawBlock(i);
    blockchain.push_back(rawBlock);
  }

  return blockchain;
}

bool hasStaleIndexKeysSince(const std::shared_ptr<IDBClient> &db, const Version &version) {
  auto iter = db->getIteratorGuard();
  const auto key = iter->seekAtLeast(DBKeyManipulator::genStaleDbKey(version)).first;
  if (!key.empty() && DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key &&
      DBKeyManipulator::getKeySubtype(key) == EKeySubtype::Stale &&
      DBKeyManipulator::extractVersionFromStaleKey(key) == version) {
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

const auto zeroDigest = BlockDigest{};

struct IDbAdapterTest {
  virtual std::shared_ptr<IDBClient> db() const = 0;
  virtual std::string type() const = 0;
  virtual ValuesVector referenceBlockchain(const std::shared_ptr<IDBClient> &db, std::size_t length) const = 0;
  virtual ~IDbAdapterTest() noexcept = default;
};

template <typename Database, ReferenceBlockchainType refBlockchainType = ReferenceBlockchainType::NoEmptyBlocks>
struct DbAdapterTest : public IDbAdapterTest {
  std::shared_ptr<IDBClient> db() const override { return Database::create(); }

  std::string type() const override {
    const auto blocksType = refBlockchainType == ReferenceBlockchainType::WithEmptyBlocks
                                ? std::string{"withEmptyBlocks"}
                                : std::string{"noEmptyBlocks"};
    return Database::type() + '_' + blocksType;
  }

  ValuesVector referenceBlockchain(const std::shared_ptr<IDBClient> &db, std::size_t length) const override {
    return createReferenceBlockchain(db, length, refBlockchainType);
  }
};

using db_adapter_custom_blockchain = ParametrizedTest<std::shared_ptr<IDbAdapterTest>>;
using db_adapter_ref_blockchain = ParametrizedTest<std::shared_ptr<IDbAdapterTest>>;

// Test that empty values in blocks are not allowed.
TEST_P(db_adapter_custom_blockchain, reject_empty_values) {
  auto adapter = DBAdapter{GetParam()->db()};
  const auto updates =
      SetOfKeyValuePairs{std::make_pair(Sliver{"k1"}, defaultSliver), std::make_pair(Sliver{"k2"}, Sliver{})};
  ASSERT_THROW(adapter.addBlock(updates), std::invalid_argument);
}

// Test the last reachable block functionality.
TEST_P(db_adapter_custom_blockchain, get_last_reachable_block) {
  auto adapter = DBAdapter{GetParam()->db()};
  const auto updates = SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)};
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_NO_THROW(adapter.addBlock(updates));
  ASSERT_EQ(adapter.getLastReachableBlockId(), 3);
}

// Test the return value from the addBlock() method.
TEST_P(db_adapter_custom_blockchain, add_block_return) {
  auto adapter = DBAdapter{GetParam()->db()};
  const auto updates = SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)};
  const auto numBlocks = 4u;
  for (auto i = 0u; i < numBlocks; ++i) {
    ASSERT_EQ(adapter.addBlock(updates), i + 1);
  }
}

// Test the hasBlock() method.
TEST_P(db_adapter_custom_blockchain, has_block) {
  auto adapter = DBAdapter{GetParam()->db()};

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
  auto adapter = DBAdapter{GetParam()->db()};
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
  auto adapter = DBAdapter{GetParam()->db()};
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
  auto adapter = DBAdapter{GetParam()->db()};
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

  auto adapter = DBAdapter{GetParam()->db()};
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
    auto adapter = DBAdapter{GetParam()->db()};

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
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->db(), numBlocks);
  auto adapter = DBAdapter{GetParam()->db()};
  for (auto i = 1u; i <= numBlocks; ++i) {
    ASSERT_NO_THROW(adapter.addBlock(block::detail::getData(referenceBlockchain[i - 1])));
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
  }
}

TEST_P(db_adapter_custom_blockchain, no_blocks) {
  const auto adapter = DBAdapter{GetParam()->db()};

  ASSERT_EQ(adapter.getLastReachableBlockId(), 0);
  ASSERT_EQ(adapter.getLatestBlockId(), 0);

  ASSERT_THROW(adapter.getRawBlock(defaultBlockId), NotFoundException);

  ASSERT_THROW(adapter.getValue(defaultSliver, defaultBlockId), NotFoundException);
}

TEST_P(db_adapter_ref_blockchain, state_transfer_reverse_order_with_blockchain_blocks) {
  const auto numBlockchainBlocks = 5;
  const auto numStBlocks = 7;
  const auto numTotalBlocks = numBlockchainBlocks + numStBlocks;
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->db(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->db()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    ASSERT_NO_THROW(adapter.addBlock(block::detail::getData(referenceBlockchain[i - 1])));
    ASSERT_EQ(adapter.getLatestBlockId(), i);
    ASSERT_EQ(adapter.getLastReachableBlockId(), i);
  }

  // Receive more blocks from state transfer and add them to the blockchain.
  for (auto i = numTotalBlocks; i > numBlockchainBlocks; --i) {
    ASSERT_NO_THROW(adapter.addRawBlock(referenceBlockchain[i - 1], i));
    ASSERT_EQ(adapter.getLatestBlockId(), numTotalBlocks);
    if (i == numBlockchainBlocks + 1) {
      // We link the blockchain and state transfer chains at that point.
      ASSERT_EQ(adapter.getLastReachableBlockId(), numTotalBlocks);
    } else {
      ASSERT_EQ(adapter.getLastReachableBlockId(), numBlockchainBlocks);
    }

    // Verify that initial blocks are accessible at all steps.
    for (auto j = 1; j <= numBlockchainBlocks; ++j) {
      const auto rawBlock = adapter.getRawBlock(j);
      const auto &referenceBlock = referenceBlockchain[j - 1];
      ASSERT_TRUE(rawBlock == referenceBlock);
      ASSERT_TRUE(block::detail::getData(rawBlock) == block::detail::getData(referenceBlock));
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
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->db(), numBlocks);

  auto adapter = DBAdapter{GetParam()->db()};

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
  const auto referenceBlockchain = GetParam()->referenceBlockchain(GetParam()->db(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->db()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    ASSERT_NO_THROW(adapter.addBlock(block::detail::getData(referenceBlockchain[i - 1])));
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
    ASSERT_TRUE(block::detail::getStateHash(rawBlock) == block::detail::getStateHash(referenceBlock));
  }
}

TEST_P(db_adapter_custom_blockchain, delete_last_reachable_block) {
  const auto numBlocks = 10;
  auto adapter = DBAdapter{GetParam()->db()};

  // Add block updates with no overlapping keys between blocks.
  for (auto i = 1; i <= numBlocks - 1; ++i) {
    ASSERT_EQ(adapter.addBlock(getOffsetUpdates(i, numBlocks)), i);
  }

  // Save the DB updates for the last reachable block and add the block.
  const auto lastReachabeDbUpdates =
      adapter.lastReachableBlockDbUpdates(getOffsetUpdates(numBlocks, numBlocks), numBlocks);
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
  auto adapter = DBAdapter{GetParam()->db()};

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
  auto adapter = DBAdapter{GetParam()->db()};

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
  auto adapter = DBAdapter{GetParam()->db()};

  ASSERT_NO_THROW(adapter.deleteBlock(0));
  ASSERT_NO_THROW(adapter.deleteBlock(1));
}

TEST_P(db_adapter_custom_blockchain, delete_single_st_block) {
  auto adapter = DBAdapter{GetParam()->db()};

  ASSERT_NO_THROW(adapter.addRawBlock(
      block::detail::create(SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)}, BlockDigest{}, Hash{}),
      8));

  // Make sure that a single ST block can be deleted.
  ASSERT_NO_THROW(adapter.deleteBlock(8));
  ASSERT_FALSE(adapter.hasBlock(8));
  ASSERT_THROW(adapter.getRawBlock(8), NotFoundException);
}

TEST_P(db_adapter_custom_blockchain, get_genesis_block_id) {
  auto adapter = DBAdapter{GetParam()->db()};

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
  auto adapter = DBAdapter{GetParam()->db()};
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

TEST_P(db_adapter_custom_blockchain, get_value_from_deleted_block) {
  auto adapter = DBAdapter{GetParam()->db()};
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
  auto adapter = DBAdapter{GetParam()->db()};

  // Add a single block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(Sliver{"k"}, Sliver{"v1"})}), 1);

  // Expect an exception - cannot delete the only block in the system.
  ASSERT_THROW(adapter.deleteBlock(1), std::logic_error);

  // Verify that the block is still there.
  ASSERT_EQ(adapter.getGenesisBlockId(), 1);
  ASSERT_EQ(adapter.getLastReachableBlockId(), 1);
  ASSERT_TRUE(adapter.hasBlock(1));
}

TEST_P(db_adapter_custom_blockchain, delete_multiple_genesis_blocks) {
  const auto numBlocks = 10;
  auto adapter = DBAdapter{GetParam()->db()};

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
  auto adapter = DBAdapter{GetParam()->db()};

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
  auto adapter = DBAdapter{GetParam()->db()};

  const auto key = Sliver{"k"};

  // Add blocks with an overlapping key. Include an empty block.
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v1"})}), 1);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{}), 2);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v3"})}), 3);
  ASSERT_EQ(adapter.addBlock(SetOfKeyValuePairs{std::make_pair(key, Sliver{"v4"})}), 4);

  // Verify there are stale keys in the index since version 2 of the tree (block 2 doesn't update the tree version).
  ASSERT_TRUE(hasStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete the genesis block.
  ASSERT_NO_THROW(adapter.deleteBlock(1));

  // Verify there are stale keys in the index since tree version 2.
  ASSERT_TRUE(hasStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete the genesis block again.
  ASSERT_NO_THROW(adapter.deleteBlock(2));

  // Verify there are stale keys in the index since tree version 2.
  ASSERT_TRUE(hasStaleIndexKeysSince(adapter.getDb(), 2));

  // Delete the genesis block again.
  ASSERT_NO_THROW(adapter.deleteBlock(3));

  // Verify there are no stale keys in the index since tree version 2.
  ASSERT_FALSE(hasStaleIndexKeysSince(adapter.getDb(), 2));
}

TEST_P(db_adapter_custom_blockchain, delete_genesis_and_verify_stale_key_deletion) {
  auto adapter = DBAdapter{GetParam()->db()};

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

#ifdef USE_ROCKSDB
const auto customBlockchainTestsParams =
    ::testing::Values(std::make_shared<DbAdapterTest<TestMemoryDb>>(), std::make_shared<DbAdapterTest<TestRocksDb>>());

const auto refBlockchainTestParams =
    ::testing::Values(std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::NoEmptyBlocks>>(),
                      std::make_shared<DbAdapterTest<TestRocksDb, ReferenceBlockchainType::NoEmptyBlocks>>(),
                      std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::WithEmptyBlocks>>(),
                      std::make_shared<DbAdapterTest<TestRocksDb, ReferenceBlockchainType::WithEmptyBlocks>>());
#else
const auto customBlockchainTestsParams = ::testing::Values(std::make_shared<DbAdapterTest<TestMemoryDb>>());

const auto refBlockchainTestParams =
    ::testing::Values(std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::NoEmptyBlocks>>(),
                      std::make_shared<DbAdapterTest<TestMemoryDb, ReferenceBlockchainType::WithEmptyBlocks>>());
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
