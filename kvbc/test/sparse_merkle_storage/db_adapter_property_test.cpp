// Copyright 2020 VMware, all rights reserved

#include "gtest/gtest.h"
#include "rapidcheck/rapidcheck.h"
#include "rapidcheck/extras/gtest.h"

#include "storage_test_common.h"

#include "kv_types.hpp"
#include "merkle_tree_block.h"
#include "merkle_tree_db_adapter.h"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "storage/db_interface.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

using ::concord::kvbc::BlockDigest;
using ::concord::kvbc::BlockId;
using ::concord::kvbc::Key;
using ::concord::kvbc::SetOfKeyValuePairs;
using ::concord::kvbc::Value;
using ::concord::storage::IDBClient;
using ::concord::kvbc::v2MerkleTree::block::detail::create;
using ::concord::kvbc::v2MerkleTree::block::detail::getData;
using ::concord::kvbc::v2MerkleTree::block::detail::getParentDigest;
using ::concord::kvbc::v2MerkleTree::block::detail::getStateHash;
using ::concord::kvbc::v2MerkleTree::DBAdapter;
using ::concord::kvbc::sparse_merkle::Hash;
using ::concordUtils::Sliver;

using HashArray = std::array<std::uint8_t, Hash::SIZE_IN_BYTES>;

namespace rc {

template <>
struct Arbitrary<Sliver> {
  static auto arbitrary() { return gen::construct<Sliver>(gen::string<std::string>()); }
};

template <>
struct Arbitrary<Hash> {
  static auto arbitrary() { return gen::construct<Hash>(gen::arbitrary<HashArray>()); }
};

// Generate key/value sets with arbitrary keys and non-empty values.
template <>
struct Arbitrary<SetOfKeyValuePairs> {
  static auto arbitrary() {
    return gen::container<SetOfKeyValuePairs>(gen::arbitrary<Sliver>(),
                                              gen::construct<Sliver>(gen::nonEmpty(gen::string<std::string>())));
  }
};

}  // namespace rc

namespace {

const auto zeroDigest = BlockDigest{};

void addBlocks(const std::vector<SetOfKeyValuePairs> &blockUpdates, DBAdapter &adapter) {
  for (const auto &updates : blockUpdates) {
    adapter.addBlock(updates);
  }
}

struct BlockUpdatesInfo {
  std::unordered_set<Key> uniqueKeys;
  std::size_t totalKeys{0};
};

BlockUpdatesInfo getBlockUpdatesInfo(const std::vector<SetOfKeyValuePairs> &blockUpdates) {
  auto info = BlockUpdatesInfo{};
  for (const auto &updates : blockUpdates) {
    for (const auto &kv : updates) {
      info.uniqueKeys.insert(kv.first);
      ++info.totalKeys;
    }
  }
  return info;
}

bool hasMultiVersionedKey(const BlockUpdatesInfo &info) { return info.totalKeys != info.uniqueKeys.size(); }

struct IDbAdapterTest {
  virtual std::shared_ptr<IDBClient> db() const = 0;
  virtual std::string type() const = 0;
  virtual bool enforceMultiVersionedKeys() const = 0;
  virtual ~IDbAdapterTest() noexcept = default;
};

template <typename Database, bool multiVersionedKeys = false>
struct DbAdapterTest : public IDbAdapterTest {
  std::shared_ptr<IDBClient> db() const override { return Database::create(); }
  std::string type() const override {
    return Database::type() + (enforceMultiVersionedKeys() ? "_enforcedMultiVerKey" : "_noMultiVerKeyEnforced");
  }
  bool enforceMultiVersionedKeys() const override { return multiVersionedKeys; }
};

using db_adapter_block_tests = ParametrizedTest<std::shared_ptr<IDbAdapterTest>>;
using db_adapter_kv_tests = ParametrizedTest<std::shared_ptr<IDbAdapterTest>>;

// Test that fetched block parameters match the ones that were used to create the block.
RC_GTEST_PROP(block,
              create,
              (const SetOfKeyValuePairs &updates, const BlockDigest &parentDigest, const Hash &stateHash)) {
  const auto block = create(updates, parentDigest, stateHash);
  RC_ASSERT(getData(block) == updates);
  RC_ASSERT(getParentDigest(block) == parentDigest);
  RC_ASSERT(getStateHash(block) == stateHash);
}

// Test that the blockchain property is maintained. In pseudocode:
//  * [getData(block) == blockUpdate] - the passed updates are the same as the fetched ones after adding the block
//  * [getParentDigest(block) == blockDigest(prevBlock)] - blockchain property
//  * [getLastReachableBlock() == getLatestBlock() == blockUpdates.size()] - last reachable block is equal to the latest
//    known block ID after adding the blocks
TEST_P(db_adapter_block_tests, blockchain_property) {
  const auto test = [this](const std::vector<SetOfKeyValuePairs> &blockUpdates) {
    auto adapter = DBAdapter{GetParam()->db()};
    addBlocks(blockUpdates, adapter);

    RC_ASSERT(adapter.getLastReachableBlockId() == blockUpdates.size());
    RC_ASSERT(adapter.getLatestBlockId() == blockUpdates.size());

    for (auto i = 0ul; i < blockUpdates.size(); ++i) {
      const auto rawBlock = adapter.getRawBlock(i + 1);
      RC_ASSERT(getData(rawBlock) == blockUpdates[i]);
      if (i == 0) {
        RC_ASSERT(getParentDigest(rawBlock) == zeroDigest);
      } else {
        const auto rawPrevBlock = adapter.getRawBlock(i);
        RC_ASSERT(getParentDigest(rawBlock) == blockDigest(i, rawPrevBlock));
      }
    }
  };

  ASSERT_TRUE(rc::check(test));
}

// Test that when adapter.addBlock(block, getLastReachableBlock() + 1) is called and there is a gap between the
// blockchain and the state transfer chain, the block is added as the last reachable one (in the gap).
// Note: This test uses arbitrary values for parent digest and state hash as they don't have a relation to the tested
// property.
TEST_P(db_adapter_block_tests, add_block_at_back_equivalent_to_last_reachable) {
  const auto test = [this](const std::vector<SetOfKeyValuePairs> &blockchainUpdates,
                           const std::vector<SetOfKeyValuePairs> &stateTransferUpdates,
                           const SetOfKeyValuePairs &lastReachableUpdates,
                           const BlockDigest &parentDigest,
                           const Hash &stateHash) {
    RC_PRE(!stateTransferUpdates.empty());

    // Ensure there is a gap of a at least 2 blocks (by setting an offset of at least 3) between blockchain and state
    // transfer blocks. We want that as we would like to make sure chains will not be linked after the test.
    const auto stateTransferOffset = *rc::gen::inRange(3, 10000);

    auto adapter = DBAdapter{GetParam()->db()};

    // First, add blockchain updates.
    addBlocks(blockchainUpdates, adapter);

    const auto lastReachableBefore = adapter.getLastReachableBlockId();

    // Then, add state transfer blocks at an offset, ensuring a gap.
    for (auto i = 0ul; i < stateTransferUpdates.size(); ++i) {
      const auto stBlockId = lastReachableBefore + stateTransferOffset + i;
      adapter.addRawBlock(create(stateTransferUpdates[i], parentDigest, stateHash), stBlockId);
      RC_ASSERT(adapter.getLatestBlockId() == stBlockId);
    }

    // Ensure the last reachable block ID hasn't changed as we are adding at an offset.
    RC_ASSERT(adapter.getLastReachableBlockId() == lastReachableBefore);

    // Lastly, add a single block at the back.
    adapter.addRawBlock(create(lastReachableUpdates, parentDigest, stateHash), lastReachableBefore + 1);

    // Verify that the block was added as the last reachable and a gap still exists.
    RC_ASSERT(adapter.getLastReachableBlockId() == lastReachableBefore + 1);
    RC_ASSERT(adapter.getLastReachableBlockId() < adapter.getLatestBlockId());
  };

  ASSERT_TRUE(rc::check(test));
}

// Test that getLastReachableBlock() <= getLatestBlock() holds during a simulated state transfer.
// Note: This test uses arbitrary values for parent digest and state hash as they don't have a relation to the tested
// property.
TEST_P(db_adapter_block_tests, reachable_during_state_transfer_property) {
  const auto test = [this](const std::vector<SetOfKeyValuePairs> &blockchainUpdates,
                           const std::vector<SetOfKeyValuePairs> &stateTransferUpdates,
                           const BlockDigest &parentDigest,
                           const Hash &stateHash) {
    auto adapter = DBAdapter{GetParam()->db()};

    // First, add blockchain updates.
    addBlocks(blockchainUpdates, adapter);

    // Then, start adding state transfer blocks in reverse order.
    const auto latestBlockId = blockchainUpdates.size() + stateTransferUpdates.size();
    for (auto i = 0ul; i < stateTransferUpdates.size(); ++i) {
      const auto blockId = latestBlockId - i;
      adapter.addRawBlock(create(stateTransferUpdates[i], parentDigest, stateHash), blockId);
      if (blockId > blockchainUpdates.size() + 1) {
        RC_ASSERT(adapter.getLastReachableBlockId() == blockchainUpdates.size());
      }
    }
    RC_ASSERT(adapter.getLastReachableBlockId() == adapter.getLatestBlockId());
    RC_ASSERT(adapter.getLatestBlockId() == latestBlockId);
  };

  ASSERT_TRUE(rc::check(test));
}

// Test that getting the value of an existing key at block version V returns the original written value at version V and
// the returned actual block version A == V.
TEST_P(db_adapter_kv_tests, get_key_at_version_it_was_written_at) {
  const auto test = [this](const std::vector<SetOfKeyValuePairs> &blockUpdates) {
    const auto blockUpdateInfo = getBlockUpdatesInfo(blockUpdates);
    if (GetParam()->enforceMultiVersionedKeys()) {
      RC_PRE(hasMultiVersionedKey(blockUpdateInfo));
    }

    auto adapter = DBAdapter{GetParam()->db()};
    addBlocks(blockUpdates, adapter);

    for (auto i = 0ul; i < blockUpdates.size(); ++i) {
      for (const auto &kv : blockUpdates[i]) {
        const auto [value, actualVersion] = adapter.getValue(kv.first, i + 1);
        RC_ASSERT(actualVersion == i + 1);
        RC_ASSERT(value == kv.second);
      }
    }
  };

  ASSERT_TRUE(rc::check(test));
}

// Test that A <= V where version A is the returned actual version and version V = getLastReachableBlock() is the
// requested version. Additionally, verify that the returned value is the original value written at version A. This
// property should hold for all keys.
TEST_P(db_adapter_kv_tests, get_all_keys_at_last_version) {
  const auto test = [this](const std::vector<SetOfKeyValuePairs> &blockUpdates) {
    const auto blockUpdateInfo = getBlockUpdatesInfo(blockUpdates);
    if (GetParam()->enforceMultiVersionedKeys()) {
      RC_PRE(hasMultiVersionedKey(blockUpdateInfo));
    }

    auto adapter = DBAdapter{GetParam()->db()};
    addBlocks(blockUpdates, adapter);

    const auto latestVersion = adapter.getLastReachableBlockId();
    for (const auto &key : blockUpdateInfo.uniqueKeys) {
      const auto [value, actualVersion] = adapter.getValue(key, latestVersion);
      RC_ASSERT(actualVersion > 0ul);
      RC_ASSERT(actualVersion <= latestVersion);
      const auto &updates = blockUpdates[actualVersion - 1];
      auto it = updates.find(key);
      RC_ASSERT(it != std::cend(updates));
      RC_ASSERT(it->second == value);
    }
  };

  ASSERT_TRUE(rc::check(test));
}

#ifdef USE_ROCKSDB
using TestDbType = TestRocksDb;
#else
using TestDbType = TestMemoryDb;
#endif

INSTANTIATE_TEST_CASE_P(db_adapter_block_tests_case,
                        db_adapter_block_tests,
                        ::testing::Values(std::make_shared<DbAdapterTest<TestDbType>>()),
                        TypePrinter{});

// Test with and without multiple-versioned keys.
INSTANTIATE_TEST_CASE_P(db_adapter_kv_tests_case,
                        db_adapter_kv_tests,
                        ::testing::Values(std::make_shared<DbAdapterTest<TestDbType, false>>(),
                                          std::make_shared<DbAdapterTest<TestDbType, true>>()),
                        TypePrinter{});

}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
