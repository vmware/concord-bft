// Copyright 2020 VMware, all rights reserved
//
// Test merkle tree DB adapter and key manipulation code.

#include "gtest/gtest.h"

#include "assertUtils.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "blockchain/merkle_tree_block.h"
#include "blockchain/merkle_tree_db_adapter.h"
#include "blockchain/merkle_tree_serialization.h"
#include "endianness.hpp"
#include "memorydb/client.h"
#include "rocksdb/client.h"
#include "sparse_merkle/base_types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif

namespace {

using ::bftEngine::SimpleBlockchainStateTransfer::computeBlockDigest;
using ::bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest;

using namespace ::concord::storage::blockchain::v2MerkleTree;
using namespace ::concord::storage::blockchain::v2MerkleTree::detail;

using ::concord::storage::IDBClient;
using ::concord::storage::blockchain::BlockId;
using ::concord::storage::blockchain::ObjectId;
using ::concord::storage::sparse_merkle::BatchedInternalNode;
using ::concord::storage::sparse_merkle::Hash;
using ::concord::storage::sparse_merkle::Hasher;
using ::concord::storage::sparse_merkle::InternalChild;
using ::concord::storage::sparse_merkle::InternalNodeKey;
using ::concord::storage::sparse_merkle::LeafChild;
using ::concord::storage::sparse_merkle::LeafKey;
using ::concord::storage::sparse_merkle::NibblePath;
using ::concord::storage::sparse_merkle::Version;

using ::concordUtils::SetOfKeyValuePairs;
using ::concordUtils::Sliver;
using ::concordUtils::Status;
using ::concordUtils::ValuesVector;

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

Sliver toSliver(std::string b) { return Sliver{std::move(b)}; }

Sliver getSliverOfSize(std::size_t size, char content = 'a') { return std::string(size, content); }

auto getHash(const std::string &str) {
  auto hasher = Hasher{};
  return hasher.hash(str.data(), str.size());
}

auto getHash(const Sliver &sliver) {
  auto hasher = Hasher{};
  return hasher.hash(sliver.data(), sliver.length());
}

auto getBlockDigest(const std::string &data) { return getHash(data).dataArray(); }

StateTransferDigest blockDigest(BlockId blockId, const Sliver &block) {
  auto digest = StateTransferDigest{};
  computeBlockDigest(blockId, block.data(), block.length(), &digest);
  return digest;
}

bool operator==(const void *lhs, const StateTransferDigest &rhs) {
  return std::memcmp(lhs, rhs.content, block::BLOCK_DIGEST_SIZE) == 0;
}

bool operator==(const StateTransferDigest &lhs, const void *rhs) {
  return std::memcmp(lhs.content, rhs, block::BLOCK_DIGEST_SIZE) == 0;
}

StateTransferDigest getZeroDigest() {
  StateTransferDigest digest;
  std::memset(digest.content, 0, block::BLOCK_DIGEST_SIZE);
  return digest;
}

SetOfKeyValuePairs getDeterministicBlockUpdates(std::uint32_t count) {
  auto updates = SetOfKeyValuePairs{};
  auto size = std::uint8_t{0};
  for (auto i = 0u; i < count; ++i) {
    updates[getSliverOfSize(size + 1)] = getSliverOfSize((size + 1) * 3);
    ++size;
  }
  return updates;
}

ValuesVector createBlockchain(const std::shared_ptr<IDBClient> &db, std::size_t length) {
  auto adapter = DBAdapter{db};
  ValuesVector blockchain;

  for (auto i = 1u; i <= length; ++i) {
    Assert(adapter.addLastReachableBlock(getDeterministicBlockUpdates(i * 2)).isOK());
    auto block = Sliver{};
    Assert(adapter.getBlockById(i, block).isOK());
    blockchain.push_back(block);
  }

  return blockchain;
}

const auto defaultData = std::string{"defaultData"};
const auto defaultSliver = Sliver::copy(defaultData.c_str(), defaultData.size());
const auto defaultHash = getHash(defaultData);
const auto defaultHashStrBuf = std::string{reinterpret_cast<const char *>(defaultHash.data()), defaultHash.size()};
const auto defaultBlockId = BlockId{42};
const auto defaultVersion = Version{42};
const auto defaultObjectId = ObjectId{42};
const auto defaultPageId = uint32_t{42};
const auto defaultChkpt = uint64_t{42};
const auto defaultDigest = getBlockDigest(defaultData + defaultData);
const auto maxNumKeys = 100u;
const auto rocksDbPathPrefix = std::string{"/tmp/merkleTreeAdapter_test_rocksdb.db"};
const auto zeroDigest = getZeroDigest();

static_assert(sizeof(EDBKeyType) == 1);
static_assert(sizeof(EKeySubtype) == 1);
static_assert(sizeof(EBFTSubtype) == 1);
static_assert(sizeof(BlockId) == 8);
static_assert(sizeof(ObjectId) == 4);

// Support multithreaded runs by appending the thread ID to the RocksDB path.
std::string rocksDbPath() {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return rocksDbPathPrefix + ss.str();
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::Block: 8, blockId: 64]
TEST(key_manipulator, block_key) {
  const auto key = DBKeyManipulator::genBlockDbKey(defaultBlockId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Block) + serializeIntegral(defaultBlockId));
  ASSERT_EQ(key.length(), 1 + sizeof(defaultBlockId));
  ASSERT_TRUE(key == expected);
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
  const auto key = DBKeyManipulator::genDataDbKey(defaultSliver, defaultBlockId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::Key) + serializeEnum(EKeySubtype::Leaf) + defaultHashStrBuf +
                                 serializeIntegral(defaultBlockId));
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
// [EDBKeyType::BFT: 8, EKeySubtype::Metadata: 8, objectId: 32]
TEST(key_manipulator, metadata_key) {
  const auto key = DBKeyManipulator::generateMetadataKey(defaultObjectId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::Metadata) +
                                 serializeIntegral(defaultObjectId));
  ASSERT_EQ(key.length(), 1 + 1 + 4);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EKeySubtype::ST: 8, objectId: 32]
TEST(key_manipulator, st_key) {
  const auto key = DBKeyManipulator::generateStateTransferKey(defaultObjectId);
  const auto expected =
      toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::ST) + serializeIntegral(defaultObjectId));
  ASSERT_EQ(key.length(), 1 + 1 + 4);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EKeySubtype::STPendingPage: 8, pageId: 32]
TEST(key_manipulator, st_pending_page_key) {
  const auto key = DBKeyManipulator::generateSTPendingPageKey(defaultPageId);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STPendingPage) +
                                 serializeIntegral(defaultPageId));
  ASSERT_EQ(key.length(), 1 + 1 + 4);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EKeySubtype::STCheckpointDescriptor: 8, chkpt: 64]
TEST(key_manipulator, st_chkpt_desc_key) {
  const auto key = DBKeyManipulator::generateSTCheckpointDescriptorKey(defaultChkpt);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STCheckpointDescriptor) +
                                 serializeIntegral(defaultChkpt));
  ASSERT_EQ(key.length(), 1 + 1 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EKeySubtype::STReservedPageStatic: 8, pageId: 32, chkpt: 64]
TEST(key_manipulator, st_res_page_static_key) {
  const auto key = DBKeyManipulator::generateSTReservedPageStaticKey(defaultPageId, defaultChkpt);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STReservedPageStatic) +
                                 serializeIntegral(defaultPageId) + serializeIntegral(defaultChkpt));
  ASSERT_EQ(key.length(), 1 + 1 + 4 + 8);
  ASSERT_TRUE(key == expected);
}

// Expected key structure with respective bit sizes:
// [EDBKeyType::BFT: 8, EKeySubtype::STReservedPageDynamic: 8, pageId: 32, chkpt: 64]
TEST(key_manipulator, st_res_page_dynamic_key) {
  const auto key = DBKeyManipulator::generateSTReservedPageDynamicKey(defaultPageId, defaultChkpt);
  const auto expected = toSliver(serializeEnum(EDBKeyType::BFT) + serializeEnum(EBFTSubtype::STReservedPageDynamic) +
                                 serializeIntegral(defaultPageId) + serializeIntegral(defaultChkpt));
  ASSERT_EQ(key.length(), 1 + 1 + 4 + 8);
  ASSERT_TRUE(key == expected);
}

TEST(block, key_data_equality) {
  ASSERT_TRUE(block::detail::KeyData{true} == block::detail::KeyData{true});
  ASSERT_FALSE(block::detail::KeyData{true} == block::detail::KeyData{false});
}

TEST(block, block_node_equality) {
  // Non-key differences.
  {
    const auto node1 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    const auto node2 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    const auto node3 = block::detail::Node{defaultBlockId + 1, defaultDigest.data(), defaultHash, defaultVersion};
    const auto node4 =
        block::detail::Node{defaultBlockId, getBlockDigest("random data").data(), defaultHash, defaultVersion};
    const auto node5 =
        block::detail::Node{defaultBlockId, defaultDigest.data(), getHash("random data2"), defaultVersion};
    const auto node6 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion + 1};

    ASSERT_EQ(node1.blockId, node2.blockId);
    ASSERT_TRUE(node1.parentDigest == node2.parentDigest);
    ASSERT_TRUE(node1.stateHash == node2.stateHash);
    ASSERT_TRUE(node1.keys == node2.keys);

    ASSERT_NE(node1.blockId, node3.blockId);
    ASSERT_TRUE(node1.parentDigest == node3.parentDigest);
    ASSERT_TRUE(node1.stateHash == node3.stateHash);
    ASSERT_TRUE(node1.keys == node3.keys);

    ASSERT_EQ(node1.blockId, node4.blockId);
    ASSERT_FALSE(node1.parentDigest == node4.parentDigest);
    ASSERT_TRUE(node1.stateHash == node4.stateHash);
    ASSERT_TRUE(node1.keys == node4.keys);

    ASSERT_EQ(node1.blockId, node5.blockId);
    ASSERT_TRUE(node1.parentDigest == node5.parentDigest);
    ASSERT_FALSE(node1.stateHash == node5.stateHash);
    ASSERT_TRUE(node1.keys == node5.keys);

    ASSERT_EQ(node1.blockId, node6.blockId);
    ASSERT_TRUE(node1.parentDigest == node6.parentDigest);
    ASSERT_TRUE(node1.stateHash == node6.stateHash);
    ASSERT_TRUE(node1.keys == node6.keys);

    ASSERT_TRUE(node1 == node2);
    ASSERT_FALSE(node1 == node3);
    ASSERT_FALSE(node1 == node4);
    ASSERT_FALSE(node1 == node5);
    ASSERT_FALSE(node1 == node6);
  }

  // Differences in keys only.
  {
    auto node1 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    auto node2 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    node2.keys.emplace(defaultSliver, block::detail::KeyData{false});
    auto node3 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    node3.keys.emplace(defaultSliver, block::detail::KeyData{true});
    auto node4 = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    node3.keys.emplace(getSliverOfSize(42), block::detail::KeyData{true});

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
    auto node = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // 1 non-deleted key.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    node.keys.emplace(defaultSliver, block::detail::KeyData{false});

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // 1 deleted key.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    node.keys.emplace(defaultSliver, block::detail::KeyData{true});

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // Empty key.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
    node.keys.emplace(getSliverOfSize(0), block::detail::KeyData{true});
    node.keys.emplace(getSliverOfSize(1), block::detail::KeyData{true});

    const auto nodeSliver = block::detail::createNode(node);
    const auto parsedNode = block::detail::parseNode(nodeSliver);

    ASSERT_TRUE(node == parsedNode);
  }

  // Multiple keys with different sizes and deleted flags.
  {
    auto node = block::detail::Node{defaultBlockId, defaultDigest.data(), defaultHash, defaultVersion};
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

TEST(block, block_serialization) {
  SetOfKeyValuePairs updates;
  for (auto i = 1u; i <= maxNumKeys; ++i) {
    updates.emplace(getSliverOfSize(i), getSliverOfSize(i * 10));
  }

  const auto block = block::create(defaultBlockId, updates, defaultDigest.data(), defaultHash);
  const auto parsedUpdates = block::getData(block);
  const auto parsedDigest = static_cast<const std::uint8_t *>(block::getParentDigest(block));
  const auto parsedStateHash = block::getStateHash(block);

  ASSERT_TRUE(updates == parsedUpdates);
  ASSERT_TRUE(std::equal(std::cbegin(defaultDigest), std::cend(defaultDigest), parsedDigest));
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

struct IDbClientFactory {
  virtual std::shared_ptr<IDBClient> db() const = 0;
  virtual std::string type() const = 0;
  virtual ~IDbClientFactory() noexcept = default;
};

struct MemoryDbClientFactory : public IDbClientFactory {
  std::shared_ptr<IDBClient> db() const override { return std::make_shared<::concord::storage::memorydb::Client>(); }

  std::string type() const override { return "memorydb"; }
};

struct RocksDbClientFactory : public IDbClientFactory {
  std::shared_ptr<IDBClient> db() const override {
    fs::remove_all(rocksDbPath());
    // Create the RocksDB client with the default lexicographical comparator.
    return std::make_shared<::concord::storage::rocksdb::Client>(rocksDbPath());
  }

  std::string type() const override { return "RocksDB"; }
};

class db_adapter : public ::testing::TestWithParam<std::shared_ptr<IDbClientFactory>> {
  void SetUp() override { fs::remove_all(rocksDbPath()); }
  void TearDown() override { fs::remove_all(rocksDbPath()); }
};

// Test the last reachable block functionality that relies on key ordering.
TEST_P(db_adapter, get_last_reachable_block) {
  auto adapter = DBAdapter{GetParam()->db()};
  const auto updates = SetOfKeyValuePairs{std::make_pair(defaultSliver, defaultSliver)};
  ASSERT_TRUE(adapter.addLastReachableBlock(updates).isOK());
  ASSERT_TRUE(adapter.addLastReachableBlock(updates).isOK());
  ASSERT_TRUE(adapter.addLastReachableBlock(updates).isOK());
  ASSERT_EQ(adapter.getLastReachableBlock(), 3);
}

TEST_P(db_adapter, get_key_by_ver_1_key) {
  const auto key = defaultSliver;
  const auto keyData = defaultData;
  auto adapter = DBAdapter{GetParam()->db()};
  const auto data1 = Sliver{"data1"};
  const auto data2 = Sliver{"data2"};
  const auto data3 = Sliver{"data3"};
  const auto updates1 = SetOfKeyValuePairs{std::make_pair(key, data1)};
  const auto updates2 = SetOfKeyValuePairs{std::make_pair(key, data2)};
  const auto updates3 = SetOfKeyValuePairs{std::make_pair(key, data3)};
  ASSERT_TRUE(adapter.addLastReachableBlock(updates1).isOK());
  ASSERT_TRUE(adapter.addLastReachableBlock(updates2).isOK());
  ASSERT_TRUE(adapter.addLastReachableBlock(updates3).isOK());

  // Get a non-existent key with a hash that is after the existent key.
  {
    const auto after = Sliver{"dummy"};
    ASSERT_TRUE(getHash(keyData) < getHash(after));

    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(1, after, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get a non-existent key with a hash that is before the existent key.
  {
    const auto before = Sliver{"aa"};
    ASSERT_TRUE(getHash(before) < getHash(keyData));

    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(1, before, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get a key with a version smaller than the first version and expect an empty response.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(0, key, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get the first one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(1, key, out, actualVersion);
    ASSERT_TRUE(status.isOK());
    ASSERT_TRUE(out == data1);
    ASSERT_EQ(actualVersion, 1);
  }

  // Get the second one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(2, key, out, actualVersion);
    ASSERT_TRUE(status.isOK());
    ASSERT_TRUE(out == data2);
    ASSERT_EQ(actualVersion, 2);
  }

  // Get the last one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(3, key, out, actualVersion);
    ASSERT_TRUE(status.isOK());
    ASSERT_TRUE(out == data3);
    ASSERT_EQ(actualVersion, 3);
  }

  // Get a key with a version bigger than the last one and expect the last one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(42, key, out, actualVersion);
    ASSERT_TRUE(status.isOK());
    ASSERT_TRUE(out == data3);
    ASSERT_EQ(actualVersion, 3);
  }
}

// Test the getKeyByReadVersion() method with multiple keys, including ones that are ordered before and after the keys
// in the system.
// Note: Leaf keys are ordered first on the key hash and then on the version. See db_types.h and
// merkle_tree_serialization.h for more information.
TEST_P(db_adapter, get_key_by_ver_multiple_keys) {
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
  ASSERT_TRUE(adapter.addLastReachableBlock(updates1).isOK());
  ASSERT_TRUE(adapter.addLastReachableBlock(updates2).isOK());
  ASSERT_TRUE(adapter.addLastReachableBlock(updates3).isOK());

  // Get keys with a version that is smaller than the first version and expect an empty response.
  {
    const auto version = 0;
    auto outValue = Sliver{};
    auto actualVersion = BlockId{119};

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, key, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, before, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, after, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, random, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get keys with a version that is bigger than the last version and expect the last one.
  {
    const auto version = 42;
    auto outValue = Sliver{};
    auto actualVersion = BlockId{119};

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, key, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data3);
    ASSERT_EQ(actualVersion, 3);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, before, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data2);
    ASSERT_EQ(actualVersion, 3);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, after, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data3);
    ASSERT_EQ(actualVersion, 2);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, random, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get keys at version 1.
  {
    const auto version = 1;
    auto outValue = Sliver{};
    auto actualVersion = BlockId{119};

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, key, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data1);
    ASSERT_EQ(actualVersion, 1);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, before, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data2);
    ASSERT_EQ(actualVersion, 1);

    // The after key is not present at version 1.
    ASSERT_TRUE(adapter.getKeyByReadVersion(version, after, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);

    // The random key is not present at version 1.
    ASSERT_TRUE(adapter.getKeyByReadVersion(version, random, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get keys at version 2.
  {
    const auto version = 2;
    auto outValue = Sliver{};
    auto actualVersion = BlockId{119};

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, key, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data2);
    ASSERT_EQ(actualVersion, 2);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, before, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data1);
    ASSERT_EQ(actualVersion, 2);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, after, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data3);
    ASSERT_EQ(actualVersion, 2);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, random, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get keys at version 3.
  {
    const auto version = 3;
    auto outValue = Sliver{};
    auto actualVersion = BlockId{119};

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, key, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data3);
    ASSERT_EQ(actualVersion, 3);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, before, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data2);
    ASSERT_EQ(actualVersion, 3);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, after, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue == data3);
    ASSERT_EQ(actualVersion, 2);

    ASSERT_TRUE(adapter.getKeyByReadVersion(version, random, outValue, actualVersion).isOK());
    ASSERT_TRUE(outValue.empty());
    ASSERT_EQ(actualVersion, 0);
  }
}

TEST_P(db_adapter, add_and_get_block) {
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
      const auto status1 = adapter.addLastReachableBlock(updates1);
      ASSERT_TRUE(status1.isOK());

      const auto status2 = adapter.addLastReachableBlock(updates2);
      ASSERT_TRUE(status2.isOK());
    }

    // Try to get a non-existent block.
    {
      Sliver block;
      const auto status = adapter.getBlockById(3, block);
      ASSERT_TRUE(status.isNotFound());
    }

    // Get the first block.
    Sliver block1;
    {
      const auto status = adapter.getBlockById(1, block1);
      ASSERT_TRUE(status.isOK());
      ASSERT_TRUE(updates1 == block::getData(block1));
    }

    // Get the second block.
    {
      Sliver block2;
      const auto status = adapter.getBlockById(2, block2);
      ASSERT_TRUE(status.isOK());
      ASSERT_TRUE(updates2 == block::getData(block2));
      ASSERT_TRUE(adapter.getStateHash() == block::getStateHash(block2));
      ASSERT_TRUE(block::getParentDigest(block2) == blockDigest(1, block1));
    }
  }
}

TEST_P(db_adapter, add_multiple_deterministic_blocks) {
  auto adapter = DBAdapter{GetParam()->db()};

  const auto numBlocks = 250;
  auto count = std::uint16_t{0};
  for (auto i = 1u; i <= numBlocks; ++i) {
    ASSERT_TRUE(adapter.addLastReachableBlock(getDeterministicBlockUpdates(count + 1)).isOK());
    ASSERT_EQ(adapter.getLastReachableBlock(), i);
    ++count;
  }

  count = 0;
  for (auto i = 1u; i <= numBlocks; ++i) {
    auto block = Sliver{};
    ASSERT_TRUE(adapter.getBlockById(i, block).isOK());
    ASSERT_FALSE(block.empty());

    // Expect a zero parent digest for block 1.
    if (i == 1) {
      ASSERT_TRUE(block::getParentDigest(block) == zeroDigest);
    } else {
      auto parentBlock = Sliver{};
      ASSERT_TRUE(adapter.getBlockById(i - 1, parentBlock).isOK());
      ASSERT_FALSE(parentBlock.empty());
      ASSERT_TRUE(blockDigest(i - 1, parentBlock) == block::getParentDigest(block));
    }

    const auto updates = getDeterministicBlockUpdates(count + 1);
    ASSERT_TRUE(block::getData(block) == updates);

    ++count;
  }
}

TEST_P(db_adapter, add_empty_block) {
  auto adapter = DBAdapter{GetParam()->db()};
  ASSERT_TRUE(adapter.addLastReachableBlock(SetOfKeyValuePairs{}).isIllegalOperation());
}

TEST_P(db_adapter, no_blocks) {
  const auto adapter = DBAdapter{GetParam()->db()};

  ASSERT_EQ(adapter.getLastReachableBlock(), 0);
  ASSERT_EQ(adapter.getLatestBlock(), 0);

  auto block = Sliver{};
  ASSERT_TRUE(adapter.getBlockById(defaultBlockId, block).isNotFound());
  ASSERT_TRUE(block.empty());

  auto value = Sliver{};
  auto actualVersion = BlockId{};
  ASSERT_TRUE(
      adapter
          .getKeyByReadVersion(
              defaultBlockId, DBKeyManipulator::genDataDbKey(defaultSliver, defaultBlockId), value, actualVersion)
          .isOK());
  ASSERT_EQ(actualVersion, 0);
  ASSERT_TRUE(value.empty());
}

TEST_P(db_adapter, state_transfer_reverse_order_with_blockchain_blocks) {
  const auto numBlockchainBlocks = 5;
  const auto numStBlocks = 7;
  const auto numTotalBlocks = numBlockchainBlocks + numStBlocks;
  const auto referenceBlockchain = createBlockchain(GetParam()->db(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->db()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    ASSERT_TRUE(adapter.addLastReachableBlock(block::getData(referenceBlockchain[i - 1])).isOK());
    ASSERT_EQ(adapter.getLatestBlock(), i);
    ASSERT_EQ(adapter.getLastReachableBlock(), i);
  }

  // Receive more blocks from state transfer and add them to the blockchain.
  for (auto i = numTotalBlocks; i > numBlockchainBlocks; --i) {
    ASSERT_TRUE(adapter.addBlock(referenceBlockchain[i - 1], i).isOK());
    ASSERT_EQ(adapter.getLatestBlock(), numTotalBlocks);
    if (i == numBlockchainBlocks + 1) {
      // We link the blockchain and state transfer chains at that point.
      ASSERT_EQ(adapter.getLastReachableBlock(), numTotalBlocks);
    } else {
      ASSERT_EQ(adapter.getLastReachableBlock(), numBlockchainBlocks);
    }

    // Verify that initial blocks are accessible at all steps.
    for (auto j = 1; j <= numBlockchainBlocks; ++j) {
      auto block = Sliver{};
      ASSERT_TRUE(adapter.getBlockById(j, block).isOK());
      const auto &referenceBlock = referenceBlockchain[j - 1];
      ASSERT_TRUE(block == referenceBlock);
      ASSERT_TRUE(block::getData(block) == block::getData(referenceBlock));
      ASSERT_TRUE(block::getStateHash(block) == block::getStateHash(referenceBlock));
      if (j > 1) {
        const auto &prevReferenceBlock = referenceBlockchain[j - 2];
        ASSERT_TRUE(block::getParentDigest(block) == blockDigest(j - 1, prevReferenceBlock));
      } else {
        ASSERT_TRUE(block::getParentDigest(block) == zeroDigest);
      }
    }
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= numTotalBlocks; ++i) {
    auto block = Sliver{};
    ASSERT_TRUE(adapter.getBlockById(i, block).isOK());
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(block == referenceBlock);
    ASSERT_TRUE(block::getData(block) == block::getData(referenceBlock));
    ASSERT_TRUE(block::getStateHash(block) == block::getStateHash(referenceBlock));
    if (i > 1) {
      const auto &prevReferenceBlock = referenceBlockchain[i - 2];
      ASSERT_TRUE(block::getParentDigest(block) == blockDigest(i - 1, prevReferenceBlock));
    } else {
      ASSERT_TRUE(block::getParentDigest(block) == zeroDigest);
    }
  }
}

TEST_P(db_adapter, state_transfer_fetch_whole_blockchain_in_reverse_order) {
  const auto numBlocks = 7;
  const auto referenceBlockchain = createBlockchain(GetParam()->db(), numBlocks);

  auto adapter = DBAdapter{GetParam()->db()};

  for (auto i = numBlocks; i > 0; --i) {
    ASSERT_TRUE(adapter.addBlock(referenceBlockchain[i - 1], i).isOK());
    ASSERT_EQ(adapter.getLatestBlock(), numBlocks);
    if (i == 1) {
      // We link the blockchain and state transfer chains at that point.
      ASSERT_EQ(adapter.getLastReachableBlock(), numBlocks);
    } else {
      ASSERT_EQ(adapter.getLastReachableBlock(), 0);
    }
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= numBlocks; ++i) {
    auto block = Sliver{};
    ASSERT_TRUE(adapter.getBlockById(i, block).isOK());
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(block == referenceBlock);
    ASSERT_TRUE(block::getData(block) == block::getData(referenceBlock));
    ASSERT_TRUE(block::getStateHash(block) == block::getStateHash(referenceBlock));
    if (i > 1) {
      const auto &prevReferenceBlock = referenceBlockchain[i - 2];
      ASSERT_TRUE(block::getParentDigest(block) == blockDigest(i - 1, prevReferenceBlock));
    } else {
      ASSERT_TRUE(block::getParentDigest(block) == zeroDigest);
    }
  }
}

TEST_P(db_adapter, state_transfer_unordered_with_blockchain_blocks) {
  const auto numBlockchainBlocks = 5;
  const auto numStBlocks = 3;
  const auto numTotalBlocks = numBlockchainBlocks + numStBlocks;
  const auto referenceBlockchain = createBlockchain(GetParam()->db(), numTotalBlocks);

  auto adapter = DBAdapter{GetParam()->db()};

  // Add blocks to the blockchain and verify both block pointers.
  for (auto i = 1; i <= numBlockchainBlocks; ++i) {
    ASSERT_TRUE(adapter.addLastReachableBlock(block::getData(referenceBlockchain[i - 1])).isOK());
    ASSERT_EQ(adapter.getLatestBlock(), i);
    ASSERT_EQ(adapter.getLastReachableBlock(), i);
  }

  // Add block 7.
  {
    ASSERT_TRUE(adapter.addBlock(referenceBlockchain[6], 7).isOK());
    ASSERT_EQ(adapter.getLastReachableBlock(), numBlockchainBlocks);
    ASSERT_EQ(adapter.getLatestBlock(), 7);
    for (auto i = 1; i <= numBlockchainBlocks; ++i) {
      auto block = Sliver{};
      ASSERT_TRUE(adapter.getBlockById(i, block).isOK());
      const auto &referenceBlock = referenceBlockchain[i - 1];
      ASSERT_TRUE(block == referenceBlock);
      ASSERT_TRUE(block::getData(block) == block::getData(referenceBlock));
      ASSERT_TRUE(block::getStateHash(block) == block::getStateHash(referenceBlock));
    }
  }

  // Add block 6.
  {
    ASSERT_TRUE(adapter.addBlock(referenceBlockchain[5], 6).isOK());
    ASSERT_EQ(adapter.getLastReachableBlock(), 7);
    ASSERT_EQ(adapter.getLatestBlock(), 7);
    for (auto i = 1; i <= 7; ++i) {
      auto block = Sliver{};
      ASSERT_TRUE(adapter.getBlockById(i, block).isOK());
      const auto &referenceBlock = referenceBlockchain[i - 1];
      ASSERT_TRUE(block == referenceBlock);
      ASSERT_TRUE(block::getData(block) == block::getData(referenceBlock));
      ASSERT_TRUE(block::getStateHash(block) == block::getStateHash(referenceBlock));
    }
  }

  // Add block 8.
  {
    ASSERT_TRUE(adapter.addBlock(referenceBlockchain[7], 8).isOK());
    ASSERT_EQ(adapter.getLastReachableBlock(), 8);
    ASSERT_EQ(adapter.getLatestBlock(), 8);
  }

  // Verify that all blocks are accessible at the end.
  for (auto i = 1; i <= numTotalBlocks; ++i) {
    auto block = Sliver{};
    ASSERT_TRUE(adapter.getBlockById(i, block).isOK());
    const auto &referenceBlock = referenceBlockchain[i - 1];
    ASSERT_TRUE(block == referenceBlock);
    ASSERT_TRUE(block::getData(block) == block::getData(referenceBlock));
    ASSERT_TRUE(block::getStateHash(block) == block::getStateHash(referenceBlock));
  }
}

// Generate test name suffixes based on the DB client type.
struct TypePrinter {
  template <typename T>
  std::string operator()(const testing::TestParamInfo<T> &info) const {
    return info.param->type();
  }
};

// Test DBAdapter with both memory and RocksDB clients.
INSTANTIATE_TEST_CASE_P(db_adapter_tests,
                        db_adapter,
                        ::testing::Values(std::make_shared<MemoryDbClientFactory>(),
                                          std::make_shared<RocksDbClientFactory>()),
                        TypePrinter{});

}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
