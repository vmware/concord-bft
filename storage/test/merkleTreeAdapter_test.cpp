// Copyright 2020 VMware, all rights reserved
//
// Test merkle tree DB adapter and key manipulation code.

#include "gtest/gtest.h"

#include "blockchain/merkle_tree_db_adapter.h"
#include "endianness.hpp"
#include "memorydb/client.h"
#include "memorydb/key_comparator.h"
#include "sparse_merkle/base_types.h"

#include <memory>
#include <string>
#include <type_traits>

using namespace concord::storage::blockchain::v2MerkleTree;
using namespace concord::storage::blockchain::v2MerkleTree::detail;
using concord::storage::blockchain::BlockId;
using concord::storage::blockchain::ObjectId;
using concord::storage::memorydb::Client;
using concord::storage::memorydb::KeyComparator;
using concord::storage::sparse_merkle::Hash;
using concord::storage::sparse_merkle::Hasher;
using concord::storage::sparse_merkle::InternalNodeKey;
using concord::storage::sparse_merkle::LeafKey;
using concord::storage::sparse_merkle::NibblePath;
using concord::storage::sparse_merkle::Version;
using concordUtils::Sliver;
using concordUtils::Status;

namespace {

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

const auto defaultData = std::string{"this is a test string"};
const auto defaultSliver = Sliver::copy(defaultData.c_str(), defaultData.size());
auto hasher = Hasher{};
const auto defaultHash = hasher.hash(defaultData.data(), defaultData.size());
const auto defaultHashStrBuf = std::string{reinterpret_cast<const char *>(defaultHash.data()), defaultHash.size()};
const auto defaultBlockId = BlockId{42};
const auto defaultVersion = Version{42};
const auto defaultObjectId = ObjectId{42};
const auto defaultPageId = uint32_t{42};
const auto defaultChkpt = uint64_t{42};

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

// Test the latest block functionality that relies on key ordering.
TEST(db_adapter, get_latest_block) {
  auto db = std::make_shared<Client>();
  db->put(DBKeyManipulator::genBlockDbKey(1), defaultSliver);
  db->put(DBKeyManipulator::genBlockDbKey(2), defaultSliver);
  db->put(DBKeyManipulator::genBlockDbKey(3), defaultSliver);
  // Insert a dummy root root node.
  db->put(DBKeyManipulator::genInternalDbKey(InternalNodeKey::root(3)), Sliver{});
  const auto adapter = DBAdapter{db};
  ASSERT_EQ(adapter.getLatestBlock(), 3);
}

TEST(db_adapter, get_key_by_ver) {
  const auto data3 = Sliver{std::string{"data3"}};
  const auto data4 = Sliver{std::string{"data4"}};
  const auto data5 = Sliver{std::string{"data5"}};

  auto db = std::make_shared<Client>();
  db->put(DBKeyManipulator::genDataDbKey(defaultSliver, 3), data3);
  db->put(DBKeyManipulator::genDataDbKey(defaultSliver, 4), data4);
  db->put(DBKeyManipulator::genDataDbKey(defaultSliver, 5), data5);
  // Insert a dummy root root node.
  db->put(DBKeyManipulator::genInternalDbKey(InternalNodeKey::root(5)), Sliver{});
  const auto adapter = DBAdapter{db};

  // Get a key before the first one and expect an empty response.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(1, defaultSliver, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out.empty());
    ASSERT_EQ(actualVersion, 0);
  }

  // Get the first one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(3, defaultSliver, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out == data3);
    ASSERT_EQ(actualVersion, 3);
  }

  // Get the second one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(4, defaultSliver, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out == data4);
    ASSERT_EQ(actualVersion, 4);
  }

  // Get the last one.
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(5, defaultSliver, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out == data5);
    ASSERT_EQ(actualVersion, 5);
  }

  // Get a key past the last one and expect the last one
  {
    Sliver out;
    BlockId actualVersion;
    const auto status = adapter.getKeyByReadVersion(42, defaultSliver, out, actualVersion);
    ASSERT_TRUE(status == Status::OK());
    ASSERT_TRUE(out == data5);
    ASSERT_EQ(actualVersion, 5);
  }
}

}  // namespace

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
