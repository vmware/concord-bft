// Copyright 2019 VMware, all rights reserved
//
// Unit tests for ECS S3 object store replication.
// IMPORTANT: the test assume that the S3 storage is up.

#include "ror_app_state.hpp"
#include <cstdlib>
#include <memory>
#include "SimpleBCStateTransfer.hpp"
#include "blockchain/block.h"
#include "blockchain/db_adapter.h"
#include "blockchain/db_interfaces.h"
#include "gtest/gtest.h"
#include "kv_types.hpp"
#include "rocksdb/comparator.h"
#include "sliver.hpp"
#include "status.hpp"
#include "storage/db_interface.h"
#include "ror_test_setup.hpp"

using namespace concord::storage;
using namespace concord::storage::blockchain;
using namespace concord::consensus;
using namespace bftEngine::SimpleBlockchainStateTransfer;
using namespace concord::storage::blockchain::block::detail;

namespace {

class RoRAppStateTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef USE_ROCKSDB
    std::string a = "sudo rm -rf unit_test_db_meta";
    int r = std::system(a.c_str());
    ASSERT_EQ(r, 0);
    a = "sudo rm -rf unit_test_db_object_store";
    r = std::system(a.c_str());
    ASSERT_EQ(r, 0);
#endif

    for (int i = 0; i < numOfBlocks * 2; i++) {
      char *block = new char[kBlockSize];
      Header *bh = (Header *)block;
      bh->numberOfElements = 1;
      bh->parentDigestLength = bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;
      if (i == 0)
        memset(bh->parentDigest, 0, bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE);
      else
        bftEngine::SimpleBlockchainStateTransfer::computeBlockDigest(
            i,
            reinterpret_cast<const char *>(blocks[i - 1] + sizeof(Header)),
            kBlockSize - sizeof(Header),
            (StateTransferDigest *)bh->parentDigest);

      memset(block + sizeof(Header), 'a' + i, kBlockSize - sizeof(Header));
      blocks.push_back(block);
    }
  }

  void TearDown() override {
    for (size_t i = 0; i < blocks.size(); i++) delete[] blocks[i];
  }

  void SetClient() {
    objectStoreClient = set_object_store_client();
    objectStoreClient->init();
    localClient = set_local_client();
    localClient->init();
    appState = std::make_shared<RoRAppState>(std::make_shared<DBKeyManipulator>(), localClient, objectStoreClient);

    for (size_t i = 0; i < blocks.size(); i++) {
      Sliver key = km.genBlockDbKey(i + 1);
      Status res = objectStoreClient->del(key);
    }
  }

  void check_metadata(uint64_t expectedLastBlockNum, int64_t expectedLastReachableBlockNum) {
    Sliver value;
    Status s = localClient->get(lastBlockIdKey, value);
    ASSERT_TRUE(s.isOK());
    auto lastWrittenBlockNum = *(uint64_t *)value.data();
    s = localClient->get(lastReachableBlockIdKey, value);
    ASSERT_TRUE(s.isOK());
    auto lastWrittenReachBlockNum = *(uint64_t *)value.data();

    auto lastInMemBlockNum = appState->getLastBlockNum();
    ASSERT_EQ(lastInMemBlockNum, expectedLastBlockNum);
    auto lastInMemReachableBlockNum = appState->getLastReachableBlockNum();
    ASSERT_EQ(lastInMemReachableBlockNum, expectedLastReachableBlockNum);

    ASSERT_EQ(lastWrittenBlockNum, lastWrittenReachBlockNum);
    ASSERT_EQ(lastInMemBlockNum, lastInMemReachableBlockNum);
    ASSERT_EQ(lastWrittenReachBlockNum, lastInMemBlockNum);
    ASSERT_EQ(expectedLastBlockNum, lastWrittenBlockNum);
  }

  const int numOfBlocks = 10;
  const int kBlockSize = 1000;
  std::vector<char *> blocks;

  DBKeyManipulator km;
  std::shared_ptr<IDBClient> objectStoreClient = nullptr;
  std::shared_ptr<IDBClient> localClient = nullptr;
  std::shared_ptr<RoRAppState> appState = nullptr;
  Sliver lastBlockIdKey = Sliver::copy(kLastBlockIdKey.c_str(), kLastBlockIdKey.size());
  Sliver lastReachableBlockIdKey = Sliver::copy(kLastReachableBlockIdKey.c_str(), kLastReachableBlockIdKey.size());
};
}  // namespace

TEST_F(RoRAppStateTest, PutBlocksCleanState) {
  SetClient();

  ASSERT_EQ(appState->getLastBlockNum(), 0);
  ASSERT_EQ(appState->getLastReachableBlockNum(), 0);

  for (int i = numOfBlocks - 1; i >= 0; i--) {
    bool res = appState->putBlock(i + 1, blocks[i], kBlockSize);
    ASSERT_TRUE(res);

    if (i > 0) {
      Sliver value;
      Status s = localClient->get(lastBlockIdKey, value);
      ASSERT_TRUE(s.isOK());
      ASSERT_EQ(numOfBlocks, *(uint64_t *)value.data());

      s = localClient->get(lastReachableBlockIdKey, value);
      ASSERT_TRUE(s.isOK());
      ASSERT_EQ(0, *(uint64_t *)value.data());

      ASSERT_EQ(appState->getLastBlockNum(), numOfBlocks);
      ASSERT_EQ(appState->getLastReachableBlockNum(), 0);
    }
  }

  check_metadata(numOfBlocks, numOfBlocks);

  // check that the object store has all blocks
  for (int i = 0; i < numOfBlocks; i++) {
    bool res = appState->hasBlock(i + 1);
    ASSERT_TRUE(res);
  }
}

TEST_F(RoRAppStateTest, PutBlocksStateExists) {
  SetClient();
  for (int i = numOfBlocks - 1; i >= 0; i--) {
    bool res = appState->putBlock(i + 1, blocks[i], kBlockSize);
    ASSERT_TRUE(res);
  }

  check_metadata(numOfBlocks, numOfBlocks);

  // check that the object store has all blocks
  for (int i = 0; i < numOfBlocks; i++) {
    bool res = appState->hasBlock(i + 1);
    ASSERT_TRUE(res);
  }

  // now send blocks 20 to 11, as the ST does
  for (int i = numOfBlocks * 2 - 1; i >= 10; i--) {
    bool res = appState->putBlock(i + 1, blocks[i], kBlockSize);
    ASSERT_TRUE(res);

    Sliver v1;
    Sliver v2;
    Status s = localClient->get(lastBlockIdKey, v1);
    ASSERT_TRUE(s.isOK());
    s = localClient->get(lastReachableBlockIdKey, v2);
    ASSERT_TRUE(s.isOK());

    ASSERT_EQ(numOfBlocks * 2, *(uint64_t *)v1.data());
    ASSERT_EQ(numOfBlocks * 2, appState->getLastBlockNum());
    if (i > numOfBlocks) {
      ASSERT_EQ(numOfBlocks, appState->getLastReachableBlockNum());
      ASSERT_EQ(numOfBlocks, *(uint64_t *)v2.data());
    } else {
      ASSERT_EQ(numOfBlocks * 2, appState->getLastReachableBlockNum());
      ASSERT_EQ(numOfBlocks * 2, *(uint64_t *)v2.data());
    }
  }

  check_metadata(numOfBlocks * 2, numOfBlocks * 2);
  // check that the object store has all blocks
  for (size_t i = 0; i < blocks.size(); i++) {
    bool res = appState->hasBlock(i + 1);
    ASSERT_TRUE(res);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto exitCode = RUN_ALL_TESTS();
  return exitCode;
}
