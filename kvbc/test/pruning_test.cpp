// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include <bftengine/ControlStateManager.hpp>
#include "NullStateTransfer.hpp"
#include "categorization/kv_blockchain.h"
#include "categorization/updates.h"
#include "db_interfaces.h"
#include "endianness.hpp"
#include "pruning_handler.hpp"

#include "storage/test/storage_test_common.h"

#include <cassert>
#include <cstdint>
#include <exception>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using concord::storage::rocksdb::NativeClient;
using concord::kvbc::BlockId;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::pruning;
using concord::crypto::openssl::OpenSSLCryptoImpl;
using concord::crypto::cryptopp::RSA_SIGNATURE_LENGTH;
using concord::crypto::cryptopp::Crypto;
using concord::crypto::signature::SIGN_VERIFY_ALGO;
using bftEngine::ReplicaConfig;

namespace {
const NodeIdType replica_0 = 0;
const NodeIdType replica_1 = 1;
const NodeIdType replica_2 = 2;
const NodeIdType replica_3 = 3;
constexpr uint8_t noOfReplicas = 4U;

std::pair<std::string, std::string> keyPair[noOfReplicas];
bftEngine::ReplicaConfig &replicaConfig = bftEngine::ReplicaConfig::instance();
std::map<uint64_t, std::string> private_keys_of_replicas;

void setUpKeysConfiguration_4() {
  for (auto i = 0; i < noOfReplicas; ++i) {
    if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::RSA) {
      keyPair[i] = Crypto::instance().generateRsaKeyPair(RSA_SIGNATURE_LENGTH);
    } else if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::EDDSA) {
      keyPair[i] = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
    }
  }

  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_0, keyPair[0].second));
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_1, keyPair[1].second));
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_2, keyPair[2].second));
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_3, keyPair[3].second));
  private_keys_of_replicas[replica_0] = keyPair[0].first;
  private_keys_of_replicas[replica_1] = keyPair[1].first;
  private_keys_of_replicas[replica_2] = keyPair[2].first;
  private_keys_of_replicas[replica_3] = keyPair[3].first;
}

class test_rocksdb : public ::testing::Test {
  void SetUp() override {
    destroyDb();
    db = TestRocksDb::createNative();
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    db.reset();
    ASSERT_EQ(0, db.use_count());
    cleanup();
  }

 protected:
  std::shared_ptr<NativeClient> db;
};

const auto GENESIS_BLOCK_ID = BlockId{1};
const auto LAST_BLOCK_ID = BlockId{150};
const auto REPLICA_PRINCIPAL_ID_START = 0;
const auto CLIENT_PRINCIPAL_ID_START = 20000;
const std::uint32_t TICK_PERIOD_SECONDS = 1;
const std::uint64_t BATCH_BLOCKS_NUM = 60;

class TestStorage : public IReader, public IBlockAdder, public IBlocksDeleter {
 public:
  TestStorage(std::shared_ptr<::concord::storage::rocksdb::NativeClient> native_client)
      : bc_{native_client,
            false,
            std::map<std::string, CATEGORY_TYPE>{{kConcordReconfigurationCategoryId, CATEGORY_TYPE::versioned_kv},
                                                 {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}} {}

  // IBlockAdder interface
  BlockId add(categorization::Updates &&updates) override { return bc_.addBlock(std::move(updates)); }

  // IReader interface
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override {
    return bc_.get(category_id, key, block_id);
  }

  std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                 const std::string &key) const override {
    return bc_.getLatest(category_id, key);
  }

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override {
    bc_.multiGet(category_id, keys, versions, values);
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override {
    bc_.multiGetLatest(category_id, keys, values);
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override {
    bc_.multiGetLatestVersion(category_id, keys, versions);
  }

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override {
    return bc_.getLatestVersion(category_id, key);
  }

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override {
    return bc_.getBlockUpdates(block_id);
  }

  BlockId getGenesisBlockId() const override {
    if (mockGenesisBlockId.has_value()) return mockGenesisBlockId.value();
    return bc_.getGenesisBlockId();
  }

  BlockId getLastBlockId() const override { return bc_.getLastReachableBlockId(); }

  // IBlocksDeleter interface
  void deleteGenesisBlock() override {
    const auto genesisBlock = bc_.getGenesisBlockId();
    bc_.deleteBlock(genesisBlock);
  }

  BlockId deleteBlocksUntil(BlockId until, bool delete_files_in_range = false) override {
    const auto genesisBlock = bc_.getGenesisBlockId();
    if (genesisBlock == 0) {
      throw std::logic_error{"Cannot delete a block range from an empty blockchain"};
    } else if (until <= genesisBlock) {
      throw std::invalid_argument{"Invalid 'until' value passed to deleteBlocksUntil()"};
    }

    const auto lastReachableBlock = bc_.getLastReachableBlockId();
    const auto lastDeletedBlock = std::min(lastReachableBlock, until - 1);
    for (auto i = genesisBlock; i <= lastDeletedBlock; ++i) {
      ConcordAssert(bc_.deleteBlock(i));
    }
    return lastDeletedBlock;
  }

  void deleteLastReachableBlock() override { return bc_.deleteLastReachableBlock(); }

  void setGenesisBlockId(BlockId bid) { mockGenesisBlockId = bid; }

 private:
  concord::kvbc::categorization::KeyValueBlockchain bc_;
  std::optional<BlockId> mockGenesisBlockId = {};
};

class TestStateTransfer : public bftEngine::impl::NullStateTransfer {
 public:
  void addOnTransferringCompleteCallback(const std::function<void(uint64_t)> &callback,
                                         StateTransferCallBacksPriorities priority) override {
    callback_ = callback;
  }

  void complete() { callback_(0); }

 private:
  std::function<void(uint64_t)> callback_;
};

using ReplicaIDs = std::set<std::uint64_t>;
TestStateTransfer state_transfer;

void CheckLatestPrunableResp(const concord::messages::LatestPrunableBlock &latest_prunable_resp,
                             int replica_idx,
                             const PruningVerifier &verifier) {
  ASSERT_EQ(latest_prunable_resp.replica, replica_idx);
  ASSERT_TRUE(verifier.verify(latest_prunable_resp));
}

// This blockchain contains only versioned keys. TODO: add more categories

void InitBlockchainStorage(std::size_t replica_count,
                           TestStorage &s,
                           bool offset_ts = false,
                           bool empty_blockchain = false,
                           bool increment_now = false) {
  if (empty_blockchain) {
    ASSERT_EQ(s.getLastBlockId(), 0);
    return;
  }
  for (auto i = GENESIS_BLOCK_ID; i <= LAST_BLOCK_ID; ++i) {
    // To test the very basic functionality of pruning, we will override a single key over and over again.
    // Every previous versions should be deleted on pruning.
    concord::kvbc::categorization::VersionedUpdates versioned_updates;
    versioned_updates.addUpdate(std::string({0x20}), std::to_string(i));
    versioned_updates.calculateRootHash(false);

    concord::kvbc::categorization::Updates updates;
    updates.add(kConcordReconfigurationCategoryId, std::move(versioned_updates));

    BlockId blockId;
    blockId = s.add(std::move(updates));

    // Make sure we've inserted the correct block ID.
    ASSERT_EQ(i, blockId);

    // Make sure our storage mock works properly.
    {
      auto stored_val = s.get(kConcordReconfigurationCategoryId, std::string({0x20}), blockId);
      ASSERT_TRUE(stored_val.has_value());
      VersionedValue out = std::get<VersionedValue>(stored_val.value());
      ASSERT_EQ(blockId, out.block_id);
      ASSERT_EQ(std::to_string(i), out.data);
    }
  }

  // Make sure getLastBlock() works properly and it is equal to
  // LAST_BLOCK_ID.
  ASSERT_EQ(s.getLastBlockId(), LAST_BLOCK_ID);
}

concord::messages::PruneRequest ConstructPruneRequest(std::size_t client_idx,
                                                      const std::map<uint64_t, std::string> &private_keys,
                                                      BlockId min_prunable_block_id = LAST_BLOCK_ID,
                                                      std::uint32_t tick_period_seconds = TICK_PERIOD_SECONDS,
                                                      std::uint64_t batch_blocks_num = BATCH_BLOCKS_NUM) {
  concord::messages::PruneRequest prune_req;
  prune_req.sender = client_idx;
  uint64_t i = 0u;
  for (auto &[idx, pkey] : private_keys) {
    auto &latest_block = prune_req.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
    latest_block.replica = idx;
    // Send different block IDs.
    latest_block.block_id = min_prunable_block_id + i;

    auto block_signer = PruningSigner{pkey};
    block_signer.sign(latest_block);
    i++;
  }
  prune_req.tick_period_seconds = tick_period_seconds;
  prune_req.batch_blocks_num = batch_blocks_num;
  return prune_req;
}

TEST_F(test_rocksdb, sign_verify_correct) {
  const auto replica_count = 4;
  uint64_t sending_id = 0;
  uint64_t client_proxy_count = 4;
  const auto verifier = PruningVerifier{replicaConfig.publicKeysOfReplicas};
  std::vector<PruningSigner> signers;
  signers.reserve(replica_count);
  for (auto i = 0; i < replica_count; ++i) {
    signers.emplace_back(PruningSigner{private_keys_of_replicas[i]});
  }

  // Sign and verify a LatestPrunableBlock message.
  {
    concord::messages::LatestPrunableBlock block;
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    block.block_id = LAST_BLOCK_ID;
    signers[sending_id].sign(block);

    ASSERT_TRUE(verifier.verify(block));
  }

  // Sign and verify a PruneRequest message.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }
    ASSERT_TRUE(verifier.verify(request));
  }
}

TEST_F(test_rocksdb, verify_malformed_messages) {
  const auto replica_count = 4;
  const auto client_proxy_count = replica_count;
  const auto sending_id = 1;
  const auto verifier = PruningVerifier{replicaConfig.publicKeysOfReplicas};
  std::vector<PruningSigner> signers;
  signers.reserve(replica_count);
  for (auto i = 0; i < replica_count; ++i) {
    signers.emplace_back(PruningSigner{private_keys_of_replicas[i]});
  }

  // Break verification of LatestPrunableBlock messages.
  {
    concord::messages::LatestPrunableBlock block;
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    block.block_id = LAST_BLOCK_ID;
    signers[sending_id].sign(block);

    // Change the replica ID after signing.
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id + 1;
    ASSERT_FALSE(verifier.verify(block));

    // Make sure it works with the correct replica ID.
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    ASSERT_TRUE(verifier.verify(block));

    // Change the block ID after signing.
    block.block_id = LAST_BLOCK_ID + 1;
    ASSERT_FALSE(verifier.verify(block));

    // Make sure it works with the correct block ID.
    block.block_id = LAST_BLOCK_ID;
    ASSERT_TRUE(verifier.verify(block));

    // Change a single byte from the signature and make sure it doesn't verify.
    block.signature[0] += 1;
    ASSERT_FALSE(verifier.verify(block));
  }

  // Change the sender in PruneRequest after signing and verify.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }

    request.sender = request.sender + 1;

    ASSERT_TRUE(verifier.verify(request));
  }

  // Verify a PruneRequest with replica_count - 1 latest prunable blocks.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count - 1; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }

    ASSERT_FALSE(verifier.verify(request));
  }

  // Change replica in a single latest prunable block message after signing it
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }
    request.latest_prunable_block[0].replica = REPLICA_PRINCIPAL_ID_START + replica_count + 8;

    ASSERT_FALSE(verifier.verify(request));
  }

  // Invalid (zero) tick_period_seconds and batch_blocks_num.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = 0;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }
    ASSERT_FALSE(verifier.verify(request));
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = 0;
    ASSERT_FALSE(verifier.verify(request));
  }
}

TEST_F(test_rocksdb, sm_latest_prunable_request_correct_num_bocks_to_keep) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = 1;
  replicaConfig.pruningEnabled_ = true;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = PruningVerifier{replicaConfig.publicKeysOfReplicas};
  replicaConfig.replicaPrivateKey = keyPair[1].first;
  InitBlockchainStorage(replica_count, storage);

  // Construct the pruning state machine with a nullptr TimeContract to verify
  // it works in case the time service is disabled.
  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlock resp;
  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, {}, rres);
  resp = std::get<concord::messages::LatestPrunableBlock>(rres.response);
  CheckLatestPrunableResp(resp, replica_idx, verifier);
  ASSERT_EQ(resp.block_id, LAST_BLOCK_ID - num_blocks_to_keep);
}
TEST_F(test_rocksdb, sm_latest_prunable_request_big_num_blocks_to_keep) {
  const auto num_blocks_to_keep = LAST_BLOCK_ID + 42;
  const auto replica_idx = 1;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = 1;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = keyPair[1].first;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = PruningVerifier{replicaConfig.publicKeysOfReplicas};

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlock resp;
  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, {}, rres);
  resp = std::get<concord::messages::LatestPrunableBlock>(rres.response);
  CheckLatestPrunableResp(resp, replica_idx, verifier);
  // Verify that the returned block ID is 0 when pruning_num_blocks_to_keep is
  // bigger than the latest block ID.
  ASSERT_EQ(resp.block_id, 0);
}
// The blockchain created in this test is the same as the one in the
// sm_latest_prunable_request_time_range test.
TEST_F(test_rocksdb, sm_latest_prunable_request_no_pruning_conf) {
  uint64_t replica_count = 4;
  const auto num_blocks_to_keep = 0;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = 1;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = keyPair[1].first;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = PruningVerifier{replicaConfig.publicKeysOfReplicas};

  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::LatestPrunableBlock resp;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, {}, rres);
  resp = std::get<concord::messages::LatestPrunableBlock>(rres.response);
  CheckLatestPrunableResp(resp, 1, verifier);
  // Verify that when pruning is enabled and both pruning_num_blocks_to_keep and
  // duration_to_keep_minutes are set to 0, then LAST_BLOCK_ID will be returned.
  ASSERT_EQ(resp.block_id, LAST_BLOCK_ID);
}

TEST_F(test_rocksdb, sm_latest_prunable_request_pruning_disabled) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 0;
  const auto replica_idx = 1;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = false;
  replicaConfig.replicaPrivateKey = keyPair[1].first;
  auto storage = TestStorage(db);
  auto &blocks_deleter = storage;
  const auto verifier = PruningVerifier{replicaConfig.publicKeysOfReplicas};

  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::LatestPrunableBlock resp;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, {}, rres);

  // Verify that when pruning is disabled, there is no answer.
  ASSERT_EQ(std::holds_alternative<concord::messages::LatestPrunableBlock>(rres.response), false);
}
TEST_F(test_rocksdb, sm_handle_prune_request_on_pruning_disabled) {
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  const auto client_idx = 0;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = false;
  replicaConfig.replicaPrivateKey = keyPair[1].first;

  TestStorage storage(db);
  auto &blocks_deleter = storage;
  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  const auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
  concord::messages::ReconfigurationResponse rres;
  auto res = sm.handle(req, 0, UINT32_MAX, {}, rres);
  ASSERT_TRUE(res);
}
TEST_F(test_rocksdb, sm_handle_correct_prune_request) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  const auto client_idx = 5;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = keyPair[1].first;

  TestStorage storage(db);
  auto &blocks_deleter = storage;
  InitBlockchainStorage(replica_count, storage);
  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  const auto latest_prunable_block_id = storage.getLastBlockId() - num_blocks_to_keep;
  const auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas, latest_prunable_block_id);
  blocks_deleter.deleteBlocksUntil(latest_prunable_block_id + 1);
  concord::messages::ReconfigurationResponse rres;
  auto res = sm.handle(req, 0, UINT32_MAX, {}, rres);

  ASSERT_TRUE(res);
}

TEST_F(test_rocksdb, sm_handle_incorrect_prune_request) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  const auto client_idx = 5;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = keyPair[1].first;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  // Add a valid N + 1 latest prunable block.
  {
    auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
    const auto &block = req.latest_prunable_block[3];
    auto latest_prunnable_block = concord::messages::LatestPrunableBlock();
    latest_prunnable_block.block_id = block.block_id;
    latest_prunnable_block.replica = block.replica;
    latest_prunnable_block.signature = block.signature;
    req.latest_prunable_block.push_back(std::move(latest_prunnable_block));
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, {}, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }

  // Send N - 1 latest prunable blocks.
  {
    auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
    req.latest_prunable_block.pop_back();
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, {}, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }

  // Send a latest prunable block with an invalid signature.
  {
    auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
    auto &block = req.latest_prunable_block[req.latest_prunable_block.size() - 1];
    block.signature[0] += 1;
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, {}, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }
}
}  // namespace

int main(int argc, char **argv) {
  setUpKeysConfiguration_4();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
