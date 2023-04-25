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
#include "util/endianness.hpp"
#include "pruning_handler.hpp"
#include "SigManager.hpp"
#include "CryptoManager.hpp"
#include "helper.hpp"
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
using bftEngine::ReplicaConfig;
using bftEngine::impl::SigManager;
using bftEngine::CryptoManager;

namespace {

const auto GENESIS_BLOCK_ID = BlockId{1};
const auto LAST_BLOCK_ID = BlockId{150};
const auto REPLICA_PRINCIPAL_ID_START = 0;
const auto CLIENT_PRINCIPAL_ID_START = 20000;

class test_rocksdb : public ::testing::Test {
  static constexpr const uint8_t noOfReplicas = 4U;

  void SetUp() override {
    GL.setLogLevel(logging::DEBUG_LOG_LEVEL);
    setUpKeysConfiguration_4();
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
  ReplicaConfig &replicaConfig = ReplicaConfig::instance();
  std::map<uint64_t, std::string> private_keys_of_replicas;
  std::array<std::unique_ptr<ReplicasInfo>, noOfReplicas> replicasInfo;
  std::array<std::shared_ptr<SigManager>, noOfReplicas> sigManagers;
  std::array<std::shared_ptr<CryptoManager>, noOfReplicas> cryptoManagers;
  static const std::pair<std::string, std::string> s_eddsaKeyPairs[noOfReplicas];

  void setUpKeysConfiguration_4() {
    replicaConfig.replicaId = 0;
    replicaConfig.numReplicas = 4;
    replicaConfig.fVal = 1;
    replicaConfig.cVal = 0;
    replicaConfig.numOfClientProxies = 4;
    replicaConfig.numOfExternalClients = 15;

    std::vector<std::string> hexPublicKeys(noOfReplicas);
    replicaConfig.publicKeysOfReplicas.clear();
    for (auto i = 0; i < noOfReplicas; ++i) {
      hexPublicKeys[i] = s_eddsaKeyPairs[i].second;
      private_keys_of_replicas.emplace(i, s_eddsaKeyPairs[i].first);
      replicaConfig.publicKeysOfReplicas.emplace(i, hexPublicKeys[i]);
    }

    for (auto i = 0; i < noOfReplicas; i++) {
      replicasInfo[i] = std::make_unique<ReplicasInfo>(replicaConfig, true, true);
      sigManagers[i] = SigManager::init(i,
                                        private_keys_of_replicas.at(i),
                                        replicaConfig.publicKeysOfReplicas,
                                        concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                        nullptr,
                                        concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                        {},
                                        *replicasInfo[i].get());
      cryptoManagers[i] = CryptoManager::init(
          std::make_unique<TestMultisigCryptoSystem>(i, hexPublicKeys, private_keys_of_replicas.at(i)));
    }
  }

  void switchReplicaContext(NodeIdType id) {
    LOG_INFO(GL, "Switching replica context to replica " << id);
    SigManager::reset(sigManagers[id]);
    CryptoManager::reset(cryptoManagers[id]);
  }

  void signAs(NodeIdType id, concord::messages::LatestPrunableBlock &block) {
    switchReplicaContext(id);
    PruningSigner{}.sign(block);
  }

  concord::messages::PruneRequest ConstructPruneRequest(std::size_t client_idx,
                                                        BlockId min_prunable_block_id = LAST_BLOCK_ID) {
    concord::messages::PruneRequest prune_req;
    prune_req.sender = client_idx;

    for (uint64_t i = 0u; i < noOfReplicas; i++) {
      auto &latest_block = prune_req.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      latest_block.replica = i;
      // Send different block IDs.
      latest_block.block_id = min_prunable_block_id + i;

      signAs(i, latest_block);
    }
    return prune_req;
  }

  std::shared_ptr<NativeClient> db;
};

// clang-format off
const std::pair<std::string, std::string> test_rocksdb::s_eddsaKeyPairs[noOfReplicas] = {
    {"61498efe1764b89357a02e2887d224154006ceacf26269f8695a4af561453eef",
        "386f4fb049a5d8bb0706d3793096c8f91842ce380dfc342a2001d50dfbc901f4"},
    {"247a74ab3620ec6b9f5feab9ee1f86521da3fa2804ad45bb5bf2c5b21ef105bc",
        "3f9e7dbde90477c24c1bacf14e073a356c1eca482d352d9cc0b16560a4e7e469"},
    {"fb539bc3d66deda55524d903da26dbec1f4b6abf41ec5db521e617c64eb2c341",
        "2311c6013ff657844669d8b803b2e1ed33fe06eed445f966a800a8fbb8d790e8"},
    {"55ea66e855b83ec4a02bd8fcce6bb4426ad3db2a842fa2a2a6777f13e40a4717",
        "1ba7449655784fc9ce193a7887de1e4d3d35f7c82b802440c4f28bf678a34b34"},
};
// clang-format on

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

TEST_F(test_rocksdb, sign_verify_correct) {
  const auto replica_count = 4;
  uint64_t sending_id = 0;
  uint64_t client_proxy_count = 4;
  const auto verifier = PruningVerifier{};

  // Sign and verify a LatestPrunableBlock message.
  {
    concord::messages::LatestPrunableBlock block;
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    block.block_id = LAST_BLOCK_ID;

    signAs(sending_id, block);

    ASSERT_TRUE(verifier.verify(block));
  }

  // Sign and verify a PruneRequest message.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signAs(i, block);
    }
    ASSERT_TRUE(verifier.verify(request));
  }
}

TEST_F(test_rocksdb, verify_malformed_messages) {
  const auto replica_count = 4;
  const auto client_proxy_count = replica_count;
  const auto sending_id = 1;
  const auto verifier = PruningVerifier{};

  // Break verification of LatestPrunableBlock messages.
  {
    concord::messages::LatestPrunableBlock block;
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    block.block_id = LAST_BLOCK_ID;
    signAs(sending_id, block);

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
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signAs(i, block);
    }

    request.sender = request.sender + 1;

    ASSERT_TRUE(verifier.verify(request));
  }

  // Verify a PruneRequest with replica_count - 1 latest prunable blocks.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    for (auto i = 0; i < replica_count - 1; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signAs(i, block);
    }

    ASSERT_FALSE(verifier.verify(request));
  }

  // Change replica in a single latest prunable block message after signing it
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signAs(i, block);
    }
    request.latest_prunable_block[0].replica = REPLICA_PRINCIPAL_ID_START + replica_count + 8;

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
  const auto verifier = PruningVerifier{};
  switchReplicaContext(replica_idx);
  InitBlockchainStorage(replica_count, storage);

  // Construct the pruning state machine with a nullptr TimeContract to verify
  // it works in case the time service is disabled.
  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

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
  replicaConfig.replicaPrivateKey = s_eddsaKeyPairs[1].first;
  switchReplicaContext(1);
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = PruningVerifier{};

  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

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
  replicaConfig.replicaPrivateKey = s_eddsaKeyPairs[1].first;
  switchReplicaContext(1);
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = PruningVerifier{};

  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

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
  replicaConfig.replicaPrivateKey = s_eddsaKeyPairs[1].first;
  auto storage = TestStorage(db);
  auto &blocks_deleter = storage;

  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

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
  replicaConfig.replicaPrivateKey = s_eddsaKeyPairs[1].first;

  TestStorage storage(db);
  auto &blocks_deleter = storage;
  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

  const auto req = ConstructPruneRequest(client_idx);
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
  replicaConfig.replicaPrivateKey = s_eddsaKeyPairs[1].first;

  TestStorage storage(db);
  auto &blocks_deleter = storage;
  InitBlockchainStorage(replica_count, storage);
  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

  const auto latest_prunable_block_id = storage.getLastBlockId() - num_blocks_to_keep;
  const auto req = ConstructPruneRequest(client_idx, latest_prunable_block_id);
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
  replicaConfig.replicaPrivateKey = s_eddsaKeyPairs[1].first;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{ReplicaConfig::instance().pathToOperatorPublicKey_,
                           ReplicaConfig::instance().operatorMsgSigningAlgo,
                           storage,
                           storage,
                           blocks_deleter,
                           false};

  // Add a valid N + 1 latest prunable block.
  {
    auto req = ConstructPruneRequest(client_idx);
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
    auto req = ConstructPruneRequest(client_idx);
    req.latest_prunable_block.pop_back();
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, {}, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }

  // Send a latest prunable block with an invalid signature.
  {
    auto req = ConstructPruneRequest(client_idx);
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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
