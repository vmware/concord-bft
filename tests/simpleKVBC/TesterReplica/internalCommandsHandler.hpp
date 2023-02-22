// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "util/OpenTracing.hpp"
#include "util/sliver.hpp"
#include "db_interfaces.h"
#include "block_metadata.hpp"
#include "KVBCInterfaces.h"
#include <memory>
#include "ControlStateManager.hpp"
#include <chrono>
#include <thread>

#include "log/logger.hpp"
#include "skvbc_messages.cmf.hpp"
#include "SharedTypes.hpp"
#include "categorization/db_categories.h"
#include "kvbc_adapter/replica_adapter.hpp"

static const std::string VERSIONED_KV_CAT_ID{concord::kvbc::categorization::kExecutionPrivateCategory};
static const std::string BLOCK_MERKLE_CAT_ID{concord::kvbc::categorization::kExecutionProvableCategory};
static constexpr const char *clientReplyStateCategory = "client_state";
static const std::string CLIENT_STATE_CAT_ID{clientReplyStateCategory};

class InternalCommandsHandler : public concord::kvbc::ICommandsHandler {
 public:
  InternalCommandsHandler(concord::kvbc::IReader *storage,
                          concord::kvbc::IBlockAdder *blocksAdder,
                          concord::kvbc::IBlockMetadata *blockMetadata,
                          logging::Logger &logger,
                          bftEngine::IStateTransfer &st,
                          bool addAllKeysAsPublic = false,
                          concord::kvbc::adapter::ReplicaBlockchain *kvbc = nullptr);

  void execute(ExecutionRequestsQueue &requests,
               std::optional<bftEngine::Timestamp> timestamp,
               const std::string &batchCid,
               concordUtils::SpanWrapper &parent_span) override;

  void preExecute(IRequestsHandler::ExecutionRequest &req,
                  std::optional<bftEngine::Timestamp> timestamp,
                  const std::string &batchCid,
                  concordUtils::SpanWrapper &parent_span) override;

  void setPerformanceManager(std::shared_ptr<concord::performance::PerformanceManager> perfManager) override;

 private:
  void add(std::string &&key,
           std::string &&value,
           concord::kvbc::categorization::VersionedUpdates &,
           concord::kvbc::categorization::BlockMerkleUpdates &) const;
  /**
   *
   * @param requestSize
   * @param request
   * @param sequenceNum
   * @param flags
   * @param maxReplySize
   * @param outReply
   * @param outReplySize
   * @param isBlockAccumulationEnabled
   * @param blockAccumulatedVerUpdates
   * @param blockAccumulatedMerkleUpdates
   * @param batchCid - The id of the request batch this request belongs to.
   *                   This information is not available in the execution layer.
   *                   The test client sets it as the Cid of the request to make it so.
   * @param requestId - The id of the request old ids are rejected if a request with a higher id
   *                    clientId was perviously executed for a request originating in clientId.
   * @param clientId - The id of the client who issued the request, used in conjunction with requestCid to prevent
   *                   the execution of old requests.
   *
   * @note  Batched client requests are expected to reach execution only once,
   *        in the current implementation, the preprocessor is the only entity aware of client request
   *        batches and is responsible for satisfying this assumption
   * @return
   */
  bftEngine::OperationResult executeWriteCommand(
      uint32_t requestSize,
      const char *request,
      uint64_t sequenceNum,
      uint8_t flags,
      size_t maxReplySize,
      char *outReply,
      uint32_t &outReplySize,
      bool isBlockAccumulationEnabled,
      concord::kvbc::categorization::VersionedUpdates &blockAccumulatedVerUpdates,
      concord::kvbc::categorization::BlockMerkleUpdates &blockAccumulatedMerkleUpdates,
      uint64_t batchCid,
      uint64_t requestId,
      uint16_t clientId);

  bftEngine::OperationResult executeReadOnlyCommand(uint32_t requestSize,
                                                    const char *request,
                                                    size_t maxReplySize,
                                                    char *outReply,
                                                    uint32_t &outReplySize,
                                                    uint32_t &specificReplicaInfoOutReplySize);

  bftEngine::OperationResult verifyWriteCommand(uint32_t requestSize,
                                                const uint8_t *request,
                                                size_t maxReplySize,
                                                uint32_t &outReplySize) const;

  bftEngine::OperationResult executeReadCommand(const skvbc::messages::SKVBCReadRequest &request,
                                                size_t maxReplySize,
                                                char *outReply,
                                                uint32_t &outReplySize);

  bftEngine::OperationResult executeGetBlockDataCommand(const skvbc::messages::SKVBCGetBlockDataRequest &request,
                                                        size_t maxReplySize,
                                                        char *outReply,
                                                        uint32_t &outReplySize);

  bftEngine::OperationResult executeGetLastBlockCommand(size_t maxReplySize, char *outReply, uint32_t &outReplySize);

  void addMetadataKeyValue(concord::kvbc::categorization::VersionedUpdates &updates, uint64_t sequenceNum) const;

 private:
  std::optional<std::string> get(const std::string &key, concord::kvbc::BlockId blockId) const;
  std::string getAtMost(const std::string &key, concord::kvbc::BlockId blockId) const;
  std::string getLatest(const std::string &key) const;
  std::optional<concord::kvbc::BlockId> getLatestVersion(const std::string &key) const;
  std::optional<std::map<std::string, std::string>> getBlockUpdates(concord::kvbc::BlockId blockId) const;
  void writeAccumulatedBlock(ExecutionRequestsQueue &blockedRequests,
                             concord::kvbc::categorization::VersionedUpdates &verUpdates,
                             concord::kvbc::categorization::BlockMerkleUpdates &merkleUpdates,
                             uint64_t sn);
  void addBlock(concord::kvbc::categorization::VersionedUpdates &verUpdates,
                concord::kvbc::categorization::BlockMerkleUpdates &merkleUpdates,
                concord::kvbc::categorization::VersionedUpdates &clientStateUpdates,
                uint64_t sn);
  void addKeys(const skvbc::messages::SKVBCWriteRequest &writeReq,
               uint64_t sequenceNum,
               concord::kvbc::categorization::VersionedUpdates &verUpdates,
               concord::kvbc::categorization::BlockMerkleUpdates &merkleUpdates);
  bool hasConflictInBlockAccumulatedRequests(
      const std::string &key,
      concord::kvbc::categorization::VersionedUpdates &blockAccumulatedVerUpdates,
      concord::kvbc::categorization::BlockMerkleUpdates &blockAccumulatedMerkleUpdates) const;

 private:
  concord::kvbc::IReader *m_storage;
  concord::kvbc::IBlockAdder *m_blockAdder;
  concord::kvbc::IBlockMetadata *m_blockMetadata;
  logging::Logger &m_logger;
  size_t m_readsCounter = 0;
  size_t m_writesCounter = 0;
  size_t m_getLastBlockCounter = 0;
  std::shared_ptr<concord::performance::PerformanceManager> perfManager_;
  bool m_addAllKeysAsPublic{false};  // Add all key-values in the block merkle category as public ones.
  concord::kvbc::adapter::ReplicaBlockchain *m_kvbc{nullptr};
  std::unordered_map<uint16_t, uint64_t> m_clientToMaxExecutedReqId;

  // This string is used by clients to distinguish blocks that should be ignored by them.
  // Some tests expect every block to be created by a request issued by test clients.
  // However, internal communication between replicas can also create blocks, e.g:
  // When rotating keys.
  static constexpr const char *s_ignoreBlockStr = "ignoreBlock";
};
