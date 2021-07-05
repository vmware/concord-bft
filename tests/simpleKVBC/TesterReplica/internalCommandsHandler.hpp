// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
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

#include "Logger.hpp"
#include "OpenTracing.hpp"
#include "sliver.hpp"
#include "simpleKVBTestsBuilder.hpp"
#include "db_interfaces.h"
#include "block_metadata.hpp"
#include "KVBCInterfaces.h"
#include <memory>
#include "ControlStateManager.hpp"
#include <chrono>
#include <thread>

static const std::string VERSIONED_KV_CAT_ID{"replica_tester_versioned_kv_category"};
static const std::string BLOCK_MERKLE_CAT_ID{"replica_tester_block_merkle_category"};

class InternalCommandsHandler : public concord::kvbc::ICommandsHandler {
 public:
  InternalCommandsHandler(concord::kvbc::IReader *storage,
                          concord::kvbc::IBlockAdder *blocksAdder,
                          concord::kvbc::IBlockMetadata *blockMetadata,
                          logging::Logger &logger)
      : m_storage(storage), m_blockAdder(blocksAdder), m_blockMetadata(blockMetadata), m_logger(logger) {}

  virtual void execute(ExecutionRequestsQueue &requests,
                       std::optional<Timestamp> timestamp,
                       const std::string &batchCid,
                       concordUtils::SpanWrapper &parent_span) override;

  void setPerformanceManager(std::shared_ptr<concord::performance::PerformanceManager> perfManager) override;

 private:
  bool executeWriteCommand(uint32_t requestSize,
                           const char *request,
                           uint64_t sequenceNum,
                           uint8_t flags,
                           size_t maxReplySize,
                           char *outReply,
                           uint32_t &outReplySize,
                           bool isBlockAccumulationEnabled,
                           concord::kvbc::categorization::VersionedUpdates &blockAccumulatedVerUpdates,
                           concord::kvbc::categorization::BlockMerkleUpdates &blockAccumulatedMerkleUpdates);

  bool executeReadOnlyCommand(uint32_t requestSize,
                              const char *request,
                              size_t maxReplySize,
                              char *outReply,
                              uint32_t &outReplySize,
                              uint32_t &specificReplicaInfoOutReplySize);

  bool verifyWriteCommand(uint32_t requestSize,
                          const BasicRandomTests::SimpleCondWriteRequest &request,
                          size_t maxReplySize,
                          uint32_t &outReplySize) const;

  bool executeReadCommand(
      uint32_t requestSize, const char *request, size_t maxReplySize, char *outReply, uint32_t &outReplySize);

  bool executeGetBlockDataCommand(
      uint32_t requestSize, const char *request, size_t maxReplySize, char *outReply, uint32_t &outReplySize);

  bool executeGetLastBlockCommand(uint32_t requestSize, size_t maxReplySize, char *outReply, uint32_t &outReplySize);

  void addMetadataKeyValue(concord::kvbc::categorization::VersionedUpdates &updates, uint64_t sequenceNum) const;

 private:
  static concordUtils::Sliver buildSliverFromStaticBuf(char *buf);
  std::optional<std::string> get(const std::string &key, concord::kvbc::BlockId blockId) const;
  std::string getAtMost(const std::string &key, concord::kvbc::BlockId blockId) const;
  std::string getLatest(const std::string &key) const;
  std::optional<concord::kvbc::BlockId> getLatestVersion(const std::string &key) const;
  std::optional<std::map<std::string, std::string>> getBlockUpdates(concord::kvbc::BlockId blockId) const;
  void writeAccumulatedBlock(ExecutionRequestsQueue &blockedRequests,
                             concord::kvbc::categorization::VersionedUpdates &verUpdates,
                             concord::kvbc::categorization::BlockMerkleUpdates &merkleUpdates);
  void addBlock(concord::kvbc::categorization::VersionedUpdates &verUpdates,
                concord::kvbc::categorization::BlockMerkleUpdates &merkleUpdates);
  void addKeys(BasicRandomTests::SimpleCondWriteRequest *writeReq,
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
};
