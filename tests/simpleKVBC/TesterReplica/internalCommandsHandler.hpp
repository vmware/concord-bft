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

class InternalControlHandlers : public bftEngine::ControlHandlers {
  bool stoppedOnSuperStableCheckpoint = false;
  bool stoppedOnStableCheckpoint = false;

 public:
  void onSuperStableCheckpoint() override { stoppedOnSuperStableCheckpoint = true; }
  void onStableCheckpoint() override { stoppedOnStableCheckpoint = true; }
  bool onPruningProcess() override { return false; }

  virtual ~InternalControlHandlers(){};
  bool haveYouStopped(uint64_t n_of_n) {
    return n_of_n == 1 ? stoppedOnSuperStableCheckpoint : stoppedOnStableCheckpoint;
  }
};
class InternalCommandsHandler : public concord::kvbc::ICommandsHandler {
 public:
  InternalCommandsHandler(concord::kvbc::IReader *storage,
                          concord::kvbc::IBlockAdder *blocksAdder,
                          concord::kvbc::IBlockMetadata *blockMetadata,
                          logging::Logger &logger)
      : m_storage(storage),
        m_blockAdder(blocksAdder),
        m_blockMetadata(blockMetadata),
        m_logger(logger),
        controlHandlers_(std::make_shared<InternalControlHandlers>()) {}

  virtual void execute(ExecutionRequestsQueue &requests,
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
                           uint32_t &outReplySize);

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

  bool executeHaveYouStoppedReadCommand(uint32_t requestSize,
                                        const char *request,
                                        size_t maxReplySize,
                                        char *outReply,
                                        uint32_t &outReplySize,
                                        uint32_t &specificReplicaInfoSize);
  bool executeGetLastBlockCommand(uint32_t requestSize, size_t maxReplySize, char *outReply, uint32_t &outReplySize);

  void addMetadataKeyValue(concord::kvbc::categorization::VersionedUpdates &updates, uint64_t sequenceNum) const;

  std::shared_ptr<bftEngine::ControlHandlers> getControlHandlers() override { return controlHandlers_; }

 private:
  static concordUtils::Sliver buildSliverFromStaticBuf(char *buf);
  std::optional<std::string> get(const std::string &key, concord::kvbc::BlockId blockId) const;
  std::string getAtMost(const std::string &key, concord::kvbc::BlockId blockId) const;
  std::string getLatest(const std::string &key) const;
  std::optional<concord::kvbc::BlockId> getLatestVersion(const std::string &key) const;
  std::optional<std::map<std::string, std::string>> getBlockUpdates(concord::kvbc::BlockId blockId) const;

 private:
  concord::kvbc::IReader *m_storage;
  concord::kvbc::IBlockAdder *m_blockAdder;
  concord::kvbc::IBlockMetadata *m_blockMetadata;
  logging::Logger &m_logger;
  size_t m_readsCounter = 0;
  size_t m_writesCounter = 0;
  size_t m_getLastBlockCounter = 0;
  std::shared_ptr<InternalControlHandlers> controlHandlers_;
  std::shared_ptr<concord::performance::PerformanceManager> perfManager_;
};
