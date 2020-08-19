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

class InternalControlHandlers : public bftEngine::ControlHandlers {
  bool stoppedOnWedge = false;

 public:
  void onSuperStableCheckpoint() override { stoppedOnWedge = true; }
  virtual ~InternalControlHandlers(){};
  bool haveYouStopped() { return stoppedOnWedge; }
};
class InternalCommandsHandler : public concord::kvbc::ICommandsHandler {
 public:
  InternalCommandsHandler(concord::kvbc::ILocalKeyValueStorageReadOnly *storage,
                          concord::kvbc::IBlocksAppender *blocksAppender,
                          concord::kvbc::IBlockMetadata *blockMetadata,
                          logging::Logger &logger)
      : m_storage(storage),
        m_blocksAppender(blocksAppender),
        m_blockMetadata(blockMetadata),
        m_logger(logger),
        controlHandlers_(std::make_shared<InternalControlHandlers>()) {}

  virtual int execute(uint16_t clientId,
                      uint64_t sequenceNum,
                      uint8_t flags,
                      uint32_t requestSize,
                      const char *request,
                      uint32_t maxReplySize,
                      char *outReply,
                      uint32_t &outActualReplySize,
                      uint32_t &outActualReplicaSpecificInfoSize,
                      concordUtils::SpanWrapper &span) override;

  void setControlStateManager(std::shared_ptr<bftEngine::ControlStateManager> controlStateManager) override;

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

  void addMetadataKeyValue(concord::storage::SetOfKeyValuePairs &updates, uint64_t sequenceNum) const;

  std::shared_ptr<bftEngine::ControlHandlers> getControlHandlers() override { return controlHandlers_; }

 private:
  static concordUtils::Sliver buildSliverFromStaticBuf(char *buf);

 private:
  concord::kvbc::ILocalKeyValueStorageReadOnly *m_storage;
  concord::kvbc::IBlocksAppender *m_blocksAppender;
  concord::kvbc::IBlockMetadata *m_blockMetadata;
  logging::Logger &m_logger;
  size_t m_readsCounter = 0;
  size_t m_writesCounter = 0;
  size_t m_getLastBlockCounter = 0;
  std::shared_ptr<bftEngine::ControlStateManager> controlStateManager_;
  std::shared_ptr<InternalControlHandlers> controlHandlers_;
};
