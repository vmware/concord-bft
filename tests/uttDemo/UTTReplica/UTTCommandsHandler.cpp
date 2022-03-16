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

#include "UTTCommandsHandler.hpp"
#include "OpenTracing.hpp"
#include "assertUtils.hpp"
#include "sliver.hpp"
#include "kv_types.hpp"
#include "block_metadata.hpp"
#include "sha_hash.hpp"
#include <unistd.h>
#include <algorithm>
#include <variant>
#include "ReplicaConfig.hpp"
#include "kvbc_key_types.hpp"

using namespace bftEngine;
using namespace concord::kvbc::categorization;

using std::holds_alternative;
using std::runtime_error;
using std::string;

using concord::kvbc::BlockId;
using concord::kvbc::KeyValuePair;
using concord::storage::SetOfKeyValuePairs;
using skvbc::messages::SKVBCGetBlockDataRequest;
using skvbc::messages::SKVBCGetLastBlockReply;
using skvbc::messages::SKVBCGetLastBlockRequest;
using skvbc::messages::SKVBCReadReply;
using skvbc::messages::SKVBCReadRequest;
using skvbc::messages::SKVBCReply;
using skvbc::messages::SKVBCRequest;
using skvbc::messages::SKVBCWriteReply;
using skvbc::messages::SKVBCWriteRequest;

using Hasher = concord::util::SHA3_256;
using Hash = Hasher::Digest;

template <typename Span>
static Hash hash(const Span &span) {
  return Hasher{}.digest(span.data(), span.size());
}

void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue &requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string &batchCid,
                                 concordUtils::SpanWrapper &parent_span) {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO execute");
}

void UTTCommandsHandler::preExecute(IRequestsHandler::ExecutionRequest &req,
                                    std::optional<bftEngine::Timestamp> timestamp,
                                    const std::string &batchCid,
                                    concordUtils::SpanWrapper &parent_span) {}

OperationResult UTTCommandsHandler::verifyWriteCommand(uint32_t requestSize,
                                                       const uint8_t *request,
                                                       size_t maxReplySize,
                                                       uint32_t &outReplySize) const {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO verifyWriteCommand");
  return OperationResult::SUCCESS;
}

OperationResult UTTCommandsHandler::executeWriteCommand(uint32_t requestSize,
                                                        const char *request,
                                                        uint64_t sequenceNum,
                                                        uint8_t flags,
                                                        size_t maxReplySize,
                                                        char *outReply,
                                                        uint32_t &outReplySize,
                                                        bool isBlockAccumulationEnabled,
                                                        VersionedUpdates &blockAccumulatedVerUpdates,
                                                        BlockMerkleUpdates &blockAccumulatedMerkleUpdates) {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO executeWriteCommand");
  return OperationResult::SUCCESS;
}

OperationResult UTTCommandsHandler::executeGetBlockDataCommand(const SKVBCGetBlockDataRequest &request,
                                                               size_t maxReplySize,
                                                               char *outReply,
                                                               uint32_t &outReplySize) {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO executeGetBlockDataCommand");
  return OperationResult::SUCCESS;
}

OperationResult UTTCommandsHandler::executeReadCommand(const SKVBCReadRequest &request,
                                                       size_t maxReplySize,
                                                       char *outReply,
                                                       uint32_t &outReplySize) {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO executeReadCommand");
  return OperationResult::SUCCESS;
}

OperationResult UTTCommandsHandler::executeGetLastBlockCommand(size_t maxReplySize,
                                                               char *outReply,
                                                               uint32_t &outReplySize) {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO executeGetLastBlockCommand");
  return OperationResult::SUCCESS;
}

OperationResult UTTCommandsHandler::executeReadOnlyCommand(uint32_t requestSize,
                                                           const char *request,
                                                           size_t maxReplySize,
                                                           char *outReply,
                                                           uint32_t &outReplySize,
                                                           uint32_t &specificReplicaInfoOutReplySize) {
  LOG_INFO(m_logger, "UTTCommandsHandler: TODO executeReadOnlyCommand");
  return OperationResult::SUCCESS;
}