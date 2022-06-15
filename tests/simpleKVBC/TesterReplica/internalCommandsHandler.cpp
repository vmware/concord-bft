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

#include "internalCommandsHandler.hpp"
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

const uint64_t LONG_EXEC_CMD_TIME_IN_SEC = 11;

template <typename Span>
static Hash createHash(const Span &span) {
  return Hasher{}.digest(span.data(), span.size());
}

static const std::string &keyHashToCategory(const Hash &keyHash) {
  // If the most significant bit of a key's hash is set, use the VersionedKeyValueCategory. Otherwise, use the
  // BlockMerkleCategory.
  if (keyHash[0] & 0x80) {
    return VERSIONED_KV_CAT_ID;
  }
  return BLOCK_MERKLE_CAT_ID;
}

static const std::string &keyToCategory(const std::string &key) { return keyHashToCategory(createHash(key)); }

void InternalCommandsHandler::add(std::string &&key,
                                  std::string &&value,
                                  VersionedUpdates &verUpdates,
                                  BlockMerkleUpdates &merkleUpdates) const {
  // Add all key-values in the block merkle category as public ones.
  if (m_addAllKeysAsPublic) {
    merkleUpdates.addUpdate(std::move(key), std::move(value));
  } else {
    const auto &cat = keyToCategory(key);
    if (cat == VERSIONED_KV_CAT_ID) {
      verUpdates.addUpdate(std::move(key), std::move(value));
      return;
    }
    merkleUpdates.addUpdate(std::move(key), std::move(value));
  }
}

void InternalCommandsHandler::execute(InternalCommandsHandler::ExecutionRequestsQueue &requests,
                                      std::optional<bftEngine::Timestamp> timestamp,
                                      const std::string &batchCid,
                                      concordUtils::SpanWrapper &parent_span) {
  if (requests.empty()) return;
  if (requests.back().flags & bftEngine::DB_CHECKPOINT_FLAG) return;

  // To handle block accumulation if enabled
  VersionedUpdates verUpdates;
  BlockMerkleUpdates merkleUpdates;
  uint64_t sequenceNum = 0;

  for (auto &req : requests) {
    if (req.outExecutionStatus != static_cast<uint32_t>(OperationResult::UNKNOWN))
      continue;  // Request already executed (internal)
    req.outReplicaSpecificInfoSize = 0;
    OperationResult res;
    if (req.requestSize <= 0) {
      LOG_ERROR(m_logger, "Received size-0 request.");
      req.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
      continue;
    }
    bool readOnly = req.flags & MsgFlag::READ_ONLY_FLAG;
    if (readOnly) {
      res = executeReadOnlyCommand(req.requestSize,
                                   req.request,
                                   req.maxReplySize,
                                   req.outReply,
                                   req.outActualReplySize,
                                   req.outReplicaSpecificInfoSize);
    } else {
      // Only if requests size is greater than 1 and other conditions are met, block accumulation is enabled.
      bool isBlockAccumulationEnabled =
          ((requests.size() > 1) && (req.flags & bftEngine::MsgFlag::HAS_PRE_PROCESSED_FLAG));
      sequenceNum = req.executionSequenceNum;
      res = executeWriteCommand(req.requestSize,
                                req.request,
                                req.executionSequenceNum,
                                req.flags,
                                req.maxReplySize,
                                req.outReply,
                                req.outActualReplySize,
                                isBlockAccumulationEnabled,
                                verUpdates,
                                merkleUpdates);
    }
    if (res != OperationResult::SUCCESS) LOG_WARN(m_logger, "Command execution failed!");

    // This is added, as Apollo test sets req.outExecutionStatus to an error to verify that the error reply gets
    // returned.
    if (req.outExecutionStatus == static_cast<uint32_t>(OperationResult::UNKNOWN)) {
      req.outExecutionStatus = static_cast<uint32_t>(res);
    }
  }

  if (merkleUpdates.size() > 0 || verUpdates.size() > 0) {
    // Write Block accumulated requests
    writeAccumulatedBlock(requests, verUpdates, merkleUpdates, sequenceNum);
  }
}

void InternalCommandsHandler::preExecute(IRequestsHandler::ExecutionRequest &req,
                                         std::optional<bftEngine::Timestamp> timestamp,
                                         const std::string &batchCid,
                                         concordUtils::SpanWrapper &parent_span) {
  if (req.flags & bftEngine::DB_CHECKPOINT_FLAG) return;

  OperationResult res;
  if (req.requestSize <= 0) {
    LOG_ERROR(m_logger, "Received size-0 request.");
    req.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
    return;
  }
  const uint8_t *request_buffer_as_uint8 = reinterpret_cast<const uint8_t *>(req.request);
  bool readOnly = req.flags & MsgFlag::READ_ONLY_FLAG;
  if (readOnly) {
    LOG_ERROR(m_logger,
              "Received READ command for pre execution: "
                  << "seqNum=" << req.executionSequenceNum << " batchCid" << batchCid);
    return;
  }
  res = verifyWriteCommand(req.requestSize, request_buffer_as_uint8, req.maxReplySize, req.outActualReplySize);
  if (res != OperationResult::SUCCESS) {
    LOG_INFO(GL, "Operation result is not success in verifying write command");
  } else {
    SKVBCRequest deserialized_request;
    deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + req.requestSize, deserialized_request);
    const SKVBCWriteRequest &write_req = std::get<SKVBCWriteRequest>(deserialized_request.request);
    LOG_INFO(m_logger,
             "Pre execute WRITE command:"
                 << " type=SKVBCWriteRequest seqNum=" << req.executionSequenceNum
                 << " numOfWrites=" << write_req.writeset.size() << " numOfKeysInReadSet=" << write_req.readset.size()
                 << " readVersion=" << write_req.read_version);
    if (write_req.long_exec) {
      sleep(LONG_EXEC_CMD_TIME_IN_SEC);
    }
  }
  req.outActualReplySize = req.requestSize;
  memcpy(req.outReply, req.request, req.requestSize);
  if (req.outExecutionStatus == static_cast<uint32_t>(OperationResult::UNKNOWN)) {
    req.outExecutionStatus = static_cast<uint32_t>(res);
  }
}

void InternalCommandsHandler::addMetadataKeyValue(VersionedUpdates &updates, uint64_t sequenceNum) const {
  updates.addUpdate(
      std::string{concord::kvbc::IBlockMetadata::kBlockMetadataKeyStr},
      concord::kvbc::categorization::VersionedUpdates::Value{m_blockMetadata->serialize(sequenceNum), true});
}

std::optional<std::string> InternalCommandsHandler::get(const std::string &key, BlockId blockId) const {
  const auto v = m_storage->get(keyToCategory(key), key, blockId);
  if (!v) {
    return std::nullopt;
  }
  return std::visit([](const auto &v) { return v.data; }, *v);
}

std::string InternalCommandsHandler::getAtMost(const std::string &key, BlockId current) const {
  if (m_storage->getLastBlockId() == 0 || m_storage->getGenesisBlockId() == 0 || current == 0) {
    return std::string();
  }

  auto value = std::string();
  do {
    const auto v = get(key, current);
    if (v) {
      value = *v;
      break;
    }
    --current;
  } while (current);
  return value;
}

std::string InternalCommandsHandler::getLatest(const std::string &key) const {
  const auto v = m_storage->getLatest(keyToCategory(key), key);
  if (!v) {
    return std::string();
  }
  return std::visit([](const auto &v) { return v.data; }, *v);
}

std::optional<BlockId> InternalCommandsHandler::getLatestVersion(const std::string &key) const {
  const auto v = m_storage->getLatestVersion(keyToCategory(key), key);
  if (!v) {
    return std::nullopt;
  }
  // We never delete keys in TesterReplica at that stage.
  ConcordAssert(!v->deleted);
  return v->version;
}

std::optional<std::map<std::string, std::string>> InternalCommandsHandler::getBlockUpdates(
    concord::kvbc::BlockId blockId) const {
  const auto updates = m_storage->getBlockUpdates(blockId);
  if (!updates) {
    return std::nullopt;
  }

  auto ret = std::map<std::string, std::string>{};

  {
    const auto verUpdates = updates->categoryUpdates(VERSIONED_KV_CAT_ID);
    if (verUpdates) {
      const auto &u = std::get<VersionedInput>(verUpdates->get());
      for (const auto &[key, valueWithFlags] : u.kv) {
        ret[key] = valueWithFlags.data;
      }
    }
  }

  {
    const auto merkleUpdates = updates->categoryUpdates(BLOCK_MERKLE_CAT_ID);
    if (merkleUpdates) {
      const auto &u = std::get<BlockMerkleInput>(merkleUpdates->get());
      for (const auto &[key, value] : u.kv) {
        ret[key] = value;
      }
    }
  }

  return ret;
}

void InternalCommandsHandler::writeAccumulatedBlock(ExecutionRequestsQueue &blockedRequests,
                                                    VersionedUpdates &verUpdates,
                                                    BlockMerkleUpdates &merkleUpdates,
                                                    uint64_t sn) {
  // Only block accumulated requests will be processed here
  BlockId currBlock = m_storage->getLastBlockId();

  for (auto &req : blockedRequests) {
    if (req.flags & bftEngine::MsgFlag::HAS_PRE_PROCESSED_FLAG) {
      SKVBCReply reply;
      size_t existing_reply_size = req.outActualReplySize;
      static_assert(sizeof(*(req.outReply)) == sizeof(uint8_t),
                    "Byte pointer type used by bftEngine::IRequestsHandler::ExecutionRequest is incompatible with byte "
                    "pointer type used by CMF.");
      const uint8_t *reply_buffer_as_uint8 = reinterpret_cast<uint8_t *>(req.outReply);
      deserialize(reply_buffer_as_uint8, reply_buffer_as_uint8 + req.outActualReplySize, reply);
      SKVBCWriteReply &write_rep = std::get<SKVBCWriteReply>(reply.reply);
      write_rep.latest_block = currBlock + 1;
      vector<uint8_t> serialized_reply;
      serialize(serialized_reply, reply);

      // We expect modifying the value of latest_block in the SKVBCWriteReply
      // will not alter the length of its serialization.
      ConcordAssert(existing_reply_size == serialized_reply.size());

      copy(serialized_reply.begin(), serialized_reply.end(), req.outReply);
      LOG_INFO(
          m_logger,
          "SKVBCWrite message handled; writesCounter=" << m_writesCounter << " currBlock=" << write_rep.latest_block);
    }
  }
  addBlock(verUpdates, merkleUpdates, sn);
}

OperationResult InternalCommandsHandler::verifyWriteCommand(uint32_t requestSize,
                                                            const uint8_t *request,
                                                            size_t maxReplySize,
                                                            uint32_t &outReplySize) const {
  SKVBCRequest deserialized_request;
  try {
    deserialize(request, request + requestSize, deserialized_request);
  } catch (const runtime_error &e) {
    LOG_ERROR(m_logger, "Failed to deserialize SKVBCRequest: " << e.what());
    return OperationResult::INTERNAL_ERROR;
  }
  if (!holds_alternative<SKVBCWriteRequest>(deserialized_request.request)) {
    LOG_ERROR(m_logger, "Received an SKVBCRequest other than an SKVBCWriteRequest but not marked as read-only.");
    return OperationResult::INVALID_REQUEST;
  }

  if (maxReplySize < outReplySize) {
    LOG_ERROR(m_logger, "replySize is too big: replySize=" << outReplySize << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  return OperationResult::SUCCESS;
}

void InternalCommandsHandler::addKeys(const SKVBCWriteRequest &writeReq,
                                      uint64_t sequenceNum,
                                      VersionedUpdates &verUpdates,
                                      BlockMerkleUpdates &merkleUpdates) {
  for (size_t i = 0; i < writeReq.writeset.size(); i++) {
    static_assert(
        (sizeof(*(writeReq.writeset[i].first.data())) == sizeof(string::value_type)) &&
            (sizeof(*(writeReq.writeset[i].second.data())) == sizeof(string::value_type)),
        "Byte pointer type used by concord::kvbc::categorization::VersionedUpdates and/or "
        "concord::kvbc::categorization::BlockMerkleUpdates is incompatible with byte pointer type used by CMF.");
    add(string(reinterpret_cast<const string::value_type *>(writeReq.writeset[i].first.data()),
               writeReq.writeset[i].first.size()),
        string(reinterpret_cast<const string::value_type *>(writeReq.writeset[i].second.data()),
               writeReq.writeset[i].second.size()),
        verUpdates,
        merkleUpdates);
  }
  addMetadataKeyValue(verUpdates, sequenceNum);
}

void InternalCommandsHandler::addBlock(VersionedUpdates &verUpdates, BlockMerkleUpdates &merkleUpdates, uint64_t sn) {
  BlockId currBlock = m_storage->getLastBlockId();
  Updates updates;

  // Add all key-values in the block merkle category as public ones.
  auto internal_updates = VersionedUpdates{};
  if (m_addAllKeysAsPublic) {
    ConcordAssertNE(m_kvbc, nullptr);
    auto public_state = m_kvbc->getPublicStateKeys();
    if (!public_state) {
      public_state = PublicStateKeys{};
    }
    for (const auto &[k, _] : merkleUpdates.getData().kv) {
      (void)_;
      // We don't allow duplicates.
      auto it = std::lower_bound(public_state->keys.cbegin(), public_state->keys.cend(), k);
      if (it != public_state->keys.cend() && *it == k) {
        continue;
      }
      // We always persist public state keys in sorted order.
      public_state->keys.push_back(k);
      std::sort(public_state->keys.begin(), public_state->keys.end());
    }
    const auto public_state_ser = detail::serialize(*public_state);

    internal_updates.addUpdate(std::string{concord::kvbc::keyTypes::state_public_key_set},
                               std::string{public_state_ser.cbegin(), public_state_ser.cend()});
  }
  addMetadataKeyValue(internal_updates, sn);
  updates.add(kConcordInternalCategoryId, std::move(internal_updates));
  updates.add(VERSIONED_KV_CAT_ID, std::move(verUpdates));
  updates.add(BLOCK_MERKLE_CAT_ID, std::move(merkleUpdates));
  const auto newBlockId = m_blockAdder->add(std::move(updates));
  ConcordAssert(newBlockId == currBlock + 1);
}

bool InternalCommandsHandler::hasConflictInBlockAccumulatedRequests(
    const std::string &key,
    VersionedUpdates &blockAccumulatedVerUpdates,
    BlockMerkleUpdates &blockAccumulatedMerkleUpdates) const {
  auto itVersionUpdates = blockAccumulatedVerUpdates.getData().kv.find(key);
  if (itVersionUpdates != blockAccumulatedVerUpdates.getData().kv.end()) {
    return true;
  }

  auto itMerkleUpdates = blockAccumulatedMerkleUpdates.getData().kv.find(key);
  if (itMerkleUpdates != blockAccumulatedMerkleUpdates.getData().kv.end()) {
    return true;
  }
  return false;
}

OperationResult InternalCommandsHandler::executeWriteCommand(uint32_t requestSize,
                                                             const char *request,
                                                             uint64_t sequenceNum,
                                                             uint8_t flags,
                                                             size_t maxReplySize,
                                                             char *outReply,
                                                             uint32_t &outReplySize,
                                                             bool isBlockAccumulationEnabled,
                                                             VersionedUpdates &blockAccumulatedVerUpdates,
                                                             BlockMerkleUpdates &blockAccumulatedMerkleUpdates) {
  static_assert(sizeof(*request) == sizeof(uint8_t),
                "Byte pointer type used by bftEngine::IRequestsHandler::ExecutionRequest is incompatible with byte "
                "pointer type used by CMF.");
  const uint8_t *request_buffer_as_uint8 = reinterpret_cast<const uint8_t *>(request);
  if (!(flags & MsgFlag::HAS_PRE_PROCESSED_FLAG)) {
    auto res = verifyWriteCommand(requestSize, request_buffer_as_uint8, maxReplySize, outReplySize);
    if (res != OperationResult::SUCCESS) {
      LOG_INFO(GL, "Operation result is not success in verifying write command");
      return res;
    }
  }
  SKVBCRequest deserialized_request;
  deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + requestSize, deserialized_request);
  const SKVBCWriteRequest &write_req = std::get<SKVBCWriteRequest>(deserialized_request.request);
  LOG_INFO(m_logger,
           "Execute WRITE command:"
               << " type=SKVBCWriteRequest seqNum=" << sequenceNum << " numOfWrites=" << write_req.writeset.size()
               << " numOfKeysInReadSet=" << write_req.readset.size() << " readVersion=" << write_req.read_version
               << " READ_ONLY_FLAG=" << ((flags & MsgFlag::READ_ONLY_FLAG) != 0 ? "true" : "false")
               << " PRE_PROCESS_FLAG=" << ((flags & MsgFlag::PRE_PROCESS_FLAG) != 0 ? "true" : "false")
               << " HAS_PRE_PROCESSED_FLAG=" << ((flags & MsgFlag::HAS_PRE_PROCESSED_FLAG) != 0 ? "true" : "false")
               << " BLOCK_ACCUMULATION_ENABLED=" << isBlockAccumulationEnabled);
  BlockId currBlock = m_storage->getLastBlockId();

  // Look for conflicts
  bool hasConflict = false;
  for (size_t i = 0; !hasConflict && i < write_req.readset.size(); i++) {
    static_assert(
        sizeof(*(write_req.readset[i].data())) == sizeof(string::value_type),
        "Byte pointer type used by concord::kvbc::IReader, concord::kvbc::categorization::VersionedUpdates, and/or "
        "concord::kvbc::categorization::BlockMerkleUpdates is incompatible with byte pointer type used by CMF.");
    const string key =
        string(reinterpret_cast<const string::value_type *>(write_req.readset[i].data()), write_req.readset[i].size());
    const auto latest_ver = getLatestVersion(key);
    hasConflict = (latest_ver && latest_ver > write_req.read_version);
    if (isBlockAccumulationEnabled && !hasConflict) {
      if (hasConflictInBlockAccumulatedRequests(key, blockAccumulatedVerUpdates, blockAccumulatedMerkleUpdates)) {
        hasConflict = true;
      }
    }
  }

  if (!hasConflict) {
    if (isBlockAccumulationEnabled) {
      // If Block Accumulation is enabled then blocks are added after all requests are processed
      addKeys(write_req, sequenceNum, blockAccumulatedVerUpdates, blockAccumulatedMerkleUpdates);
    } else {
      // If Block Accumulation is not enabled then blocks are added after all requests are processed
      VersionedUpdates verUpdates;
      BlockMerkleUpdates merkleUpdates;
      addKeys(write_req, sequenceNum, verUpdates, merkleUpdates);
      addBlock(verUpdates, merkleUpdates, sequenceNum);
    }
  }

  SKVBCReply reply;
  reply.reply = SKVBCWriteReply();
  SKVBCWriteReply &write_rep = std::get<SKVBCWriteReply>(reply.reply);
  write_rep.success = (!hasConflict);
  if (!hasConflict)
    write_rep.latest_block = currBlock + 1;
  else
    write_rep.latest_block = currBlock;

  vector<uint8_t> serialized_reply;
  serialize(serialized_reply, reply);
  ConcordAssert(serialized_reply.size() <= maxReplySize);
  copy(serialized_reply.begin(), serialized_reply.end(), outReply);
  outReplySize = serialized_reply.size();
  ++m_writesCounter;

  if (!isBlockAccumulationEnabled)
    LOG_INFO(m_logger,
             "ConditionalWrite message handled; writesCounter=" << m_writesCounter
                                                                << " currBlock=" << write_rep.latest_block);
  return OperationResult::SUCCESS;
}

OperationResult InternalCommandsHandler::executeGetBlockDataCommand(const SKVBCGetBlockDataRequest &request,
                                                                    size_t maxReplySize,
                                                                    char *outReply,
                                                                    uint32_t &outReplySize) {
  LOG_INFO(m_logger, "Execute GET_BLOCK_DATA command: type=SKVBCGetBlockDataRequest, BlockId=" << request.block_id);

  auto block_id = request.block_id;
  const auto updates = getBlockUpdates(block_id);
  if (!updates) {
    LOG_WARN(m_logger, "GetBlockData: Failed to retrieve block ID " << block_id);
    return OperationResult::INTERNAL_ERROR;
  }

  // Each block contains a single metadata key holding the sequence number
  const int numMetadataKeys = 1;
  auto numOfElements = updates->size() - numMetadataKeys;
  LOG_INFO(m_logger, "NUM OF ELEMENTS IN BLOCK = " << numOfElements);

  SKVBCReply reply;
  reply.reply = SKVBCReadReply();
  SKVBCReadReply &read_rep = std::get<SKVBCReadReply>(reply.reply);
  read_rep.reads.resize(numOfElements);
  size_t i = 0;
  for (const auto &[key, value] : *updates) {
    if (key != concord::kvbc::IBlockMetadata::kBlockMetadataKeyStr) {
      read_rep.reads[i].first.assign(key.begin(), key.end());
      read_rep.reads[i].second.assign(value.begin(), value.end());
      ++i;
    }
  }

  vector<uint8_t> serialized_reply;
  serialize(serialized_reply, reply);
  if (maxReplySize < serialized_reply.size()) {
    LOG_ERROR(m_logger,
              "replySize is too big: replySize=" << serialized_reply.size() << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  copy(serialized_reply.begin(), serialized_reply.end(), outReply);
  outReplySize = serialized_reply.size();
  return OperationResult::SUCCESS;
}

OperationResult InternalCommandsHandler::executeReadCommand(const SKVBCReadRequest &request,
                                                            size_t maxReplySize,
                                                            char *outReply,
                                                            uint32_t &outReplySize) {
  LOG_INFO(m_logger,
           "Execute READ command: type=SKVBCReadRequest, numberOfKeysToRead=" << request.keys.size() << ", readVersion="
                                                                              << request.read_version);
  SKVBCReply reply;
  reply.reply = SKVBCReadReply();
  SKVBCReadReply &read_rep = std::get<SKVBCReadReply>(reply.reply);
  read_rep.reads.resize(request.keys.size());
  for (size_t i = 0; i < request.keys.size(); i++) {
    read_rep.reads[i].first = request.keys[i];
    string value = "";
    static_assert(
        sizeof(*(request.keys[i].data())) == sizeof(string::value_type),
        "Byte pointer type used by concord::kvbc::IReader is incompatible with byte pointer type used by CMF.");
    string key(reinterpret_cast<const string::value_type *>(request.keys[i].data()), request.keys[i].size());
    if (request.read_version > m_storage->getLastBlockId()) {
      value = getLatest(key);
    } else {
      value = getAtMost(key, request.read_version);
    }
    read_rep.reads[i].second.assign(value.begin(), value.end());
  }

  vector<uint8_t> serialized_reply;
  serialize(serialized_reply, reply);
  if (maxReplySize < serialized_reply.size()) {
    LOG_ERROR(m_logger,
              "replySize is too big: replySize=" << serialized_reply.size() << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  copy(serialized_reply.begin(), serialized_reply.end(), outReply);
  outReplySize = serialized_reply.size();
  ++m_readsCounter;
  LOG_INFO(m_logger, "READ message handled; readsCounter=" << m_readsCounter);
  return OperationResult::SUCCESS;
}

OperationResult InternalCommandsHandler::executeGetLastBlockCommand(size_t maxReplySize,
                                                                    char *outReply,
                                                                    uint32_t &outReplySize) {
  LOG_INFO(m_logger, "GET LAST BLOCK");
  SKVBCReply reply;
  reply.reply = SKVBCGetLastBlockReply();
  SKVBCGetLastBlockReply &glb_rep = std::get<SKVBCGetLastBlockReply>(reply.reply);
  glb_rep.latest_block = m_storage->getLastBlockId();

  vector<uint8_t> serialized_reply;
  serialize(serialized_reply, reply);
  if (serialized_reply.size() > maxReplySize) {
    LOG_ERROR(m_logger,
              "Reply size is too large: replySize=" << serialized_reply.size() << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  copy(serialized_reply.begin(), serialized_reply.end(), outReply);
  outReplySize = serialized_reply.size();
  ++m_getLastBlockCounter;
  LOG_INFO(m_logger,
           "GetLastBlock message handled; getLastBlockCounter=" << m_getLastBlockCounter
                                                                << ", latestBlock=" << glb_rep.latest_block);
  return OperationResult::SUCCESS;
}

OperationResult InternalCommandsHandler::executeReadOnlyCommand(uint32_t requestSize,
                                                                const char *request,
                                                                size_t maxReplySize,
                                                                char *outReply,
                                                                uint32_t &outReplySize,
                                                                uint32_t &specificReplicaInfoOutReplySize) {
  SKVBCRequest deserialized_request;
  try {
    static_assert(sizeof(*request) == sizeof(uint8_t),
                  "Byte pointer type used by bftEngine::IRequestsHandler::ExecutionRequest is incompatible with byte "
                  "pointer type used by CMF.");
    const uint8_t *request_buffer_as_uint8 = reinterpret_cast<const uint8_t *>(request);
    deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + requestSize, deserialized_request);
  } catch (const runtime_error &e) {
    LOG_ERROR(m_logger, "Failed to deserialize SKVBCRequest: " << e.what());
    strcpy(outReply, "Failed to deserialize SKVBCRequest");
    outReplySize = strlen(outReply);
    return OperationResult::INTERNAL_ERROR;
  }
  if (holds_alternative<SKVBCReadRequest>(deserialized_request.request)) {
    return executeReadCommand(
        std::get<SKVBCReadRequest>(deserialized_request.request), maxReplySize, outReply, outReplySize);
  } else if (holds_alternative<SKVBCGetLastBlockRequest>(deserialized_request.request)) {
    return executeGetLastBlockCommand(maxReplySize, outReply, outReplySize);
  } else if (holds_alternative<SKVBCGetBlockDataRequest>(deserialized_request.request)) {
    return executeGetBlockDataCommand(
        std::get<SKVBCGetBlockDataRequest>(deserialized_request.request), maxReplySize, outReply, outReplySize);
  } else {
    LOG_WARN(m_logger, "Received read-only request of unrecognized message type.");
    strcpy(outReply, "Invalid read request");
    outReplySize = strlen(outReply);
    return OperationResult::INVALID_REQUEST;
  }
}

void InternalCommandsHandler::setPerformanceManager(
    std::shared_ptr<concord::performance::PerformanceManager> perfManager) {
  perfManager_ = perfManager;
}
