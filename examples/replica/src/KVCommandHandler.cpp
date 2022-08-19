// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <unistd.h>
#include <algorithm>
#include <variant>

#include "KVCommandHandler.hpp"
#include "assertUtils.hpp"
#include "kv_types.hpp"
#include "sha_hash.hpp"
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
using concord::osexample::kv::messages::KVGetBlockDataRequest;
using concord::osexample::kv::messages::KVGetLastBlockReply;
using concord::osexample::kv::messages::KVGetLastBlockRequest;
using concord::osexample::kv::messages::KVReadReply;
using concord::osexample::kv::messages::KVReadRequest;
using concord::osexample::kv::messages::KVReply;
using concord::osexample::kv::messages::KVRequest;
using concord::osexample::kv::messages::KVWriteReply;
using concord::osexample::kv::messages::KVWriteRequest;

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

void KVCommandHandler::add(std::string &&key,
                           std::string &&value,
                           VersionedUpdates &verUpdates,
                           BlockMerkleUpdates &merkleUpdates) const {
  // Add all key-values in the block merkle category as public ones.
  if (addAllKeysAsPublic_) {
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

void KVCommandHandler::execute(KVCommandHandler::ExecutionRequestsQueue &requests,
                               std::optional<bftEngine::Timestamp> timestamp,
                               const std::string &batchCid,
                               concordUtils::SpanWrapper &parentSpan) {
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
      LOG_ERROR(getLogger(), "Received size-0 request.");
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
    if (res != OperationResult::SUCCESS) LOG_WARN(getLogger(), "Command execution failed!");

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

void KVCommandHandler::preExecute(IRequestsHandler::ExecutionRequest &req,
                                  std::optional<bftEngine::Timestamp> timestamp,
                                  const std::string &batchCid,
                                  concordUtils::SpanWrapper &parentSpan) {
  if (req.flags & bftEngine::DB_CHECKPOINT_FLAG) return;

  OperationResult res;
  if (req.requestSize <= 0) {
    LOG_ERROR(getLogger(), "Received size-0 request.");
    req.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
    return;
  }
  const uint8_t *request_buffer_as_uint8 = reinterpret_cast<const uint8_t *>(req.request);
  bool readOnly = req.flags & MsgFlag::READ_ONLY_FLAG;
  if (readOnly) {
    LOG_ERROR(getLogger(),
              "Received READ command for pre execution: "
                  << "seqNum=" << req.executionSequenceNum << " batchCid" << batchCid);
    return;
  }
  res = verifyWriteCommand(req.requestSize, request_buffer_as_uint8, req.maxReplySize, req.outActualReplySize);
  if (res != OperationResult::SUCCESS) {
    LOG_INFO(GL, "Operation result is not success in verifying write command");
  } else {
    KVRequest deserialized_request;
    deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + req.requestSize, deserialized_request);
    const KVWriteRequest &write_req = std::get<KVWriteRequest>(deserialized_request.request);
    LOG_INFO(getLogger(),
             "Pre execute WRITE command:"
                 << " type=KVWriteRequest seqNum=" << req.executionSequenceNum
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

void KVCommandHandler::addMetadataKeyValue(VersionedUpdates &updates, uint64_t sequenceNum) const {
  updates.addUpdate(
      std::string{concord::kvbc::IBlockMetadata::kBlockMetadataKeyStr},
      concord::kvbc::categorization::VersionedUpdates::Value{blockMetadata_->serialize(sequenceNum), true});
}

std::optional<std::string> KVCommandHandler::get(const std::string &key, BlockId blockId) const {
  const auto v = storageReader_->get(keyToCategory(key), key, blockId);
  if (!v) {
    return std::nullopt;
  }
  return std::visit([](const auto &v) { return v.data; }, *v);
}

std::string KVCommandHandler::getAtMost(const std::string &key, BlockId current) const {
  if (storageReader_->getLastBlockId() == 0 || storageReader_->getGenesisBlockId() == 0 || current == 0) {
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

std::string KVCommandHandler::getLatest(const std::string &key) const {
  const auto v = storageReader_->getLatest(keyToCategory(key), key);
  if (!v) {
    return std::string();
  }
  return std::visit([](const auto &v) { return v.data; }, *v);
}

std::optional<BlockId> KVCommandHandler::getLatestVersion(const std::string &key) const {
  const auto v = storageReader_->getLatestVersion(keyToCategory(key), key);
  if (!v) {
    return std::nullopt;
  }
  // We never delete keys in TesterReplica at that stage.
  ConcordAssert(!v->deleted);
  return v->version;
}

std::optional<std::map<std::string, std::string>> KVCommandHandler::getBlockUpdates(
    concord::kvbc::BlockId blockId) const {
  const auto updates = storageReader_->getBlockUpdates(blockId);
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

void KVCommandHandler::writeAccumulatedBlock(ExecutionRequestsQueue &blockedRequests,
                                             VersionedUpdates &verUpdates,
                                             BlockMerkleUpdates &merkleUpdates,
                                             uint64_t sn) {
  // Only block accumulated requests will be processed here
  BlockId currBlock = storageReader_->getLastBlockId();

  for (auto &req : blockedRequests) {
    if (req.flags & bftEngine::MsgFlag::HAS_PRE_PROCESSED_FLAG) {
      KVReply reply;
      size_t existing_reply_size = req.outActualReplySize;
      static_assert(sizeof(*(req.outReply)) == sizeof(uint8_t),
                    "Byte pointer type used by bftEngine::IRequestsHandler::ExecutionRequest is incompatible with byte "
                    "pointer type used by CMF.");
      const uint8_t *reply_buffer_as_uint8 = reinterpret_cast<uint8_t *>(req.outReply);
      deserialize(reply_buffer_as_uint8, reply_buffer_as_uint8 + req.outActualReplySize, reply);
      KVWriteReply &write_rep = std::get<KVWriteReply>(reply.reply);
      write_rep.latest_block = currBlock + 1;
      vector<uint8_t> serialized_reply;
      serialize(serialized_reply, reply);

      // We expect modifying the value of latest_block in the KVWriteReply
      // will not alter the length of its serialization.
      ConcordAssert(existing_reply_size == serialized_reply.size());

      copy(serialized_reply.begin(), serialized_reply.end(), req.outReply);
      LOG_INFO(getLogger(),
               "KVWrite message handled; writesCounter=" << writesCounter_ << " currBlock=" << write_rep.latest_block);
    }
  }
  addBlock(verUpdates, merkleUpdates, sn);
}

OperationResult KVCommandHandler::verifyWriteCommand(uint32_t requestSize,
                                                     const uint8_t *request,
                                                     size_t maxReplySize,
                                                     uint32_t &outReplySize) const {
  KVRequest deserializedRequest;
  try {
    deserialize(request, request + requestSize, deserializedRequest);
  } catch (const runtime_error &e) {
    LOG_ERROR(getLogger(), "Failed to deserialize KVRequest: " << e.what());
    return OperationResult::INTERNAL_ERROR;
  }
  if (!holds_alternative<KVWriteRequest>(deserializedRequest.request)) {
    LOG_ERROR(getLogger(), "Received an KVRequest other than an KVWriteRequest but not marked as read-only.");
    return OperationResult::INVALID_REQUEST;
  }

  if (maxReplySize < outReplySize) {
    LOG_ERROR(getLogger(), "replySize is too big: replySize=" << outReplySize << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  return OperationResult::SUCCESS;
}

void KVCommandHandler::addKeys(const KVWriteRequest &writeReq,
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

void KVCommandHandler::addBlock(VersionedUpdates &verUpdates, BlockMerkleUpdates &merkleUpdates, uint64_t sn) {
  BlockId currBlock = storageReader_->getLastBlockId();
  Updates updates;

  // Add all key-values in the block merkle category as public ones.
  auto internalUpdates = VersionedUpdates{};
  if (addAllKeysAsPublic_) {
    ConcordAssertNE(kvbc_, nullptr);
    auto publicState = kvbc_->getPublicStateKeys();
    if (!publicState) {
      publicState = PublicStateKeys{};
    }
    for (const auto &[k, _] : merkleUpdates.getData().kv) {
      (void)_;
      // We don't allow duplicates.
      auto it = std::lower_bound(publicState->keys.cbegin(), publicState->keys.cend(), k);
      if (it != publicState->keys.cend() && *it == k) {
        continue;
      }
      // We always persist public state keys in sorted order.
      publicState->keys.push_back(k);
      std::sort(publicState->keys.begin(), publicState->keys.end());
    }
    const auto publicStateSer = detail::serialize(*publicState);
    internalUpdates.addUpdate(std::string{concord::kvbc::keyTypes::state_public_key_set},
                              std::string{publicStateSer.cbegin(), publicStateSer.cend()});
  }
  addMetadataKeyValue(internalUpdates, sn);
  updates.add(kConcordInternalCategoryId, std::move(internalUpdates));
  updates.add(VERSIONED_KV_CAT_ID, std::move(verUpdates));
  updates.add(BLOCK_MERKLE_CAT_ID, std::move(merkleUpdates));
  const auto newBlockId = blockAdder_->add(std::move(updates));
  ConcordAssert(newBlockId == currBlock + 1);
}

bool KVCommandHandler::hasConflictInBlockAccumulatedRequests(const std::string &key,
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

OperationResult KVCommandHandler::executeWriteCommand(uint32_t requestSize,
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
  KVRequest deserializedRequest;
  deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + requestSize, deserializedRequest);
  const KVWriteRequest &writeReq = std::get<KVWriteRequest>(deserializedRequest.request);
  LOG_INFO(getLogger(),
           "Execute WRITE command:"
               << " type=KVWriteRequest seqNum=" << sequenceNum << " numOfWrites=" << writeReq.writeset.size()
               << " numOfKeysInReadSet=" << writeReq.readset.size() << " readVersion=" << writeReq.read_version
               << " READ_ONLY_FLAG=" << ((flags & MsgFlag::READ_ONLY_FLAG) != 0 ? "true" : "false")
               << " PRE_PROCESS_FLAG=" << ((flags & MsgFlag::PRE_PROCESS_FLAG) != 0 ? "true" : "false")
               << " HAS_PRE_PROCESSED_FLAG=" << ((flags & MsgFlag::HAS_PRE_PROCESSED_FLAG) != 0 ? "true" : "false")
               << " BLOCK_ACCUMULATION_ENABLED=" << isBlockAccumulationEnabled);
  BlockId currBlock = storageReader_->getLastBlockId();

  // Look for conflicts
  bool hasConflict = false;
  for (size_t i = 0; !hasConflict && i < writeReq.readset.size(); i++) {
    static_assert(
        sizeof(*(writeReq.readset[i].data())) == sizeof(string::value_type),
        "Byte pointer type used by concord::kvbc::IReader, concord::kvbc::categorization::VersionedUpdates, and/or "
        "concord::kvbc::categorization::BlockMerkleUpdates is incompatible with byte pointer type used by CMF.");
    const string key =
        string(reinterpret_cast<const string::value_type *>(writeReq.readset[i].data()), writeReq.readset[i].size());
    const auto latestVer = getLatestVersion(key);
    hasConflict = (latestVer && latestVer > writeReq.read_version);
    if (isBlockAccumulationEnabled && !hasConflict) {
      if (hasConflictInBlockAccumulatedRequests(key, blockAccumulatedVerUpdates, blockAccumulatedMerkleUpdates)) {
        hasConflict = true;
      }
    }
  }

  if (!hasConflict) {
    if (isBlockAccumulationEnabled) {
      // If Block Accumulation is enabled then blocks are added after all requests are processed
      addKeys(writeReq, sequenceNum, blockAccumulatedVerUpdates, blockAccumulatedMerkleUpdates);
    } else {
      // If Block Accumulation is not enabled then blocks are added after all requests are processed
      VersionedUpdates verUpdates;
      BlockMerkleUpdates merkleUpdates;
      addKeys(writeReq, sequenceNum, verUpdates, merkleUpdates);
      addBlock(verUpdates, merkleUpdates, sequenceNum);
    }
  }

  KVReply reply;
  reply.reply = KVWriteReply();
  KVWriteReply &writeRep = std::get<KVWriteReply>(reply.reply);
  writeRep.success = (!hasConflict);
  if (!hasConflict)
    writeRep.latest_block = currBlock + 1;
  else
    writeRep.latest_block = currBlock;

  vector<uint8_t> serializedReply;
  serialize(serializedReply, reply);
  ConcordAssert(serializedReply.size() <= maxReplySize);
  copy(serializedReply.begin(), serializedReply.end(), outReply);
  outReplySize = serializedReply.size();
  ++writesCounter_;

  if (!isBlockAccumulationEnabled)
    LOG_INFO(
        getLogger(),
        "ConditionalWrite message handled; writesCounter=" << writesCounter_ << " currBlock=" << writeRep.latest_block);
  return OperationResult::SUCCESS;
}

OperationResult KVCommandHandler::executeGetBlockDataCommand(const KVGetBlockDataRequest &request,
                                                             size_t maxReplySize,
                                                             char *outReply,
                                                             uint32_t &outReplySize) {
  LOG_INFO(getLogger(), "Execute GET_BLOCK_DATA command: type=KVGetBlockDataRequest, BlockId=" << request.block_id);

  auto blockId = request.block_id;
  const auto updates = getBlockUpdates(blockId);
  if (!updates) {
    LOG_WARN(getLogger(), "GetBlockData: Failed to retrieve block ID " << blockId);
    return OperationResult::INTERNAL_ERROR;
  }

  // Each block contains a single metadata key holding the sequence number
  const int numMetadataKeys = 1;
  auto numOfElements = updates->size() - numMetadataKeys;
  LOG_INFO(getLogger(), "NUM OF ELEMENTS IN BLOCK = " << numOfElements);

  KVReply reply;
  reply.reply = KVReadReply();
  KVReadReply &readRep = std::get<KVReadReply>(reply.reply);
  readRep.reads.resize(numOfElements);
  size_t i = 0;
  for (const auto &[key, value] : *updates) {
    if (key != concord::kvbc::IBlockMetadata::kBlockMetadataKeyStr) {
      readRep.reads[i].first.assign(key.begin(), key.end());
      readRep.reads[i].second.assign(value.begin(), value.end());
      ++i;
    }
  }

  vector<uint8_t> serializedReply;
  serialize(serializedReply, reply);
  if (maxReplySize < serializedReply.size()) {
    LOG_ERROR(getLogger(),
              "replySize is too big: replySize=" << serializedReply.size() << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  copy(serializedReply.begin(), serializedReply.end(), outReply);
  outReplySize = serializedReply.size();
  return OperationResult::SUCCESS;
}

OperationResult KVCommandHandler::executeReadCommand(const KVReadRequest &request,
                                                     size_t maxReplySize,
                                                     char *outReply,
                                                     uint32_t &outReplySize) {
  LOG_INFO(getLogger(),
           "Execute READ command: type=KVReadRequest, numberOfKeysToRead=" << request.keys.size()
                                                                           << ", readVersion=" << request.read_version);
  KVReply reply;
  reply.reply = KVReadReply();
  KVReadReply &readRep = std::get<KVReadReply>(reply.reply);
  readRep.reads.resize(request.keys.size());
  for (size_t i = 0; i < request.keys.size(); i++) {
    readRep.reads[i].first = request.keys[i];
    string value = "";
    static_assert(
        sizeof(*(request.keys[i].data())) == sizeof(string::value_type),
        "Byte pointer type used by concord::kvbc::IReader is incompatible with byte pointer type used by CMF.");
    string key(reinterpret_cast<const string::value_type *>(request.keys[i].data()), request.keys[i].size());
    if (request.read_version > storageReader_->getLastBlockId()) {
      value = getLatest(key);
    } else {
      value = getAtMost(key, request.read_version);
    }
    readRep.reads[i].second.assign(value.begin(), value.end());
  }

  vector<uint8_t> serializedReply;
  serialize(serializedReply, reply);
  if (maxReplySize < serializedReply.size()) {
    LOG_ERROR(getLogger(),
              "replySize is too big: replySize=" << serializedReply.size() << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  copy(serializedReply.begin(), serializedReply.end(), outReply);
  outReplySize = serializedReply.size();
  ++readsCounter_;
  LOG_INFO(getLogger(), "READ message handled; readsCounter=" << readsCounter_);
  return OperationResult::SUCCESS;
}

OperationResult KVCommandHandler::executeGetLastBlockCommand(size_t maxReplySize,
                                                             char *outReply,
                                                             uint32_t &outReplySize) {
  LOG_INFO(getLogger(), "GET LAST BLOCK");
  KVReply reply;
  reply.reply = KVGetLastBlockReply();
  KVGetLastBlockReply &glbRep = std::get<KVGetLastBlockReply>(reply.reply);
  glbRep.latest_block = storageReader_->getLastBlockId();

  vector<uint8_t> serializedReply;
  serialize(serializedReply, reply);
  if (serializedReply.size() > maxReplySize) {
    LOG_ERROR(getLogger(),
              "Reply size is too large: replySize=" << serializedReply.size() << ", maxReplySize=" << maxReplySize);
    return OperationResult::EXEC_DATA_TOO_LARGE;
  }
  copy(serializedReply.begin(), serializedReply.end(), outReply);
  outReplySize = serializedReply.size();
  ++getLastBlockCounter_;
  LOG_INFO(getLogger(),
           "GetLastBlock message handled; getLastBlockCounter_=" << getLastBlockCounter_
                                                                 << ", latestBlock=" << glbRep.latest_block);
  return OperationResult::SUCCESS;
}

OperationResult KVCommandHandler::executeReadOnlyCommand(uint32_t requestSize,
                                                         const char *request,
                                                         size_t maxReplySize,
                                                         char *outReply,
                                                         uint32_t &outReplySize,
                                                         uint32_t &specificReplicaInfoOutReplySize) {
  KVRequest deserializedRequest;
  try {
    static_assert(sizeof(*request) == sizeof(uint8_t),
                  "Byte pointer type used by bftEngine::IRequestsHandler::ExecutionRequest is incompatible with byte "
                  "pointer type used by CMF.");
    const uint8_t *request_buffer_as_uint8 = reinterpret_cast<const uint8_t *>(request);
    deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + requestSize, deserializedRequest);
  } catch (const runtime_error &e) {
    LOG_ERROR(getLogger(), "Failed to deserialize KVRequest: " << e.what());
    strcpy(outReply, "Failed to deserialize KVRequest");
    outReplySize = strlen(outReply);
    return OperationResult::INTERNAL_ERROR;
  }
  if (holds_alternative<KVReadRequest>(deserializedRequest.request)) {
    return executeReadCommand(
        std::get<KVReadRequest>(deserializedRequest.request), maxReplySize, outReply, outReplySize);
  } else if (holds_alternative<KVGetLastBlockRequest>(deserializedRequest.request)) {
    return executeGetLastBlockCommand(maxReplySize, outReply, outReplySize);
  } else if (holds_alternative<KVGetBlockDataRequest>(deserializedRequest.request)) {
    return executeGetBlockDataCommand(
        std::get<KVGetBlockDataRequest>(deserializedRequest.request), maxReplySize, outReply, outReplySize);
  } else {
    LOG_WARN(getLogger(), "Received read-only request of unrecognized message type.");
    strcpy(outReply, "Invalid read request");
    outReplySize = strlen(outReply);
    return OperationResult::INVALID_REQUEST;
  }
}

void KVCommandHandler::setPerformanceManager(std::shared_ptr<concord::performance::PerformanceManager> perfManager) {
  perfManager_ = perfManager;
}
