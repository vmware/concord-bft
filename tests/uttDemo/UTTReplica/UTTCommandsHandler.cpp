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

#include "kvbc_key_types.hpp"

using namespace bftEngine;
using namespace utt::messages;

using concord::kvbc::BlockId;
namespace kvbc_cat = concord::kvbc::categorization;

using Hasher = concord::util::SHA3_256;
using Hash = Hasher::Digest;

template <typename Span>
static Hash hash(const Span& span) {
  return Hasher{}.digest(span.data(), span.size());
}

static const std::string& keyHashToCategory(const Hash& keyHash) {
  // If the most significant bit of a key's hash is set, use the VersionedKeyValueCategory. Otherwise, use the
  // BlockMerkleCategory.
  if (keyHash[0] & 0x80) {
    return VERSIONED_KV_CAT_ID;
  }
  return BLOCK_MERKLE_CAT_ID;
}

static const std::string& keyToCategory(const std::string& key) { return keyHashToCategory(hash(key)); }

void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue& requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string& batchCid,
                                 concordUtils::SpanWrapper& parent_span) {
  // Print state before execution
  std::stringstream ss;
  for (const auto& block : state_.GetBlocks()) ss << block << " ";
  ss << '\n';
  LOG_INFO(logger_, "[BC lastBlock=" << storage_->getLastBlockId() << "] AppState: " << ss.str());

  LOG_INFO(logger_, "UTTCommandsHandler execute");
  for (auto& req : requests) {
    UTTRequest uttRequest;
    try {
      auto uttRequestBytes = (const uint8_t*)req.request;
      deserialize(uttRequestBytes, uttRequestBytes + req.requestSize, uttRequest);

      UTTReply reply;
      const auto* reqVariant = &uttRequest.request;

      if (const auto* txRequest = std::get_if<TxRequest>(reqVariant)) {
        reply.reply = handleRequest(*txRequest);
      } else if (const auto* lastBlockRequest = std::get_if<GetLastBlockRequest>(reqVariant)) {
        reply.reply = handleRequest(*lastBlockRequest);
      } else if (const auto* blockDataReq = std::get_if<GetBlockDataRequest>(reqVariant)) {
        reply.reply = handleRequest(*blockDataReq);
      } else {
        throw std::runtime_error("Unhandled UTTRquest type!");
      }

      // Serialize reply
      vector<uint8_t> replyBuffer;
      serialize(replyBuffer, reply);
      if (req.maxReplySize < replyBuffer.size()) {
        LOG_ERROR(logger_,
                  "replySize is too big: replySize=" << replyBuffer.size() << ", maxReplySize=" << req.maxReplySize);
        req.outExecutionStatus = static_cast<uint32_t>(OperationResult::EXEC_DATA_TOO_LARGE);
      } else {
        copy(replyBuffer.begin(), replyBuffer.end(), req.outReply);
        req.outActualReplySize = replyBuffer.size();

        LOG_INFO(logger_, "UTTRequest successfully executed");
        req.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);
      }
    } catch (const std::exception& e) {
      LOG_ERROR(logger_, "Failed to execute UTTRequest: " << e.what());
      strcpy(req.outReply, "Failed to execute UTTRequest");
      req.outActualReplySize = strlen(req.outReply);
      req.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
    }
  }
}

TxReply UTTCommandsHandler::handleRequest(const TxRequest& txRequest) {
  auto cmd = BytesToStr(txRequest.tx);
  LOG_INFO(logger_, "Executing TxRequest with command: " << cmd);
  auto tx = parseTx(cmd);
  if (!tx) throw std::runtime_error("Failed to parse tx!");

  TxReply reply;
  std::string err;

  if (state_.canExecuteTx(*tx, err)) {
    // Add a real block to storage
    kvbc_cat::VersionedUpdates verUpdates;
    kvbc_cat::BlockMerkleUpdates merkleUpdates;
    add("tx", std::move(cmd), verUpdates, merkleUpdates);
    addBlock(verUpdates, merkleUpdates);

    state_.appendBlock(Block{std::move(*tx)});
    state_.executeBlocks();
    reply.success = true;
  } else {
    LOG_WARN(logger_, "Failed to execute TxRequest: " << err);
    reply.err = std::move(err);
    reply.success = false;
  }

  reply.last_block_id = state_.getLastKnownBlockId();

  return reply;
}

GetLastBlockReply UTTCommandsHandler::handleRequest(const GetLastBlockRequest&) {
  LOG_INFO(logger_, "Executing GetLastBlockRequest");

  GetLastBlockReply reply;
  reply.last_block_id = state_.getLastKnownBlockId();

  return reply;
}

GetBlockDataReply UTTCommandsHandler::handleRequest(const GetBlockDataRequest& req) {
  LOG_INFO(logger_, "Executing GetBlockDataRequest for block_id=" << req.block_id);

  GetBlockDataReply reply;

  const auto* block = state_.getBlockById(req.block_id);
  if (block) {
    reply.block_id = block->id_;
    if (block->tx_) {
      std::stringstream ss;
      ss << *block->tx_;
      reply.tx = StrToBytes(ss.str());
    }
  }

  return reply;
}

void UTTCommandsHandler::add(std::string&& key,
                             std::string&& value,
                             kvbc_cat::VersionedUpdates& verUpdates,
                             kvbc_cat::BlockMerkleUpdates& merkleUpdates) const {
  // Add all key-values in the block merkle category as public ones.
  if (addAllKeysAsPublic_) {
    merkleUpdates.addUpdate(std::move(key), std::move(value));
  } else {
    const auto& cat = keyToCategory(key);
    if (cat == VERSIONED_KV_CAT_ID) {
      verUpdates.addUpdate(std::move(key), std::move(value));
      return;
    }
    merkleUpdates.addUpdate(std::move(key), std::move(value));
  }
}

void UTTCommandsHandler::addBlock(kvbc_cat::VersionedUpdates& verUpdates, kvbc_cat::BlockMerkleUpdates& merkleUpdates) {
  BlockId currBlock = storage_->getLastBlockId();
  kvbc_cat::Updates updates;

  // Add all key-values in the block merkle category as public ones.
  if (addAllKeysAsPublic_) {
    ConcordAssertNE(kvbc_, nullptr);
    auto public_state = kvbc_->getPublicStateKeys();
    if (!public_state) {
      public_state = kvbc_cat::PublicStateKeys{};
    }
    for (const auto& [k, _] : merkleUpdates.getData().kv) {
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
    const auto public_state_ser = kvbc_cat::detail::serialize(*public_state);
    auto public_state_updates = kvbc_cat::VersionedUpdates{};
    public_state_updates.addUpdate(std::string{concord::kvbc::keyTypes::state_public_key_set},
                                   std::string{public_state_ser.cbegin(), public_state_ser.cend()});
    updates.add(kvbc_cat::kConcordInternalCategoryId, std::move(public_state_updates));
  }

  updates.add(VERSIONED_KV_CAT_ID, std::move(verUpdates));
  updates.add(BLOCK_MERKLE_CAT_ID, std::move(merkleUpdates));
  const auto newBlockId = blockAdder_->add(std::move(updates));
  ConcordAssert(newBlockId == currBlock + 1);
}