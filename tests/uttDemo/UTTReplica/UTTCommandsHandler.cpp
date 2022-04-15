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

#include <fstream>

#include "kvbc_key_types.hpp"
#include "ReplicaConfig.hpp"

#include <utt/Replica.h>

using namespace bftEngine;
using namespace utt::messages;

using concord::kvbc::BlockId;
namespace kvbc_cat = concord::kvbc::categorization;

void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue& requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string& batchCid,
                                 concordUtils::SpanWrapper& parent_span) {
  syncAppState();

  for (auto& req : requests) {
    if (req.outExecutionStatus != static_cast<uint32_t>(OperationResult::UNKNOWN))
      continue;  // Request already executed (internal)

    LOG_INFO(logger_, "UTTCommandsHandler execute");

    UTTRequest uttRequest;
    try {
      auto uttRequestBytes = (const uint8_t*)req.request;
      deserialize(uttRequestBytes, uttRequestBytes + req.requestSize, uttRequest);

      UTTReply reply;
      const auto* reqVariant = &uttRequest.request;

      // Replica specific info is used to provide signature shares of output coins
      vector<uint8_t> rsi;

      if (const auto* txRequest = std::get_if<TxRequest>(reqVariant)) {
        reply.reply = handleRequest(*txRequest);
      } else if (const auto* lastBlockRequest = std::get_if<GetLastBlockRequest>(reqVariant)) {
        reply.reply = handleRequest(*lastBlockRequest);
      } else if (const auto* blockDataReq = std::get_if<GetBlockDataRequest>(reqVariant)) {
        reply.reply = handleRequest(*blockDataReq, rsi);
      } else {
        throw std::runtime_error("Unhandled UTTRquest type!");
      }

      // Serialize reply and rsi (if not empty)
      vector<uint8_t> replyBuffer;
      serialize(replyBuffer, reply);

      size_t totalReplySize = replyBuffer.size() + rsi.size();

      if (req.maxReplySize < totalReplySize) {
        LOG_ERROR(logger_,
                  "replySize is too big: total=" << totalReplySize << " max=" << req.maxReplySize
                                                 << " reply=" << replyBuffer.size() << " rsi=" << rsi.size());
        req.outExecutionStatus = static_cast<uint32_t>(OperationResult::EXEC_DATA_TOO_LARGE);
      } else {
        copy(replyBuffer.begin(), replyBuffer.end(), req.outReply);
        copy(rsi.begin(), rsi.end(), req.outReply + replyBuffer.size());

        req.outActualReplySize = totalReplySize;
        req.outReplicaSpecificInfoSize = rsi.size();

        LOG_INFO(logger_,
                 "UTTRequest successfully executed. totalReplySize=" << totalReplySize << " max=" << req.maxReplySize);
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
  auto tx = parseTx(txRequest.tx);
  if (!tx) throw std::runtime_error("Failed to parse tx!");

  if (const auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
    LOG_INFO(logger_, "Handling UTT Tx: " << txUtt->utt_.getHashHex());
  } else {
    LOG_INFO(logger_, "Handling Public Tx: " << txRequest.tx);
  }

  TxReply reply;
  std::string err;

  if (state_.canExecuteTx(*tx, err, config_)) {
    // Add a real block to the kv blockchain
    {
      BlockId nextExpectedBlockId = storage_->getLastBlockId() + 1;

      kvbc_cat::VersionedUpdates verUpdates;

      auto copyTx = txRequest.tx;
      verUpdates.addUpdate("tx" + std::to_string(nextExpectedBlockId), std::move(copyTx));

      kvbc_cat::Updates updates;
      updates.add(VERSIONED_KV_CAT_ID, std::move(verUpdates));
      const auto newBlockId = blockAdder_->add(std::move(updates));
      ConcordAssert(newBlockId == nextExpectedBlockId);
    }

    // Execute the added block
    // Note that for utt txs the signatures are computed lazily
    // and cached when the block data is requested

    state_.appendBlock(Block{std::move(*tx)});
    state_.executeBlocks();
    reply.success = true;
  } else {
    LOG_WARN(logger_, "Failed to execute Tx request: " << err);
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

GetBlockDataReply UTTCommandsHandler::handleRequest(const GetBlockDataRequest& req, std::vector<uint8_t>& outRsi) {
  LOG_INFO(logger_, "Executing GetBlockDataRequest for block_id=" << req.block_id);

  GetBlockDataReply reply;

  const auto* block = state_.getBlockById(req.block_id);
  if (!block) {
    LOG_WARN(logger_, "Request block " << req.block_id << " does not exist!");
    reply.success = false;
    return reply;
  }

  reply.success = true;
  reply.block_id = block->id_;

  if (block->tx_) {
    std::stringstream ss;
    ss << *block->tx_;
    reply.tx = ss.str();

    if (const auto* txUtt = std::get_if<TxUtt>(&(*block->tx_))) {
      // Compute signature shares lazily when the block is requested
      // Precondition: monotonically increasing blocks
      auto& sigShareRsiForBlock = sigSharesRsiCache_[req.block_id];

      if (sigShareRsiForBlock.empty()) {
        auto sigShares = libutt::Replica::signShareOutputCoins(txUtt->utt_, config_.bskShare_);
        ConcordAssert(!sigShares.empty());

        LOG_INFO(logger_, "Computed utt sign shares for block " << req.block_id << " tx: " << txUtt->utt_.getHashHex());

        // Cache the rsi reply
        std::stringstream ssRsi;
        ssRsi << sigShares.size() << '\n';
        for (const auto& sigShare : sigShares) {
          ssRsi << sigShare;
        }
        sigShareRsiForBlock = StrToBytes(ssRsi.str());
      }

      outRsi = sigShareRsiForBlock;  // Copy
    }
  }

  return reply;
}

std::string UTTCommandsHandler::getLatest(const std::string& key) const {
  const auto v = storage_->getLatest(VERSIONED_KV_CAT_ID, key);
  if (!v) {
    return std::string();
  }
  return std::visit([](const auto& v) { return v.data; }, *v);
}

void UTTCommandsHandler::initAppState() {
  AppState::initUTTLibrary();

  const auto replicaId = ReplicaConfig::instance().getreplicaId();
  const auto fileName = "config/utt_replica_" + std::to_string(replicaId);
  std::ifstream ifs(fileName);
  if (!ifs.is_open()) throw std::runtime_error("Failed to open " + fileName);

  ifs >> config_;

  LOG_INFO(logger_, "Loaded config '" << fileName);

  for (const auto& pid : config_.pids_) {
    state_.addAccount(Account{pid, config_.initPublicBalance_});
  }
  if (state_.GetAccounts().empty()) throw std::runtime_error("No accounts loaded!");
}

void UTTCommandsHandler::syncAppState() {
  const auto lastBlockId = storage_->getLastBlockId();

  ConcordAssert(lastBlockId >= state_.getLastKnownBlockId());

  state_.setLastKnownBlockId(lastBlockId);

  auto missingBlockId = state_.executeBlocks();
  while (missingBlockId) {
    const auto key = "tx" + std::to_string(*missingBlockId);
    const auto v = getLatest(key);

    LOG_INFO(logger_, "Fetching tx for missing block " << *missingBlockId);

    auto tx = parseTx(v);
    if (!tx) throw std::runtime_error("Failed to parse tx!");

    if (const auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
      LOG_INFO(logger_, "Executing utt tx: " << txUtt->utt_.getHashHex());
    } else {
      LOG_INFO(logger_, "Executing public tx: " << v);
    }

    state_.appendBlock(Block{std::move(*tx)});

    missingBlockId = state_.executeBlocks();
  }
}