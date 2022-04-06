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

      if (const auto* publicTx = std::get_if<PublicTx>(reqVariant)) {
        reply.reply = handleRequest(*publicTx);
      } else if (const auto* uttTx = std::get_if<UttTx>(reqVariant)) {
        reply.reply = handleRequest(*uttTx);
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

TxReply UTTCommandsHandler::handleRequest(const PublicTx& publicTx) {
  auto cmd = publicTx.tx;
  LOG_INFO(logger_, "Executing TxRequest with command: " << cmd);
  auto tx = parsePublicTx(cmd);
  if (!tx) throw std::runtime_error("Failed to parse tx!");

  TxReply reply;
  std::string err;

  if (state_.canExecuteTx(*tx, err)) {
    // Add a real block to the kv blockchain
    {
      BlockId nextExpectedBlockId = storage_->getLastBlockId() + 1;

      kvbc_cat::VersionedUpdates verUpdates;
      verUpdates.addUpdate("tx" + std::to_string(nextExpectedBlockId), std::move(cmd));

      kvbc_cat::Updates updates;
      updates.add(VERSIONED_KV_CAT_ID, std::move(verUpdates));
      const auto newBlockId = blockAdder_->add(std::move(updates));
      ConcordAssert(newBlockId == nextExpectedBlockId);
    }

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

utt::messages::TxReply UTTCommandsHandler::handleRequest(const utt::messages::UttTx& req) {
  LOG_INFO(logger_, "Executing UttTx!");

  TxReply reply;
  reply.success = false;
  reply.err = "NYI";

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

      if (std::holds_alternative<TxUttTransfer>(*block->tx_)) {
        // To-Do: Need to compute and send signatures on the output coins in each utt tx
        UttTx uttTx;
        uttTx.tx = ss.str();
        reply.tx = std::move(uttTx);
      } else {
        PublicTx publicTx;
        publicTx.tx = ss.str();
        reply.tx = std::move(publicTx);
      }
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
  const auto fileName = "utt_replica_" + std::to_string(replicaId);
  std::ifstream ifs(fileName);
  if (!ifs.is_open()) throw std::runtime_error("Failed to open " + fileName);

  ifs >> config_;

  LOG_INFO(logger_, "Loaded config '" << fileName);

  // ToDo: Init public balances with some value in the config
  for (const auto& pid : config_.pids_) {
    state_.addAccount(Account{pid});
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

    LOG_WARN(logger_, "UTT AppState fetching missing block " << *missingBlockId << " tx: " << v);

    auto tx = parsePublicTx(v);
    if (!tx) throw std::runtime_error("Failed to parse public tx!");

    state_.appendBlock(Block{std::move(*tx)});

    missingBlockId = state_.executeBlocks();
  }
}