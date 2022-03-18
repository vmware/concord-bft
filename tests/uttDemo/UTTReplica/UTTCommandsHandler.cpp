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

using namespace bftEngine;
using namespace utt::messages;

void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue& requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string& batchCid,
                                 concordUtils::SpanWrapper& parent_span) {
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