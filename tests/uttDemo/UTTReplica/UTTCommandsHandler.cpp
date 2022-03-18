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

#include "utt_messages.cmf.hpp"

using namespace bftEngine;
using namespace utt::messages;

void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue& requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string& batchCid,
                                 concordUtils::SpanWrapper& parent_span) {
  for (auto& it : requests) {
    const auto* request = it.request;
    const auto requestSize = it.requestSize;
    const auto maxReplySize = it.maxReplySize;
    auto outReply = it.outReply;
    auto& outReplySize = it.outActualReplySize;

    LOG_INFO(logger_, "UTTCommandsHandler execute");

    UTTRequest deserialized_request;
    try {
      static_assert(sizeof(*request) == sizeof(uint8_t),
                    "Byte pointer type used by bftEngine::IRequestsHandler::ExecutionRequest is incompatible with byte "
                    "pointer type used by CMF.");
      const uint8_t* request_buffer_as_uint8 = reinterpret_cast<const uint8_t*>(request);
      deserialize(request_buffer_as_uint8, request_buffer_as_uint8 + requestSize, deserialized_request);

      if (std::holds_alternative<TxRequest>(deserialized_request.request)) {
        const auto& txRequest = std::get<TxRequest>(deserialized_request.request);
        auto cmd = BytesToStr(txRequest.tx);
        LOG_INFO(logger_, "Received TxRequest with command: " << cmd);
        if (auto tx = parseTx(cmd)) {
          state_.validateTx(*tx);

          UTTReply reply;
          reply.reply = TxReply();
          auto& txReply = std::get<TxReply>(reply.reply);

          txReply.success = true;
          txReply.last_block_id = state_.appendBlock(Block{std::move(*tx)});

          state_.sync();

          vector<uint8_t> serialized_reply;
          serialize(serialized_reply, reply);
          if (maxReplySize < serialized_reply.size()) {
            LOG_ERROR(
                logger_,
                "replySize is too big: replySize=" << serialized_reply.size() << ", maxReplySize=" << maxReplySize);
            it.outExecutionStatus = static_cast<uint32_t>(OperationResult::EXEC_DATA_TOO_LARGE);
          }
          copy(serialized_reply.begin(), serialized_reply.end(), outReply);
          outReplySize = serialized_reply.size();

          LOG_INFO(logger_, "TxRequest message handled");
          it.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);
        } else {
          throw std::runtime_error("Failed to parse TxRequest!");
        }

      } else if (std::holds_alternative<GetLastBlockRequest>(deserialized_request.request)) {
        LOG_INFO(logger_, "Received GetLastBlockRequest");

        UTTReply reply;
        reply.reply = GetLastBlockReply();
        auto& lastBlockReply = std::get<GetLastBlockReply>(reply.reply);
        lastBlockReply.last_block_id = state_.getLastKnowBlockId();

        vector<uint8_t> serialized_reply;
        serialize(serialized_reply, reply);
        if (maxReplySize < serialized_reply.size()) {
          LOG_ERROR(logger_,
                    "replySize is too big: replySize=" << serialized_reply.size() << ", maxReplySize=" << maxReplySize);
          it.outExecutionStatus = static_cast<uint32_t>(OperationResult::EXEC_DATA_TOO_LARGE);
        }
        copy(serialized_reply.begin(), serialized_reply.end(), outReply);
        outReplySize = serialized_reply.size();

        LOG_INFO(logger_, "GetLastBlockRequest message handled");
        it.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);

      } else if (std::holds_alternative<GetBlockDataRequest>(deserialized_request.request)) {
        const auto& blockDataReq = std::get<GetBlockDataRequest>(deserialized_request.request);
        LOG_INFO(logger_, "Received GetBlockDataRequest for block_id=" << blockDataReq.block_id);

        UTTReply reply;
        reply.reply = GetBlockDataReply();
        auto& blockDataReply = std::get<GetBlockDataReply>(reply.reply);

        const auto* block = state_.getBlockById(blockDataReq.block_id);
        if (block) {
          blockDataReply.block_id = block->id_;
          if (block->tx_) {
            std::stringstream ss;
            ss << *block->tx_;
            blockDataReply.tx = StrToBytes(ss.str());
          }
        }

        vector<uint8_t> serialized_reply;
        serialize(serialized_reply, reply);
        if (maxReplySize < serialized_reply.size()) {
          LOG_ERROR(logger_,
                    "replySize is too big: replySize=" << serialized_reply.size() << ", maxReplySize=" << maxReplySize);
          it.outExecutionStatus = static_cast<uint32_t>(OperationResult::EXEC_DATA_TOO_LARGE);
        }
        copy(serialized_reply.begin(), serialized_reply.end(), outReply);
        outReplySize = serialized_reply.size();

        LOG_INFO(logger_, "GetBlockDataRequest message handled");
        it.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);

      } else {
        throw std::runtime_error("Unhandled UTTRquest type!");
      }
    } catch (const std::domain_error& e) {
      LOG_ERROR(logger_, "Failed to execute transaction: " << e.what());
      strcpy(outReply, "Failed to execute transaction");
      outReplySize = strlen(outReply);
      it.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
    } catch (const std::runtime_error& e) {
      LOG_WARN(logger_, "Invalid UTTRequest: " << e.what());
      strcpy(outReply, "Invalid UTTRequest");
      outReplySize = strlen(outReply);
      it.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
    }
  }
}