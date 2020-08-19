// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#include "bftclient/bft_client.h"
#include "bftengine/ClientMsgs.hpp"
#include "assertUtils.hpp"

namespace bft::client {

// This function creates a ClientRequestMsg or a ClientPreProcessRequestMsg depending upon config.
//
// Since both of these are just instances of a `ClientRequestMsgHeader` followed by the message
// data, we construct them here, rather than relying on the type constructors embedded into the
// bftEngine impl. This allows us to not have to link with the bftengine library, and also allows us
// to return the messages as vectors with proper RAII based memory management.
Msg makeClientMsg(const RequestConfig& config, Msg&& request, bool read_only, uint16_t client_id) {
  uint8_t flags = read_only ? READ_ONLY_REQ : EMPTY_FLAGS_REQ;
  if (config.pre_execute) {
    flags |= PRE_PROCESS_REQ;
  }

  auto header_size = sizeof(bftEngine::ClientRequestMsgHeader);

  Msg msg(header_size + request.size() + config.correlation_id.size() + config.span_context.size());
  bftEngine::ClientRequestMsgHeader* header = reinterpret_cast<bftEngine::ClientRequestMsgHeader*>(msg.data());
  header->msgType = config.pre_execute ? PRE_PROCESS_REQUEST_MSG_TYPE : REQUEST_MSG_TYPE;
  header->spanContextSize = config.span_context.size();
  header->idOfClientProxy = client_id;
  header->flags = flags;
  header->reqSeqNum = config.sequence_number;
  header->requestLength = request.size();
  header->timeoutMilli = config.timeout.count();
  header->cid_length = config.correlation_id.size();

  auto* position = msg.data() + header_size;

  // Copy the span context
  std::memcpy(position, config.span_context.data(), config.span_context.size());
  position += config.span_context.size();

  // Copy the request data
  std::memcpy(position, request.data(), request.size());
  position += request.size();

  // Copy the correlation ID
  std::memcpy(position, config.correlation_id.data(), config.correlation_id.size());

  return msg;
}

Reply Client::send(const WriteConfig& config, Msg&& request) {
  ConcordAssert(!outstanding_request_.has_value());
  auto match_config = writeConfigToMatchConfig(config);
  bool read_only = false;
  return send(match_config, config.request, std::move(request), read_only);
}

Reply Client::send(const ReadConfig& config, Msg&& request) {
  ConcordAssert(!outstanding_request_.has_value());
  auto match_config = readConfigToMatchConfig(config);
  bool read_only = true;
  return send(match_config, config.request, std::move(request), read_only);
}

Reply Client::send(const MatchConfig& match_config,
                   const RequestConfig& request_config,
                   Msg&& request,
                   bool read_only) {
  metrics_.retransmissionTimer.Get().Set(expected_commit_time_ms_.upperLimit());
  metrics_.updateAggregator();
  outstanding_request_ = Matcher(match_config);
  receiver_.activate(request_config.max_reply_size);
  const auto msg = makeClientMsg(request_config, std::move(request), read_only, config_.id.val);
  auto start = std::chrono::steady_clock::now();
  auto end = start + request_config.timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (primary_ && !read_only) {
      communication_->sendAsyncMessage(primary_.value().val, (const char*)msg.data(), msg.size());
    } else {
      sendToGroup(match_config, msg);
    }

    if (auto reply = wait()) {
      expected_commit_time_ms_.add(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
      return reply.value();
    }
    metrics_.retransmissions.Get().Inc();
  }

  expected_commit_time_ms_.add(request_config.timeout.count());
  outstanding_request_ = std::nullopt;
  throw TimeoutException(request_config.sequence_number, request_config.correlation_id);
}

std::optional<Reply> Client::wait() {
  auto now = std::chrono::steady_clock::now();
  auto retry_timeout = std::chrono::milliseconds(expected_commit_time_ms_.upperLimit());
  auto end_wait = now + retry_timeout;

  // Keep trying to receive messages until we get quorum or a retry timeout.
  while ((now = std::chrono::steady_clock::now()) < end_wait) {
    auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_wait - now);
    auto unmatched_requests = receiver_.wait(wait_time);
    for (auto&& req : unmatched_requests) {
      if (auto match = outstanding_request_->onReply(std::move(req))) {
        primary_ = match->primary;
        outstanding_request_ = std::nullopt;
        return match->reply;
      }
    }
  }
  // If there are multiple distinct replies, we may want to clear any replies to free memory. This really only matters
  // for long running/indefinite requests.
  static constexpr size_t CLEAR_MATCHER_REPLIES_THRESHOLD = 5;
  if (outstanding_request_->numDifferentReplies() > CLEAR_MATCHER_REPLIES_THRESHOLD) {
    outstanding_request_->clearReplies();
    metrics_.repliesCleared.Get().Inc();
  }
  primary_ = std::nullopt;
  return std::nullopt;
}

void Client::sendToGroup(const MatchConfig& config, const Msg& msg) {
  for (auto dest : config.quorum.destinations) {
    communication_->sendAsyncMessage(dest.val, (const char*)msg.data(), msg.size());
  }
}

MatchConfig Client::writeConfigToMatchConfig(const WriteConfig& write_config) {
  MatchConfig mc;
  mc.sequence_number = write_config.request.sequence_number;

  if (std::holds_alternative<LinearizableQuorum>(write_config.quorum)) {
    mc.quorum = quorum_converter_.toMofN(std::get<LinearizableQuorum>(write_config.quorum));
  } else {
    mc.quorum = quorum_converter_.toMofN(std::get<ByzantineSafeQuorum>(write_config.quorum));
  }
  return mc;
}

MatchConfig Client::readConfigToMatchConfig(const ReadConfig& read_config) {
  MatchConfig mc;
  mc.sequence_number = read_config.request.sequence_number;

  if (std::holds_alternative<LinearizableQuorum>(read_config.quorum)) {
    mc.quorum = quorum_converter_.toMofN(std::get<LinearizableQuorum>(read_config.quorum));

  } else if (std::holds_alternative<ByzantineSafeQuorum>(read_config.quorum)) {
    mc.quorum = quorum_converter_.toMofN(std::get<ByzantineSafeQuorum>(read_config.quorum));

  } else if (std::holds_alternative<All>(read_config.quorum)) {
    mc.quorum = quorum_converter_.toMofN(std::get<All>(read_config.quorum));

  } else {
    mc.quorum = quorum_converter_.toMofN(std::get<MofN>(read_config.quorum));
  }
  return mc;
}

}  // namespace bft::client
