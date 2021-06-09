// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
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
#include "secrets_manager_enc.h"
#include "secrets_manager_plain.h"

using namespace concord::diagnostics;
using namespace concord::secretsmanager;
using namespace bftEngine;
using namespace bftEngine::impl;

namespace bft::client {

Client::Client(std::unique_ptr<bft::communication::ICommunication> comm, const ClientConfig& config)
    : communication_(std::move(comm)),
      config_(config),
      quorum_converter_(config_.all_replicas, config_.f_val, config_.c_val),
      expected_commit_time_ms_(config_.retry_timeout_config.initial_retry_timeout.count(),
                               config_.retry_timeout_config.number_of_standard_deviations_to_tolerate,
                               config_.retry_timeout_config.max_retry_timeout.count(),
                               config_.retry_timeout_config.min_retry_timeout.count(),
                               config_.retry_timeout_config.samples_per_evaluation,
                               config_.retry_timeout_config.samples_until_reset,
                               config_.retry_timeout_config.max_increasing_factor,
                               config_.retry_timeout_config.max_decreasing_factor),
      metrics_(config.id),
      histograms_(std::unique_ptr<Recorders>(new Recorders(config.id))) {
  // secrets_manager_config can be set only if transaction_signing_private_key_file_path is set
  if (config.secrets_manager_config) ConcordAssert(config.transaction_signing_private_key_file_path != std::nullopt);
  if (config.transaction_signing_private_key_file_path) {
    // transaction signing is enabled
    auto file_path = config.transaction_signing_private_key_file_path.value();
    std::optional<std::string> key_plaintext;

    if (config.secrets_manager_config) {
      SecretsManagerEnc sme(config.secrets_manager_config.value());
      key_plaintext = sme.decryptFile(file_path);
    } else {
      // private key file is in plain text, use secrets manager plain to read the file
      SecretsManagerPlain smp;
      key_plaintext = smp.decryptFile(file_path);
    }
    if (!key_plaintext) throw InvalidPrivateKeyException(file_path, config.secrets_manager_config != std::nullopt);
    transaction_signer_ = RSASigner(key_plaintext.value().c_str(), KeyFormat::PemFormat);
  }
  communication_->setReceiver(config_.id.val, &receiver_);
  communication_->Start();
}

Msg Client::createClientMsg(const RequestConfig& config, Msg&& request, bool read_only, uint16_t client_id) {
  uint8_t flags = read_only ? READ_ONLY_REQ : EMPTY_FLAGS_REQ;
  size_t expected_sig_len = 0;
  bool write_req_with_pre_exec = !read_only && config.pre_execute;

  if (write_req_with_pre_exec) {  // read_only messages are never pre-executed
    flags |= PRE_PROCESS_REQ;
  }
  if (config.key_exchange) {
    flags |= KEY_EXCHANGE_REQ;
  }

  if (config.reconfiguration) {
    flags |= RECONFIG_FLAG;
  }
  auto header_size = sizeof(ClientRequestMsgHeader);
  auto msg_size = header_size + request.size() + config.correlation_id.size() + config.span_context.size();
  if (transaction_signer_) {
    expected_sig_len = transaction_signer_->signatureLength();
    msg_size += expected_sig_len;
  }
  Msg msg(msg_size);
  ClientRequestMsgHeader* header = reinterpret_cast<ClientRequestMsgHeader*>(msg.data());
  header->msgType = write_req_with_pre_exec ? PRE_PROCESS_REQUEST_MSG_TYPE : REQUEST_MSG_TYPE;
  header->spanContextSize = config.span_context.size();
  header->idOfClientProxy = client_id;
  header->flags = flags;
  header->reqSeqNum = config.sequence_number;
  header->requestLength = request.size();
  header->timeoutMilli = config.timeout.count();
  header->cidLength = config.correlation_id.size();

  auto* position = msg.data() + header_size;

  // Copy the span context
  std::memcpy(position, config.span_context.data(), config.span_context.size());
  position += config.span_context.size();

  // Copy the request data
  std::memcpy(position, request.data(), request.size());
  position += request.size();

  // Copy the correlation ID
  std::memcpy(position, config.correlation_id.data(), config.correlation_id.size());

  if (transaction_signer_) {
    // Sign the request data, add the signature at the end of the request
    TimeRecorder scoped_timer(*histograms_->sign_duration);
    size_t actualSigSize = 0;
    position += config.correlation_id.size();
    transaction_signer_->sign(reinterpret_cast<const char*>(request.data()),
                              request.size(),
                              reinterpret_cast<char*>(position),
                              expected_sig_len,
                              actualSigSize);
    ConcordAssert(expected_sig_len == actualSigSize);
    header->reqSignatureLength = actualSigSize;
  } else {
    header->reqSignatureLength = 0;
  }

  return msg;
}

Msg Client::createClientBatchMsg(const std::deque<Msg>& client_requests,
                                 uint32_t batch_buf_size,
                                 const string& cid,
                                 uint16_t client_id) {
  auto header_size = sizeof(ClientBatchRequestMsgHeader);
  auto msg_size = sizeof(ClientBatchRequestMsgHeader) + cid.size() + batch_buf_size;
  Msg msg(msg_size);
  ClientBatchRequestMsgHeader* header = reinterpret_cast<ClientBatchRequestMsgHeader*>(msg.data());
  header->msgType = BATCH_REQUEST_MSG_TYPE;
  header->cidSize = cid.size();
  header->clientId = client_id;
  header->numOfMessagesInBatch = client_requests.size();
  header->dataSize = batch_buf_size;

  auto* position = msg.data() + header_size;

  if (cid.size()) {
    memcpy(position, cid.c_str(), cid.size());
    position += cid.size();
  }

  for (auto const& client_msg : client_requests) {
    memcpy(position, client_msg.data(), client_msg.size());
    position += client_msg.size();
  }

  return msg;
}

Msg Client::initBatch(std::deque<WriteRequest>& write_requests,
                      const std::string& cid,
                      std::chrono::milliseconds& max_time_to_wait) {
  Msg client_msg;
  MatchConfig match_config;
  uint32_t max_reply_size = 0;
  for (auto& req : write_requests) {
    match_config = writeConfigToMatchConfig(req.config);
    reply_certificates_.insert(std::make_pair(req.config.request.sequence_number, Matcher(match_config)));
    pending_requests_.push_back(createClientMsg(req.config.request, std::move(req.request), false, config_.id.val));
    if (req.config.request.timeout > max_time_to_wait) max_time_to_wait = req.config.request.timeout;
    if (req.config.request.max_reply_size > max_reply_size) max_reply_size = req.config.request.max_reply_size;
  }
  uint32_t batch_buf_size = 0;
  for (auto const& msg : pending_requests_) batch_buf_size += msg.size();
  receiver_.activate(max_reply_size);
  return createClientBatchMsg(pending_requests_, batch_buf_size, cid, config_.id.val);
}

Reply Client::send(const WriteConfig& config, Msg&& request) {
  ConcordAssertEQ(reply_certificates_.size(), 0);
  auto match_config = writeConfigToMatchConfig(config);
  bool read_only = false;
  return send(match_config, config.request, std::move(request), read_only);
}

Reply Client::send(const ReadConfig& config, Msg&& request) {
  ConcordAssertEQ(reply_certificates_.size(), 0);
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
  reply_certificates_.insert(std::make_pair(request_config.sequence_number, Matcher(match_config)));
  receiver_.activate(request_config.max_reply_size);
  auto orig_msg = createClientMsg(request_config, std::move(request), read_only, config_.id.val);
  auto start = std::chrono::steady_clock::now();
  auto end = start + request_config.timeout;
  while (std::chrono::steady_clock::now() < end) {
    bft::client::Msg msg(orig_msg);  // create copy here due to the loop
    if (primary_ && !read_only) {
      communication_->send(primary_.value().val, std::move(msg));
    } else {
      std::set<bft::communication::NodeNum> dests;
      for (const auto& d : match_config.quorum.destinations) {
        dests.emplace(d.val);
      }
      communication_->send(dests, std::move(msg));
    }

    if (auto reply = wait()) {
      expected_commit_time_ms_.add(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
      reply_certificates_.clear();
      return reply.value();
    }
    metrics_.retransmissions.Get().Inc();
  }

  expected_commit_time_ms_.add(request_config.timeout.count());
  reply_certificates_.clear();
  throw TimeoutException(request_config.sequence_number, request_config.correlation_id);
}

SeqNumToReplyMap Client::sendBatch(std::deque<WriteRequest>& write_requests, const std::string& cid) {
  SeqNumToReplyMap replies;
  std::chrono::milliseconds max_time_to_wait = 0s;
  MatchConfig match_config = writeConfigToMatchConfig(write_requests.front().config);
  auto batch_msg = initBatch(write_requests, cid, max_time_to_wait);
  auto start = std::chrono::steady_clock::now();
  auto end = start + max_time_to_wait;
  while (std::chrono::steady_clock::now() < end && replies.size() != pending_requests_.size()) {
    bft::client::Msg msg(batch_msg);  // create copy here due to the loop
    if (primary_) {
      communication_->send(primary_.value().val, std::move(msg));
    } else {
      std::set<bft::communication::NodeNum> dests;
      for (const auto& d : match_config.quorum.destinations) {
        dests.emplace(d.val);
      }
      communication_->send(dests, std::move(msg));
    }

    wait(replies);
    metrics_.retransmissions.Get().Inc();
  }
  reply_certificates_.clear();
  if (replies.size() == pending_requests_.size()) {
    expected_commit_time_ms_.add(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
    pending_requests_.clear();
    return replies;
  }
  pending_requests_.clear();
  expected_commit_time_ms_.add(max_time_to_wait.count());
  throw BatchTimeoutException(cid);
}

std::optional<Reply> Client::wait() {
  SeqNumToReplyMap replies;
  wait(replies);
  if (replies.empty()) {
    static constexpr size_t CLEAR_MATCHER_REPLIES_THRESHOLD = 5;
    // reply_certificates_ should hold just one request when using this wait method
    auto req = reply_certificates_.begin();
    if (req->second.numDifferentReplies() > CLEAR_MATCHER_REPLIES_THRESHOLD) {
      req->second.clearReplies();
      metrics_.repliesCleared.Get().Inc();
    }
    primary_ = std::nullopt;
    return std::nullopt;
  }
  auto reply = std::move(replies.begin()->second);
  return reply;
}

void Client::wait(SeqNumToReplyMap& replies) {
  auto now = std::chrono::steady_clock::now();
  auto retry_timeout = std::chrono::milliseconds(expected_commit_time_ms_.upperLimit());
  auto end_wait = now + retry_timeout;
  // Keep trying to receive messages until we get quorum or a retry timeout.
  while ((now = std::chrono::steady_clock::now()) < end_wait && !reply_certificates_.empty()) {
    auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_wait - now);
    auto unmatched_requests = receiver_.wait(wait_time);
    for (auto&& reply : unmatched_requests) {
      auto request = reply_certificates_.find(reply.metadata.seq_num);
      if (request == reply_certificates_.end()) continue;
      if (pending_requests_.size() > 0 && replies.size() == pending_requests_.size()) return;
      if (auto match = request->second.onReply(std::move(reply))) {
        primary_ = match->primary;
        replies.insert(std::make_pair(request->first, match->reply));
        reply_certificates_.erase(request->first);
      }
    }
  }
  if (!reply_certificates_.empty()) primary_ = std::nullopt;
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

bool Client::isServing(int numOfReplicas, int requiredNumOfReplicas) const {
  int connected = 0;
  for (int i = 0; i < numOfReplicas; i++) {
    if (communication_->getCurrentConnectionStatus(i) == communication::ConnectionStatus::Connected) connected++;
    if (connected == requiredNumOfReplicas) return true;
  }
  return false;
}

}  // namespace bft::client
