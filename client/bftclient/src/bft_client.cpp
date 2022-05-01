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
#include "secrets_manager_impl.h"
#include "secrets_manager_enc.h"
#include "secrets_manager_plain.h"
#include "communication/StateControl.hpp"

using namespace concord::diagnostics;
using namespace concord::secretsmanager;
using namespace bftEngine;
using namespace bftEngine::impl;

namespace bft::client {

Client::Client(SharedCommPtr comm, const ClientConfig& config, std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : communication_(std::move(comm)),
      config_(config),
      quorum_converter_(config_.all_replicas, config.ro_replicas, config.f_val, config_.c_val),
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
  setAggregator(aggregator);
  // secrets_manager_config can be set only if transaction_signing_private_key_file_path is set
  if (config.secrets_manager_config) ConcordAssert(config.transaction_signing_private_key_file_path != std::nullopt);
  if (config.transaction_signing_private_key_file_path) {
    // transaction signing is enabled
    auto file_path = config.transaction_signing_private_key_file_path.value();
    std::optional<std::string> key_plaintext;
    std::unique_ptr<ISecretsManagerImpl> secretsManager;

    if (config.secrets_manager_config) {
      secretsManager = std::make_unique<SecretsManagerEnc>(config.secrets_manager_config.value());
    } else {
      // private key file is in plain text, use secrets manager plain to read the file
      secretsManager = std::make_unique<SecretsManagerPlain>();
    }

    key_plaintext = secretsManager->decryptFile(file_path);
    if (!key_plaintext) throw InvalidPrivateKeyException(file_path, config.secrets_manager_config != std::nullopt);
    transaction_signer_ = std::make_unique<concord::util::crypto::RSASigner>(
        key_plaintext.value().c_str(), concord::util::crypto::KeyFormat::PemFormat);
  }
  communication_->setReceiver(config_.id.val, &receiver_);
  communication_->start();
  if (config_.replicas_master_key_folder_path.has_value()) {
    bft::communication::StateControl::instance().setGetPeerPubKeyMethod([&](uint32_t rid) {
      concord::secretsmanager::SecretsManagerPlain psm_;
      std::string key_path = config_.replicas_master_key_folder_path.value() + "/" + std::to_string(rid) + "/pub_key";
      return psm_.decryptFile(key_path).value_or("");
    });
  }
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
  header->result = 1;  // UNKNOWN
  header->reqSignatureLength = 0;
  header->extraDataLength = 0;

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
    size_t actualSigSize = 0;
    position += config.correlation_id.size();
    {
      std::string sig;
      std::string data(request.begin(), request.end());
      TimeRecorder scoped_timer(*histograms_->sign_duration);
      sig = transaction_signer_->sign(data);
      actualSigSize = sig.size();
      std::memcpy(position, sig.data(), sig.size());
      ConcordAssert(expected_sig_len == actualSigSize);
      header->reqSignatureLength = actualSigSize;
      histograms_->transaction_size->record(request.size());
    }

    metrics_.transactionSigning++;
    if ((metrics_.transactionSigning.Get().Get() % count_between_snapshots) == 0) {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      auto& name = histograms_->getComponentName();
      registrar.perf.snapshot(name);
      LOG_DEBUG(logger_,
                "Signing stats snapshot #" << snapshot_index_ << " for " << name << std::endl
                                           << registrar.perf.toString(registrar.perf.get(name)));
      snapshot_index_++;
    }
  } else {
    header->reqSignatureLength = 0;
  }

  return msg;
}

Msg Client::createClientBatchMsg(const std::deque<Msg>& client_requests,
                                 uint32_t batch_buf_size,
                                 const std::string& cid,
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
      communication_->send(primary_.value().val, std::move(msg), config_.id.val);
    } else {
      std::set<bft::communication::NodeNum> dests;
      for (const auto& d : match_config.quorum.destinations) {
        dests.emplace(d.val);
      }
      communication_->send(dests, std::move(msg), config_.id.val);
    }

    if (auto reply = wait()) {
      expected_commit_time_ms_.add(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
      reply_certificates_.clear();
      return reply.value();
    }
    metrics_.retransmissions++;
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
      communication_->send(primary_.value().val, std::move(msg), config_.id.val);
    } else {
      std::set<bft::communication::NodeNum> dests;
      for (const auto& d : match_config.quorum.destinations) {
        dests.emplace(d.val);
      }
      communication_->send(dests, std::move(msg), config_.id.val);
    }

    wait(replies);
    metrics_.retransmissions++;
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
    static const size_t CLEAR_MATCHER_REPLIES_THRESHOLD = 2 * config_.f_val + config_.c_val + 1;
    // reply_certificates_ should hold just one request when using this wait method
    auto req = reply_certificates_.begin();
    if (req->second.numDifferentReplies() > CLEAR_MATCHER_REPLIES_THRESHOLD) {
      req->second.clearReplies();
      metrics_.repliesCleared++;
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
        primary_ = request->second.getPrimary();
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
    for (const auto& r : std::get<All>(read_config.quorum).destinations) {
      if (config_.ro_replicas.find(r) != config_.ro_replicas.end()) {
        // We are about to send a read request to ro replica, so we must ignore the primary in the replies
        mc.include_primary_ = false;
      }
    }

  } else {
    mc.quorum = quorum_converter_.toMofN(std::get<MofN>(read_config.quorum));
  }
  return mc;
}

bool Client::isServing(int num_replicas, int num_replicas_required) const {
  ConcordAssert(num_replicas >= num_replicas_required);
  int connected = 0;
  for (int i = 0; i < num_replicas; i++) {
    if (communication_->getCurrentConnectionStatus(i) == communication::ConnectionStatus::Connected) connected++;
    if (connected == num_replicas_required) return true;
  }
  return false;
}
std::string Client::signMessage(std::vector<uint8_t>& data) {
  std::string signature = std::string();
  if (transaction_signer_) {
    auto expected_sig_len = transaction_signer_->signatureLength();
    signature.resize(expected_sig_len);
    signature = transaction_signer_->sign(std::string(data.begin(), data.end()));
  }
  return signature;
}
Reply Client::sendThreadSafe(const WriteConfig& config, Msg&& request) {
  std::lock_guard<std::mutex> lg(lock_);
  return send(config, std::move(request));
}
Reply Client::sendThreadSafe(const ReadConfig& config, Msg&& request) {
  std::lock_guard<std::mutex> lg(lock_);
  return send(config, std::move(request));
}
}  // namespace bft::client
