// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "client/client_pool/external_client.hpp"
#include <string>
#include <utility>
#include "SharedTypes.hpp"
#include "bftclient/fake_comm.h"

namespace concord::external_client {

using bftEngine::ClientMsgFlag;
using namespace config_pool;
using namespace bftEngine;
using namespace bft::communication;

std::shared_ptr<std::vector<char>> ConcordClient::reply_ = std::make_shared<std::vector<char>>(0);
uint16_t ConcordClient::required_num_of_replicas_ = 0;
uint16_t ConcordClient::num_of_replicas_ = 0;
size_t ConcordClient::max_reply_size_ = 0;
bool ConcordClient::delayed_behaviour_ = false;
std::set<ReplicaId> ConcordClient::all_replicas_;
config_pool::ConcordClientPoolConfig ConcordClient::pool_config_;
bftEngine::SimpleClientParams ConcordClient::client_params_;
BaseCommConfig* ConcordClient::multiplexConfig_ = nullptr;
bft::client::SharedCommPtr ConcordClient::multiplex_comm_channel_;

void ConcordClient::setStatics(uint16_t required_num_of_replicas,
                               uint16_t num_of_replicas,
                               uint32_t max_reply_size,
                               size_t batch_size,
                               ConcordClientPoolConfig& pool_config,
                               SimpleClientParams& client_params,
                               BaseCommConfig* multiplexConfig) {
  ConcordClient::max_reply_size_ = max_reply_size;
  ConcordClient::reply_->resize((batch_size) ? batch_size * max_reply_size : max_reply_size_);
  ConcordClient::required_num_of_replicas_ = required_num_of_replicas;
  ConcordClient::num_of_replicas_ = num_of_replicas;
  pool_config_ = pool_config;
  client_params_ = client_params;
  multiplexConfig_ = multiplexConfig;
  for (const auto& replica : pool_config.replicas) {
    all_replicas_.insert(ReplicaId{static_cast<uint16_t>(replica.first)});
  }
}

ConcordClient::ConcordClient(int client_id, std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : logger_(logging::getLogger("concord.client.client_pool.external_client")),
      clientRequestExecutionResult_(OperationResult::SUCCESS) {
  client_id_ = client_id;
  CreateClient(aggregator);
}

ConcordClient::~ConcordClient() noexcept = default;

bft::client::Reply ConcordClient::SendRequest(const bft::client::WriteConfig& config, bft::client::Msg&& request) {
  LOG_INFO(
      logger_,
      "Request processing started" << KVLOG(client_id_, config.request.sequence_number, config.request.correlation_id));
  bft::client::Reply res;
  clientRequestExecutionResult_ = OperationResult::SUCCESS;
  try {
    cid_before_send_map_[config.request.correlation_id] = std::chrono::steady_clock::now();
    res = new_client_->send(config, std::move(request));
    cid_response_map_[config.request.correlation_id] = std::chrono::steady_clock::now();
  } catch (const BadQuorumConfigException& e) {
    clientRequestExecutionResult_ = OperationResult::INVALID_REQUEST;
    LOG_ERROR(logger_, "Invalid write config: " << e.what());
  } catch (const TimeoutException& e) {
    clientRequestExecutionResult_ = OperationResult::TIMEOUT;
    LOG_ERROR(logger_,
              "reqSeqNum=" << config.request.sequence_number << " cid=" << config.request.correlation_id
                           << " has failed to invoke, timeout=" << config.request.timeout.count()
                           << " ms has been reached");
  }
  if (res.result != static_cast<uint32_t>(OperationResult::SUCCESS))
    clientRequestExecutionResult_ = static_cast<OperationResult>(res.result);
  LOG_INFO(logger_,
           "Request processing completed" << KVLOG(client_id_,
                                                   config.request.sequence_number,
                                                   config.request.correlation_id,
                                                   static_cast<uint32_t>(clientRequestExecutionResult_)));
  return res;
}

bft::client::Reply ConcordClient::SendRequest(const bft::client::ReadConfig& config, bft::client::Msg&& request) {
  LOG_INFO(
      logger_,
      "Request processing started" << KVLOG(client_id_, config.request.sequence_number, config.request.correlation_id));
  bft::client::Reply res;
  clientRequestExecutionResult_ = OperationResult::SUCCESS;
  try {
    cid_before_send_map_[config.request.correlation_id] = std::chrono::steady_clock::now();
    res = new_client_->send(config, std::move(request));
    cid_response_map_[config.request.correlation_id] = std::chrono::steady_clock::now();
  } catch (const BadQuorumConfigException& e) {
    clientRequestExecutionResult_ = OperationResult::INVALID_REQUEST;
    LOG_ERROR(logger_, "Invalid read config: " << e.what());
  } catch (const TimeoutException& e) {
    clientRequestExecutionResult_ = OperationResult::TIMEOUT;
    LOG_ERROR(logger_,
              "reqSeqNum=" << config.request.sequence_number << " cid=" << config.request.correlation_id
                           << " has failed to invoke, timeout=" << config.request.timeout.count()
                           << " ms has been reached");
  }
  if (res.result != static_cast<uint32_t>(OperationResult::SUCCESS))
    clientRequestExecutionResult_ = static_cast<OperationResult>(res.result);
  LOG_INFO(logger_,
           "Request processing completed" << KVLOG(client_id_,
                                                   config.request.sequence_number,
                                                   config.request.correlation_id,
                                                   static_cast<uint32_t>(clientRequestExecutionResult_)));
  return res;
}

void ConcordClient::AddPendingRequest(std::vector<uint8_t>&& request,
                                      bftEngine::ClientMsgFlag flags,
                                      char* reply_buffer,
                                      std::chrono::milliseconds timeout_ms,
                                      std::uint32_t reply_size,
                                      uint64_t seq_num,
                                      const std::string& correlation_id,
                                      const std::string& span_context,
                                      RequestCallBack callback) {
  bftEngine::ClientRequest pending_request;
  pending_request.lengthOfRequest = request.size();
  pending_request.request = std::move(request);
  pending_request.flags = flags;
  pending_request.timeoutMilli = timeout_ms.count();
  pending_request.reqSeqNum = seq_num;
  pending_request.cid = correlation_id;
  pending_request.span_context = span_context;
  pending_requests_.push_back(std::move(pending_request));

  bftEngine::ClientReply pending_reply;
  if (reply_size) {
    pending_reply.replyBuffer = reply_buffer;
    pending_reply.lengthOfReplyBuffer = reply_size;
  } else {
    pending_reply.replyBuffer = reply_->data() + batching_buffer_reply_offset_ * max_reply_size_;
    LOG_DEBUG(logger_,
              "Given reply size is 0, setting internal buffer with offset="
                  << (batching_buffer_reply_offset_ * max_reply_size_));
    ++batching_buffer_reply_offset_;
    pending_reply.lengthOfReplyBuffer = max_reply_size_;
  }
  pending_reply.actualReplyLength = 0UL;
  pending_reply.cid = correlation_id;
  pending_reply.span_context = span_context;
  pending_reply.cb = std::move(callback);
  pending_replies_.push_back(std::move(pending_reply));
}

std::pair<int32_t, ConcordClient::PendingReplies> ConcordClient::SendPendingRequests() {
  const auto& batch_cid =
      std::to_string(client_id_) + "-" + std::to_string(seqGen_->generateUniqueSequenceNumberForRequest());
  OperationResult ret = OperationResult::SUCCESS;
  clientRequestExecutionResult_ = OperationResult::SUCCESS;
  std::deque<bft::client::WriteRequest> request_queue;
  std::map<uint64_t, std::string> seq_num_to_cid;
  for (auto req : pending_requests_) {
    bft::client::WriteConfig single_config;
    single_config.request.timeout = std::chrono::milliseconds(req.timeoutMilli);
    single_config.request.span_context = req.span_context;
    single_config.request.sequence_number = req.reqSeqNum;
    single_config.request.correlation_id = req.cid;
    seq_num_to_cid.insert(std::make_pair(req.reqSeqNum, req.cid));
    single_config.request.span_context = req.span_context;
    single_config.request.reconfiguration = req.flags & bftEngine::RECONFIG_FLAG_REQ;
    single_config.request.pre_execute = req.flags & bftEngine::PRE_PROCESS_REQ;
    request_queue.push_back(bft::client::WriteRequest{single_config, std::move(req.request)});
    cid_before_send_map_[req.cid] = std::chrono::steady_clock::now();
  }
  try {
    LOG_INFO(logger_, "Batch processing started" << KVLOG(client_id_, batch_cid));
    auto received_replies_map = new_client_->sendBatch(request_queue, batch_cid);
    for (const auto& received_reply_entry : received_replies_map) {
      const auto received_reply_seq_num = received_reply_entry.first;
      const auto& pending_seq_num_to_cid_entry = seq_num_to_cid.find(received_reply_seq_num);
      if (pending_seq_num_to_cid_entry == seq_num_to_cid.end()) {
        LOG_ERROR(logger_,
                  "Received reply for a non pending for reply request"
                      << KVLOG(client_id_, batch_cid, received_reply_seq_num));
        continue;
      }
      auto cid = pending_seq_num_to_cid_entry->second;
      cid_response_map_[cid] = std::chrono::steady_clock::now();
      auto data_size = received_reply_entry.second.matched_data.size();
      for (auto& pending_reply : pending_replies_) {
        if (pending_reply.cid != cid) continue;
        auto response = received_reply_entry.second.result ? bftEngine::SendResult{received_reply_entry.second.result}
                                                           : bftEngine::SendResult{received_reply_entry.second};
        if (pending_reply.cb) {
          pending_reply.cb(move(response));
          LOG_INFO(logger_,
                   "Request processing completed; return response through the callback"
                       << KVLOG(client_id_,
                                batch_cid,
                                received_reply_seq_num,
                                pending_reply.cid,
                                received_reply_entry.second.result));
        } else {
          // Used for testing only
          if (data_size > pending_reply.lengthOfReplyBuffer) {
            LOG_WARN(logger_,
                     "Reply is too big" << KVLOG(client_id_, cid, pending_reply.lengthOfReplyBuffer, data_size));
            continue;
          }
          memcpy(pending_reply.replyBuffer, received_reply_entry.second.matched_data.data(), data_size);
          pending_reply.actualReplyLength = data_size;
          pending_reply.opResult = static_cast<bftEngine::OperationResult>(received_reply_entry.second.result);
          LOG_INFO(logger_,
                   "Request processing completed" << KVLOG(client_id_,
                                                           batch_cid,
                                                           received_reply_seq_num,
                                                           pending_reply.cid,
                                                           received_reply_entry.second.result));
        }
      }
    }
  } catch (BatchTimeoutException& e) {
    clientRequestExecutionResult_ = OperationResult::TIMEOUT;
    LOG_ERROR(logger_, "Batch cid =" << batch_cid << " has failed to invoke, timeout has been reached");
    ret = OperationResult::TIMEOUT;
  }
  LOG_INFO(logger_, "Batch processing completed" << KVLOG(client_id_, batch_cid));
  batching_buffer_reply_offset_ = 0UL;
  pending_requests_.clear();
  return {static_cast<uint32_t>(ret), std::move(pending_replies_)};
}

BaseCommConfig* ConcordClient::CreateCommConfig() const {
  const auto& client_conf = pool_config_.participant_nodes.at(0).externalClients.at(client_id_);
  const auto commType =
      pool_config_.comm_to_use == "tls"
          ? TlsTcp
          : pool_config_.comm_to_use == "tcp" ? PlainTcp : pool_config_.comm_to_use == "udp" ? PlainUdp : SimpleAuthTcp;
  const auto listenPort = client_conf.client_port;
  const auto bufferLength = std::stoul(pool_config_.concord_bft_communication_buffer_length);
  const auto selfId = client_conf.principal_id;
  if (commType == PlainTcp)
    return new PlainTcpConfig{"external_client",
                              listenPort,
                              static_cast<uint32_t>(bufferLength),
                              pool_config_.replicas,
                              static_cast<int32_t>(pool_config_.num_replicas - 1),
                              selfId};
  auto const secretData = pool_config_.encrypted_config_enabled
                              ? std::optional<concord::secretsmanager::SecretData>(pool_config_.secret_data)
                              : std::nullopt;
  return new TlsTcpConfig{"external_client",
                          listenPort,
                          static_cast<uint32_t>(bufferLength),
                          pool_config_.replicas,
                          static_cast<std::int32_t>(pool_config_.num_replicas - 1),
                          selfId,
                          pool_config_.tls_certificates_folder_path,
                          pool_config_.tls_cipher_suite_list,
                          pool_config_.use_unified_certificates,
                          nullptr,
                          secretData};
}

SharedCommPtr ConcordClient::ToCommunication(const BaseCommConfig& comm_config) {
  if (comm_config.commType_ == TlsTcp) {
    return SharedCommPtr{CommFactory::create(comm_config)};
  } else if (comm_config.commType_ == PlainUdp) {
    const auto udp_config = PlainUdpConfig{comm_config.listenHost_,
                                           comm_config.listenPort_,
                                           comm_config.bufferLength_,
                                           comm_config.nodes_,
                                           comm_config.selfId_,
                                           comm_config.statusCallback_};
    return SharedCommPtr{CommFactory::create(udp_config)};
  }
  throw std::invalid_argument{"Unknown communication module type=" + std::to_string(comm_config.commType_)};
}

std::tuple<BaseCommConfig*, SharedCommPtr> ConcordClient::CreateCommConfigAndCommChannel() {
  const auto& nodes = std::move(pool_config_.participant_nodes);
  const auto& client_conf = nodes.at(0).externalClients.at(client_id_);
  BaseCommConfig* comm_config = nullptr;
  SharedCommPtr comm_layer;
  if (multiplexConfig_)
    comm_config = multiplexConfig_;
  else {
    comm_config = CreateCommConfig();
    for (const auto& replica : pool_config_.replicas) {
      comm_config->nodes_[replica.first] = replica.second;
    }
    enable_mock_comm_ = pool_config_.enable_mock_comm;
    if (enable_mock_comm_) {
      const auto behaviour = delayed_behaviour_ ? delayedBehaviour : immediateBehaviour;
      comm_layer = std::make_shared<FakeCommunication>(behaviour);
    }
  }
  client_id_ = client_conf.principal_id;
  LOG_DEBUG(logger_, "Creating communication module" << KVLOG(client_id_));
  if (!enable_mock_comm_) {
    if (multiplexConfig_) {
      if (!multiplex_comm_channel_) {
        multiplex_comm_channel_ = SharedCommPtr{CommFactory::create(*multiplexConfig_)};
      }
      comm_layer = multiplex_comm_channel_;
    } else
      comm_layer = ToCommunication(*comm_config);
  }
  return {comm_config, comm_layer};
}

void ConcordClient::CreateClientConfig(BaseCommConfig* comm_config, ClientConfig& client_config) {
  static constexpr char transaction_signing_plain_file_name[] = "transaction_signing_priv.pem";
  static constexpr char transaction_signing_enc_file_name[] = "transaction_signing_priv.pem.enc";
  client_config.all_replicas = all_replicas_;
  client_config.c_val = pool_config_.c_val;
  client_config.f_val = pool_config_.f_val;
  client_config.id = ClientId{static_cast<uint16_t>(client_id_)};
  client_config.retry_timeout_config =
      RetryTimeoutConfig{std::chrono::milliseconds(client_params_.clientInitialRetryTimeoutMilli),
                         std::chrono::milliseconds(client_params_.clientMinRetryTimeoutMilli),
                         std::chrono::milliseconds(client_params_.clientMaxRetryTimeoutMilli),
                         client_params_.numberOfStandardDeviationsToTolerate,
                         client_params_.samplesPerEvaluation,
                         static_cast<int16_t>(client_params_.samplesUntilReset)};
  if (!pool_config_.path_to_replicas_master_key.empty())
    client_config.replicas_master_key_folder_path = pool_config_.path_to_replicas_master_key;
  bool transaction_signing_enabled = pool_config_.transaction_signing_enabled;
  auto tls_config = dynamic_cast<TlsTcpConfig*>(comm_config);
  if (transaction_signing_enabled) {
    std::string priv_key_path = pool_config_.signing_key_path;
    const char* transaction_signing_file_name = transaction_signing_plain_file_name;
    if (tls_config->secretData_) {
      transaction_signing_file_name = transaction_signing_enc_file_name;
      client_config.secrets_manager_config = tls_config->secretData_;
    }
    client_config.transaction_signing_private_key_file_path =
        priv_key_path + std::string("/") + transaction_signing_file_name;
  }
}

void ConcordClient::CreateClient(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  ClientConfig client_config;
  auto [comm_config, comm_layer] = CreateCommConfigAndCommChannel();
  CreateClientConfig(comm_config, client_config);
  LOG_DEBUG(logger_, "Creating new bft-client instance" << KVLOG(client_id_));
  auto new_client =
      std::unique_ptr<bft::client::Client>{new bft::client::Client(comm_layer, client_config, aggregator)};
  new_client_ = std::move(new_client);
  seqGen_ = bftEngine::SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests();
  LOG_INFO(logger_, "Client creation succeeded" << KVLOG(client_id_));
}

int ConcordClient::getClientId() const { return client_id_; }

uint64_t ConcordClient::generateClientSeqNum() { return seqGen_->generateUniqueSequenceNumberForRequest(); }

void ConcordClient::setStartRequestTime() { start_job_time_ = std::chrono::steady_clock::now(); }

std::chrono::steady_clock::time_point ConcordClient::getStartRequestTime() const { return start_job_time_; }

std::chrono::steady_clock::time_point ConcordClient::getAndDeleteCidBeforeSendTime(const std::string& cid) {
  auto time = cid_before_send_map_[cid];
  cid_before_send_map_.erase(cid);
  return time;
}

std::chrono::steady_clock::time_point ConcordClient::getAndDeleteCidResponseTime(const std::string& cid) {
  auto time = cid_response_map_[cid];
  cid_response_map_.erase(cid);
  return time;
}

void ConcordClient::setStartWaitingTime() { waiting_job_time_ = std::chrono::steady_clock::now(); }

std::chrono::steady_clock::time_point ConcordClient::getWaitingTime() const { return waiting_job_time_; }

bool ConcordClient::isServing() const { return new_client_->isServing(num_of_replicas_, required_num_of_replicas_); }

void ConcordClient::setDelayFlagForTest(bool delay) { ConcordClient::delayed_behaviour_ = delay; }

OperationResult ConcordClient::getRequestExecutionResult() { return clientRequestExecutionResult_; }

std::string ConcordClient::messageSignature(bft::client::Msg& message) { return new_client_->signMessage(message); }

void ConcordClient::stopClientComm() { new_client_->stop(); }

}  // namespace concord::external_client
