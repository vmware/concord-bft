// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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
#include "SimpleClient.hpp"
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
bftEngine::OperationResult ConcordClient::clientRequestError_ = SUCCESS;

ConcordClient::ConcordClient(int client_id,
                             ConcordClientPoolConfig& struct_config,
                             const SimpleClientParams& client_params)
    : logger_(logging::getLogger("concord.client.client_pool.external_client")) {
  client_id_ = client_id;
  CreateClient(struct_config, client_params);
}

ConcordClient::~ConcordClient() noexcept = default;

bft::client::Reply ConcordClient::SendRequest(const bft::client::WriteConfig& config, bft::client::Msg&& request) {
  bft::client::Reply res;
  clientRequestError_ = SUCCESS;
  try {
    res = new_client_->send(config, std::move(request));
  } catch (const TimeoutException& e) {
    clientRequestError_ = TIMEOUT;
    LOG_ERROR(logger_,
              "reqSeqNum=" << config.request.sequence_number << " cid=" << config.request.correlation_id
                           << " has failed to invoke, timeout=" << config.request.timeout.count()
                           << " ms has been reached");
  }
  LOG_DEBUG(logger_,
            "Request has completed processing"
                << KVLOG(client_id_, config.request.sequence_number, config.request.correlation_id));
  if (externalReplyBuffer) {
    memcpy(externalReplyBuffer, res.matched_data.data(), res.matched_data.size());
  }
  return res;
}

bft::client::Reply ConcordClient::SendRequest(const bft::client::ReadConfig& config, bft::client::Msg&& request) {
  bft::client::Reply res;
  clientRequestError_ = SUCCESS;
  try {
    res = new_client_->send(config, std::move(request));
  } catch (const TimeoutException& e) {
    clientRequestError_ = TIMEOUT;
    LOG_ERROR(logger_,
              "reqSeqNum=" << config.request.sequence_number << " cid=" << config.request.correlation_id
                           << " has failed to invoke, timeout=" << config.request.timeout.count()
                           << " ms has been reached");
  }
  LOG_DEBUG(logger_,
            "Request has completed processing"
                << KVLOG(client_id_, config.request.sequence_number, config.request.correlation_id));
  if (externalReplyBuffer) {
    memcpy(externalReplyBuffer, res.matched_data.data(), res.matched_data.size());
  }

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

std::pair<int8_t, ConcordClient::PendingReplies> ConcordClient::SendPendingRequests() {
  const auto& batch_cid =
      std::to_string(client_id_) + "-" + std::to_string(seqGen_->generateUniqueSequenceNumberForRequest());
  OperationResult ret = OperationResult::SUCCESS;
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
    single_config.request.pre_execute = req.flags & bftEngine::PRE_PROCESS_REQ;
    request_queue.push_back(bft::client::WriteRequest{single_config, std::move(req.request)});
  }
  try {
    auto res = new_client_->sendBatch(request_queue, batch_cid);
    for (auto rep : res) {
      for (auto reply : pending_replies_) {
        if (reply.lengthOfReplyBuffer == max_reply_size_) continue;
        auto cid = seq_num_to_cid.find(rep.first)->second;
        if (reply.cid == cid) {
          reply.actualReplyLength = rep.second.matched_data.size();
          memcpy(reply.replyBuffer, rep.second.matched_data.data(), rep.second.matched_data.size());
          auto result = bftEngine::SendResult{rep.second};
          if (reply.cb) reply.cb(std::move(result));
          LOG_DEBUG(logger_, "Request has completed processing" << KVLOG(client_id_, batch_cid, reply.cid));
        }
      }
    }
  } catch (BatchTimeoutException& e) {
    LOG_ERROR(logger_, "Batch cid =" << batch_cid << " has failed to invoke, timeout has been reached");
    ret = OperationResult::TIMEOUT;
  }
  batching_buffer_reply_offset_ = 0UL;
  pending_requests_.clear();
  return {ret, std::move(pending_replies_)};
}

bft::communication::BaseCommConfig* ConcordClient::CreateCommConfig(int num_replicas,
                                                                    const ConcordClientPoolConfig& config) const {
  const auto& client_conf = config.participant_nodes.at(0).externalClients.at(client_id_);
  const auto commType =
      config.comm_to_use == "tls"
          ? TlsTcp
          : config.comm_to_use == "tcp" ? PlainTcp : config.comm_to_use == "udp" ? PlainUdp : SimpleAuthTcp;
  const auto listenPort = client_conf.client_port;
  const auto bufferLength = std::stoul(config.concord_bft_communication_buffer_length);
  const auto selfId = client_conf.principal_id;
  NodeMap m;
  if (commType == PlainTcp)
    return new PlainTcpConfig{"external_client",
                              listenPort,
                              static_cast<uint32_t>(bufferLength),
                              m,
                              static_cast<int32_t>(num_replicas - 1),
                              selfId};
  if (config.encrypted_config_enabled)
    return new TlsTcpConfig{"external_client",
                            listenPort,
                            static_cast<uint32_t>(bufferLength),
                            m,
                            static_cast<std::int32_t>(num_replicas - 1),
                            selfId,
                            config.tls_certificates_folder_path,
                            config.tls_cipher_suite_list,
                            nullptr,
                            config.secret_data};
  return new TlsTcpConfig{"external_client",
                          listenPort,
                          static_cast<uint32_t>(bufferLength),
                          m,
                          static_cast<std::int32_t>(num_replicas - 1),
                          selfId,
                          config.tls_certificates_folder_path,
                          config.tls_cipher_suite_list,
                          nullptr,
                          std::nullopt};
}

void ConcordClient::CreateClient(ConcordClientPoolConfig& config, const SimpleClientParams& client_params) {
  const auto num_replicas = config.num_replicas;
  const auto& nodes = std::move(config.participant_nodes);
  const auto& node = nodes.at(0);
  const auto& external_clients_conf = node.externalClients;
  const auto& client_conf = external_clients_conf.at(client_id_);
  auto fVal = config.f_val;
  auto cVal = config.c_val;
  auto clientId = client_conf.principal_id;
  enable_mock_comm_ = config.enable_mock_comm;
  BaseCommConfig* comm_config = CreateCommConfig(num_replicas, config);
  client_id_ = clientId;
  std::set<ReplicaId> all_replicas;
  const std::unordered_map<bft::communication::NodeNum, Replica> node_conf = config.node;
  for (auto i = 0u; i < num_replicas; ++i) {
    const auto& replica_conf = node_conf.at(i);
    const auto replica_id = replica_conf.principal_id;
    NodeInfo node_info;
    node_info.host = replica_conf.replica_host;
    node_info.port = replica_conf.replica_port;
    node_info.isReplica = true;
    (*comm_config).nodes[replica_id] = node_info;
    all_replicas.insert(ReplicaId{static_cast<uint16_t>(i)});
  }
  // Ensure exception safety by creating local pointers and only moving to
  // object members if construction and startup haven't thrown.
  LOG_DEBUG(logger_, "Client_id=" << client_id_ << " Creating communication module");
  if (enable_mock_comm_) {
    if (delayed_behaviour_) {
      std::unique_ptr<FakeCommunication> fakecomm(new FakeCommunication(delayedBehaviour));
      comm_ = std::move(fakecomm);
    } else {
      std::unique_ptr<FakeCommunication> fakecomm(new FakeCommunication(immideateBehaviour));
      comm_ = std::move(fakecomm);
    }
  } else {
    auto comm = ToCommunication(*comm_config);
    comm_ = std::move(comm);
  }
  static constexpr char transaction_signing_plain_file_name[] = "transaction_signing_priv.pem";
  static constexpr char transaction_signing_enc_file_name[] = "transaction_signing_priv.pem.enc";

  LOG_DEBUG(logger_, "Client_id=" << client_id_ << " starting communication and creating new bft client instance");
  ClientConfig cfg;
  cfg.all_replicas = all_replicas;
  cfg.c_val = cVal;
  cfg.f_val = fVal;
  cfg.id = ClientId{clientId};
  cfg.retry_timeout_config = RetryTimeoutConfig{std::chrono::milliseconds(client_params.clientInitialRetryTimeoutMilli),
                                                std::chrono::milliseconds(client_params.clientMinRetryTimeoutMilli),
                                                std::chrono::milliseconds(client_params.clientMaxRetryTimeoutMilli),
                                                client_params.numberOfStandardDeviationsToTolerate,
                                                client_params.samplesPerEvaluation,
                                                static_cast<int16_t>(client_params.samplesUntilReset)};

  bool transaction_signing_enabled = config.transaction_signing_enabled;
  if (transaction_signing_enabled) {
    std::string priv_key_path = config.signing_key_path;
    const char* transaction_signing_file_name = transaction_signing_plain_file_name;

    auto tls_config = dynamic_cast<TlsTcpConfig*>(comm_config);
    if (tls_config->secretData) {
      transaction_signing_file_name = transaction_signing_enc_file_name;
      cfg.secrets_manager_config = tls_config->secretData;
    }
    cfg.transaction_signing_private_key_file_path = priv_key_path + std::string("/") + transaction_signing_file_name;
  }
  auto new_client = std::unique_ptr<bft::client::Client>{new bft::client::Client(std::move(comm_), cfg)};
  new_client_ = std::move(new_client);
  seqGen_ = bftEngine::SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests();
  LOG_INFO(logger_, "client_id=" << client_id_ << " creation succeeded");
}

int ConcordClient::getClientId() const { return client_id_; }

uint64_t ConcordClient::generateClientSeqNum() { return seqGen_->generateUniqueSequenceNumberForRequest(); }

void ConcordClient::setStartRequestTime() { start_job_time_ = std::chrono::steady_clock::now(); }

std::chrono::steady_clock::time_point ConcordClient::getStartRequestTime() const { return start_job_time_; }

void ConcordClient::setStartWaitingTime() { waiting_job_time_ = std::chrono::steady_clock::now(); }

std::chrono::steady_clock::time_point ConcordClient::getWaitingTime() const { return waiting_job_time_; }

bool ConcordClient::isServing() const { return new_client_->isServing(num_of_replicas_, required_num_of_replicas_); }

void ConcordClient::setStatics(uint16_t required_num_of_replicas,
                               uint16_t num_of_replicas,
                               uint32_t max_reply_size,
                               size_t batch_size) {
  ConcordClient::max_reply_size_ = max_reply_size;
  ConcordClient::reply_->resize((batch_size) ? batch_size * max_reply_size : max_reply_size_);
  ConcordClient::required_num_of_replicas_ = required_num_of_replicas;
  ConcordClient::num_of_replicas_ = num_of_replicas;
}

void ConcordClient::setReplyBuffer(char* buf, uint32_t size) {
  externalReplyBuffer = buf;
  externalReplyBufferSize = size;
}

void ConcordClient::unsetReplyBuffer() {
  externalReplyBuffer = nullptr;
  externalReplyBufferSize = 0;
}

void ConcordClient::setDelayFlagForTest(bool delay) { ConcordClient::delayed_behaviour_ = delay; }

bftEngine::OperationResult ConcordClient::getClientRequestError() { return clientRequestError_; }

void ConcordClient::stopClientComm() { new_client_->stop(); }

std::unique_ptr<bft::communication::ICommunication> ConcordClient::ToCommunication(const BaseCommConfig& comm_config) {
  if (comm_config.commType == TlsTcp) {
    return std::unique_ptr<ICommunication>{CommFactory::create(comm_config)};
  } else if (comm_config.commType == PlainUdp) {
    const auto udp_config = PlainUdpConfig{comm_config.listenHost,
                                           comm_config.listenPort,
                                           comm_config.bufferLength,
                                           comm_config.nodes,
                                           comm_config.selfId,
                                           comm_config.statusCallback};
    return std::unique_ptr<ICommunication>{CommFactory::create(udp_config)};
  }
  throw std::invalid_argument{"Unknown communication module type=" + std::to_string(comm_config.commType)};
}

}  // namespace concord::external_client
