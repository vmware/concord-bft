// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>

#include "SimpleThreadPool.hpp"
#include "bftclient/base_types.h"
#include "bftclient/config.h"
#include "bftclient/quorums.h"
#include "client_pool_timer.hpp"
#include "external_client.hpp"

// the parameters are sequence number and cid
typedef std::function<void(const std::string /* cid */, uint32_t /*reply_size*/)> EXT_DONE_CALLBACK;

namespace concord {
namespace external_client {
class ConcordClient;
}

namespace concord_client_pool {

using TextMap = std::unordered_map<std::string, std::string>;

// Represents the answer that the DAML Ledger API could get when sending a
// request
enum SubmitResult {
  Acknowledged,  // The request has been queued for submission
  Overloaded,    // There is no available client to the moment to process the
  // request
};

// An internal error has occurred. Reason is recorded in logs.
class InternalError : public std::exception {
 public:
  InternalError() = default;
  const char* what() const noexcept override { return "Internal error occurred, please check the log files"; }
};

// Represents the answer that the DAML Ledger API could get when sending a
// request
enum PoolStatus {
  Serving,     // At least one client is running
  NotServing,  // All clients not running
};

struct externalRequest {
  std::vector<uint8_t> request;
  bftEngine::ClientMsgFlag flags;
  std::chrono::milliseconds timeout_ms;
  uint64_t seq_num;
  std::string correlation_id;
  std::string span_context;
  std::chrono::steady_clock::time_point arrival_time;
  char* reply_buffer = nullptr;
  std::uint32_t reply_size;
};

// Represents a Concord BFT client pool. The purpose of this class is to be
// easy to use for external users. This is achieved by:
//  * providing a simple public interface
//  * providing a generic public interface that allows for various use cases
//  * configuration via a file - users don't need to know what the structure
//  of the file is and changes to the file will not affect the interface of
//  the client
class ConcordClientPool {
  using ClientPtr = std::shared_ptr<concord::external_client::ConcordClient>;
  static constexpr uint16_t SECOND_LEG_CID_LEN = 16;

 public:
  // Return a unique pointer containing a ConcordClientPool. This is useful when
  // the ConcordClientPool is in a shared library where the main application may
  // be compiled with a different compiler. This assures layout and size is
  // determined by the shared library.
  static std::unique_ptr<ConcordClientPool> create(config_pool::ConcordClientPoolConfig&,
                                                   std::shared_ptr<concordMetrics::Aggregator>);
  // Construction executes all needed steps to provide a ready-to-use
  // object (including starting internal threads, if needed).
  explicit ConcordClientPool(config_pool::ConcordClientPoolConfig&, std::shared_ptr<concordMetrics::Aggregator>);

  explicit ConcordClientPool(config_pool::ConcordClientPoolConfig&,
                             std::shared_ptr<concordMetrics::Aggregator>,
                             bool delay_flag);

  ~ConcordClientPool();
  // This method is responsible to deal with requests in an asynchronous way,
  // for each request that comes, we will check if there is an available
  // client to deal with the problem if there is a client the request enters
  // into a thread pool and a positive answer is immediately returned to the
  // application. If there is no available client, a negative answer is
  // returned to the application. request - a vector that holds the request
  // from the client application. flags - holds the request flag (EMPTY_FLAG,
  // READ_ONLY, PRE_PROCESS). timeout_ms - the request timeout which specifies
  // for how long a client should wait for the request execution response.
  // reply_buffer - client application allocated buffer that stores returned
  // response.
  // max_reply_size - holds the size of reply_buffer.
  // seq_num - sequence number for the request
  SubmitResult SendRequest(std::vector<uint8_t>&& request,
                           bftEngine::ClientMsgFlag flags,
                           std::chrono::milliseconds timeout_ms,
                           char* reply_buffer,
                           std::uint32_t max_reply_size,
                           uint64_t seq_num,
                           std::string correlation_id = {},
                           std::string span_context = std::string(),
                           const bftEngine::RequestCallBack& callback = {});

  // This method is responsible to get write requests with the new client
  // paramters and parse it to the old SimpleClient interface.
  SubmitResult SendRequest(const bft::client::WriteConfig& config,
                           bft::client::Msg&& request,
                           const bftEngine::RequestCallBack& callback = {});

  // This method is responsible to get read requests with the new client
  // paramters and parse it to the old SimpleClient interface.
  SubmitResult SendRequest(const bft::client::ReadConfig& config,
                           bft::client::Msg&& request,
                           const bftEngine::RequestCallBack& callback = {});

  void InsertClientToQueue(std::shared_ptr<concord::external_client::ConcordClient>& client,
                           std::pair<int8_t, external_client::ConcordClient::PendingReplies>&& replies);

  // For batching jobs
  void assignJobToClient(const ClientPtr& client);

  // For single jobs
  void assignJobToClient(const ClientPtr& client,
                         std::vector<uint8_t>&& request,
                         bftEngine::ClientMsgFlag flags,
                         std::chrono::milliseconds timeout_ms,
                         char* reply_buffer,
                         std::uint32_t max_reply_size,
                         uint64_t seq_num,
                         const std::string& correlation_id,
                         const std::string& span_context,
                         const bftEngine::RequestCallBack& callback);

  PoolStatus HealthStatus();

  void Done(std::pair<int8_t, external_client::ConcordClient::PendingReplies>&& replies);
  void SetDoneCallback(EXT_DONE_CALLBACK cb);

  inline bool IsBatchingEnabled() { return client_batching_enabled_; }

 private:
  void setUpClientParams(bftEngine::SimpleClientParams& client_params,
                         const concord::config_pool::ConcordClientPoolConfig&);
  void CreatePool(concord::config_pool::ConcordClientPoolConfig&);

  void OnBatchingTimeout(const ClientPtr& client);
  bool clusterHasKeys(ClientPtr& cl);
  std::string SampleSpan(const std::string& span_blob);
  std::atomic_bool hasKeys_{false};
  std::atomic_bool stop_{false};
  size_t batch_size_ = 0UL;
  bool client_batching_enabled_{false};

  // Clients that are available for use (i.e. not already in use).
  std::deque<ClientPtr> clients_;
  // holds jobs that no clients was available to get.
  std::deque<externalRequest> external_requests_queue_;
  // Thread pool, on each thread on client will run
  ::util::SimpleThreadPool jobs_thread_pool_;
  // Clients queue mutex
  std::mutex clients_queue_lock_;
  // Metric
  concordMetrics::Component metricsComponent_;
  struct ClientPoolMetrics {
    concordMetrics::CounterHandle requests_counter;
    concordMetrics::CounterHandle executed_requests_counter;
    concordMetrics::CounterHandle rejected_counter;
    concordMetrics::CounterHandle full_batch_counter;
    concordMetrics::CounterHandle partial_batch_counter;
    concordMetrics::CounterHandle first_leg_counter;
    concordMetrics::CounterHandle second_leg_counter;
    concordMetrics::GaugeHandle size_of_batch_gauge;
    concordMetrics::GaugeHandle clients_gauge;
    concordMetrics::GaugeHandle last_request_time_gauge;
    concordMetrics::GaugeHandle average_req_dur_gauge;
    concordMetrics::GaugeHandle average_batch_agg_dur_gauge;
  } ClientPoolMetrics_;

  // Logger
  logging::Logger logger_;
  std::atomic_bool is_overloaded_ = false;
  EXT_DONE_CALLBACK done_callback_ = nullptr;
  uint32_t jobs_queue_max_size_ = 0;
  uint32_t span_rate = 0;
  uint32_t transaction_count = 0;
  std::mutex transaction_count_lock_;
  using Timer_t = ::concord_client_pool::Timer<ClientPtr>;
  std::unique_ptr<Timer_t> batch_timer_;
  bftEngine::impl::RollingAvgAndVar average_req_dur_;
  bftEngine::impl::RollingAvgAndVar batch_agg_dur_;
};

class BatchRequestProcessingJob : public ::util::SimpleThreadPool::Job {
 public:
  BatchRequestProcessingJob(concord_client_pool::ConcordClientPool& clients,
                            std::shared_ptr<external_client::ConcordClient> client)
      : clients_pool_{clients}, processing_client_{client} {}

  virtual ~BatchRequestProcessingJob() = default;

  void release() override { delete this; }

  void execute() override;

 protected:
  concord_client_pool::ConcordClientPool& clients_pool_;
  std::shared_ptr<external_client::ConcordClient> processing_client_;
};

class SingleRequestProcessingJob : public BatchRequestProcessingJob {
 public:
  SingleRequestProcessingJob(concord_client_pool::ConcordClientPool& clients,
                             std::shared_ptr<external_client::ConcordClient> client,
                             std::vector<uint8_t>&& request,
                             bftEngine::ClientMsgFlag flags,
                             std::chrono::milliseconds timeout_ms,
                             std::string correlation_id,
                             uint64_t seq_num,
                             std::string span_context,
                             const bftEngine::RequestCallBack& callback)
      : BatchRequestProcessingJob(clients, std::move(client)),
        request_(std::move(request)),
        flags_{flags},
        timeout_ms_{timeout_ms},
        correlation_id_{std::move(correlation_id)},
        span_context_{std::move(span_context)},
        seq_num_{seq_num},
        callback_{callback} {};

  void execute() override;

 private:
  std::vector<uint8_t> request_;
  bftEngine::ClientMsgFlag flags_;
  std::chrono::milliseconds timeout_ms_;
  const std::string correlation_id_;
  std::string span_context_;
  uint64_t seq_num_;
  bft::client::WriteConfig write_config_;
  bft::client::ReadConfig read_config_;
  const bftEngine::RequestCallBack callback_;
};
}  // namespace concord_client_pool

}  // namespace concord
