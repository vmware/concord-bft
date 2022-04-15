// Concord
//
// Copyright (c) 2021-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// Primary Thin Replica Client Library header file; you should include this file
// to use the Thin Replica Client Library.
//

#pragma once

#include <atomic>
#include <opentracing/span.h>
#include <condition_variable>
#include <thread>

#include "grpc_connection.hpp"
#include "thread_pool.hpp"
#include "assertUtils.hpp"
#include "Metrics.hpp"

#include "Logger.hpp"
#include "client/concordclient/client_health.hpp"
#include "client/concordclient/snapshot_update.hpp"
#include "client/thin-replica-client/grpc_connection.hpp"

namespace client::replica_state_snapshot_client {

// Configuration for Replica State Snapshot client.
struct ReplicaStateSnapshotClientConfig {
  // trs_conns is a vector of connection objects. Each representing a direct
  // connection from this TRC to a specific Thin Replica Server.
  std::vector<std::shared_ptr<client::concordclient::GrpcConnection>>& rss_conns;

  uint32_t concurrency_level;
  ReplicaStateSnapshotClientConfig(std::vector<std::shared_ptr<client::concordclient::GrpcConnection>>& rss_conns_,
                                   uint32_t concurrency_level_)
      : rss_conns(rss_conns_), concurrency_level(concurrency_level_) {}
};

// TODO: Add metrics
// TODO: Add more comments

struct SnapshotRequest {
  uint64_t snapshot_id;
  std::optional<std::string> last_received_key;
};

class ReplicaStateSnapshotClient {
 public:
  ReplicaStateSnapshotClient(std::unique_ptr<ReplicaStateSnapshotClientConfig> config)
      : logger_(logging::getLogger("concord.client.replica_stream_snapshot")),
        config_(std::move(config)),
        threadpool_(config_->concurrency_level),
        count_of_concurrent_request_{0},
        is_serving_{false},
        health_check_enabled_{false} {}
  void readSnapshotStream(const SnapshotRequest& request,
                          std::shared_ptr<concord::client::concordclient::SnapshotQueue> remote_queue);

  bool isServing() { return is_serving_; }
  void setHealthCheckEnabled(bool is_enabled);
  concord::client::concordclient::ClientHealth getClientHealth();
  void setClientHealth(concord::client::concordclient::ClientHealth health);

 private:
  // Thread function to start subscription_thread_ with snapshot.
  void receiveSnapshot(const SnapshotRequest& request,
                       std::shared_ptr<concord::client::concordclient::SnapshotQueue> remote_queue);

  void pushFinalStateToRemoteQueue(const concordclient::GrpcConnection::Result& result);

  void pushDatumToRemoteQueue(const vmware::concord::replicastatesnapshot::StreamSnapshotResponse& datum,
                              std::shared_ptr<concord::client::concordclient::SnapshotQueue> remote_queue,
                              std::string& last_key);

  void updateOpCounter();
  void updateOpErrorCounter();

  logging::Logger logger_;
  std::unique_ptr<ReplicaStateSnapshotClientConfig> config_;
  concord::util::ThreadPool threadpool_;
  std::atomic_uint32_t count_of_concurrent_request_;
  bool is_serving_;

  bool health_check_enabled_;
  // operations we perform for checking for health
  std::atomic_uint op_counter_ = 0;
  // number of operations with errors in current operation count period.
  std::atomic_uint op_error_counter_ = 0;
  std::atomic_bool unhealthy_ = false;
};
}  // namespace client::replica_state_snapshot_client
