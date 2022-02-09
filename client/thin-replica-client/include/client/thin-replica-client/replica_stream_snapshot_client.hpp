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

#include "grpc_connection.hpp"
#include "thread_pool.hpp"
#include "assertUtils.hpp"
#include "Metrics.hpp"

#include <opentracing/span.h>
#include <condition_variable>
#include <thread>
#include "Logger.hpp"
#include "client/concordclient/remote_update_queue.hpp"
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
  std::string last_received_key;
};

class ReplicaStreamSnapshotClient {
 public:
  ReplicaStreamSnapshotClient(std::unique_ptr<ReplicaStateSnapshotClientConfig> config)
      : logger_(logging::getLogger("concord.client.replica_stream_snapshot")),
        config_(std::move(config)),
        threadpool_(config_->concurrency_level) {}
  void readSnapshotStream(const SnapshotRequest& request,
                          std::shared_ptr<concord::client::concordclient::StreamUpdateQueue> remote_queue);

 private:
  // Thread function to start subscription_thread_ with with snapshot.
  void receiveSnapshot(const SnapshotRequest& request,
                       std::shared_ptr<concord::client::concordclient::StreamUpdateQueue> remote_queue);

  void pushFinalStateToRemoteQueue(const concordclient::GrpcConnection::Result& result);

  void pushDatumToRemoteQueue(const vmware::concord::replicastatesnapshot::StreamSnapshotResponse& datum,
                              std::shared_ptr<concord::client::concordclient::StreamUpdateQueue> remote_queue,
                              std::string& last_key);

  logging::Logger logger_;
  std::unique_ptr<ReplicaStateSnapshotClientConfig> config_;
  concord::util::ThreadPool threadpool_;
};
}  // namespace client::replica_state_snapshot_client