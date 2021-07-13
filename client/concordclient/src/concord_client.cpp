// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "client/concordclient/concord_client.hpp"
#include "client/thin-replica-client/thin_replica_client.hpp"

using ::client::thin_replica_client::BasicUpdateQueue;
using ::client::thin_replica_client::ThinReplicaClient;
using ::client::thin_replica_client::ThinReplicaClientConfig;
using ::client::thin_replica_client::TrsConnection;

namespace concord::client::concordclient {

ConcordClient::ConcordClient(const ConcordClientConfig& config)
    : logger_(logging::getLogger("concord.client.concordclient")), config_(config) {
  std::vector<std::unique_ptr<TrsConnection>> trs_connections;
  for (const auto& replica : config_.topology.replicas) {
    auto addr = replica.host + ":" + std::to_string(replica.event_port);
    auto trsc = std::make_unique<TrsConnection>(addr, config_.subscribe_config.id, /* TODO */ 3, /* TODO */ 3);
    trs_connections.push_back(std::move(trsc));
  }
  trc_queue_ = std::make_shared<BasicUpdateQueue>();
  auto trc_config = std::make_unique<ThinReplicaClientConfig>(
      config_.subscribe_config.id, trc_queue_, config_.topology.f_val, std::move(trs_connections));
  trc_ = std::make_unique<ThinReplicaClient>(std::move(trc_config), metrics_);
}

void ConcordClient::send(const bft::client::ReadConfig& config,
                         bft::client::Msg&& msg,
                         const std::unique_ptr<opentracing::Span>& parent_span,
                         const std::function<void(SendResult&&)>& callback) {
  LOG_INFO(logger_, "Log message until config is used f=" << config_.topology.f_val);
  bft::client::Reply reply;
  reply.matched_data = std::move(msg);
  callback(SendResult{reply});
}

void ConcordClient::send(const bft::client::WriteConfig& config,
                         bft::client::Msg&& msg,
                         const std::unique_ptr<opentracing::Span>& parent_span,
                         const std::function<void(SendResult&&)>& callback) {
  bft::client::Reply reply;
  reply.matched_data = std::move(msg);
  callback(SendResult{reply});
}

void ConcordClient::subscribe(const SubscribeRequest& request,
                              const std::unique_ptr<opentracing::Span>& parent_span,
                              const std::function<void(SubscribeResult&&)>& callback) {
  if (subscriber_) {
    LOG_ERROR(logger_, "subscription already in progress - unsubscribe first");
    throw SubscriptionExists();
  }

  // TODO: Define SubscribeRequest in TRC
  trc_->Subscribe(::client::thin_replica_client::SubscribeRequest{});

  stop_subscriber_ = false;
  subscriber_ = std::make_unique<std::thread>([&] {
    while (not stop_subscriber_) {
      auto update = trc_queue_->TryPop();
      if (not update) {
        // We need to check if the client cancelled the subscription.
        // Therefore, we cannot block via Pop(). Can we do bettern than sleep?
        std::this_thread::sleep_for(10ms);
        continue;
      }

      // TODO: We fill event group with data from legacy updates.
      // This needs to change depending on how the legacy API will be implemented.
      EventGroup eg;
      eg.id = update->block_id;
      for (const auto& e : update->kv_pairs) {
        eg.events.push_back({e.second.begin(), e.second.end()});
      }
      std::chrono::duration time_now = std::chrono::system_clock::now().time_since_epoch();
      eg.record_time = std::chrono::duration_cast<std::chrono::microseconds>(time_now);
      eg.trace_context = {};

      callback(SubscribeResult{eg});
    }
  });
}

void ConcordClient::unsubscribe() {
  if (stop_subscriber_ == false) {
    LOG_INFO(logger_, "Closing subscription. Waiting for subscriber to finish.");
    stop_subscriber_ = true;
    subscriber_->join();
    subscriber_.reset();
    LOG_INFO(logger_, "Subscriber finished.");
  }
}

}  // namespace concord::client::concordclient
