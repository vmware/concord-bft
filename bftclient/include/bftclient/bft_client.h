// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <memory>
#include <optional>

#include "communication/ICommunication.hpp"
#include "Logger.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"

#include "bftclient/config.h"
#include "matcher.h"
#include "msg_receiver.h"
#include "exception.h"
#include "metrics.h"

namespace bft::client {

class Client {
 public:
  Client(std::unique_ptr<bft::communication::ICommunication> comm, const ClientConfig& config)
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
        metrics_(config.id) {
    communication_->setReceiver(config_.id.val, &receiver_);
    communication_->Start();
  }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    metrics_.setAggregator(aggregator);
  }

  void stop() { communication_->Stop(); }

  // Send a message where the reply gets allocated by the callee and returned in a vector.
  // The message to be sent is moved into the caller to prevent unnecessary copies.
  //
  // Throws a BftClientException on error.
  Reply send(const WriteConfig& config, Msg&& request);
  Reply send(const ReadConfig& config, Msg&& request);

  // Useful for testing. Shouldn't be relied on in production.
  std::optional<ReplicaId> primary() { return primary_; }

 private:
  // Generic function for sending a read or write message.
  Reply send(const MatchConfig& match_config, const RequestConfig& request_config, Msg&& request, bool read_only);

  // Wait for messages until we get a quorum or a retry timeout.
  //
  // Return a Reply on quorum, or std::nullopt on timeout.
  std::optional<Reply> wait();

  // Send a Msg to all destinations in the configured quorum.
  void sendToGroup(const MatchConfig& config, const Msg& msg);

  // Extract a matcher configurations from operational configurations
  //
  // Throws BftClientException on error.
  MatchConfig writeConfigToMatchConfig(const WriteConfig&);
  MatchConfig readConfigToMatchConfig(const ReadConfig&);

  MsgReceiver receiver_;

  std::unique_ptr<bft::communication::ICommunication> communication_;
  ClientConfig config_;
  logging::Logger logger_ = logging::getLogger("bftclient");

  // The client doesn't always know the current primary.
  std::optional<ReplicaId> primary_;

  // Each outstanding request matches replies using a new matcher.
  // If there are no outstanding requests, then this is a nullopt;
  std::optional<Matcher> outstanding_request_;

  // A class that takes all Quorum types and converts them to an MofN quorum, with validation.
  QuorumConverter quorum_converter_;

  // A utility for calculating dynamic timeouts for replies.
  bftEngine::impl::DynamicUpperLimitWithSimpleFilter<uint64_t> expected_commit_time_ms_;

  Metrics metrics_;
};

}  // namespace bft::client
