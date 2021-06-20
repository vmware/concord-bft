// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>

namespace concord {
namespace thin_replica {

// Interface to create and set TRS metrics using any external monitoring system
// of choice, for e.g. Prometheus, OpenTSDB. The implementations of the methods
// must be thread-safe, as metrics will be updated per client connection. Note
// that metrics such as last_sent_block_id and live_update_queue_size are unique
// per client connection (uniquely identified by stream_type and client_id)
// whereas subscriber_list_size is not.
class ThinReplicaServerMetrics {
 public:
  virtual ~ThinReplicaServerMetrics(){};
  // set the gauge metric_subscriber_list_size_
  virtual void setSubscriberListSize(size_t subscriber_list_size) = 0;
  // set the gauge metric_last_sent_block_id
  virtual void setLastSentBlockId(const std::string& stream_type,
                                  const std::string& client_id,
                                  std::uint64_t block_id) = 0;
  // set the gauge metric_queue_size
  virtual void setLiveUpdateQueueSize(const std::string& stream_type,
                                      const std::string& client_id,
                                      size_t queue_size) = 0;
};

// Ignores TRS metrics
class IgnoreTrsMetrics : public ThinReplicaServerMetrics {
 public:
  IgnoreTrsMetrics() {}

  void setSubscriberListSize(size_t subscriber_list_size) override {}

  void setLastSentBlockId(const std::string& stream_type,
                          const std::string& client_id,
                          std::uint64_t block_id) override {}

  void setLiveUpdateQueueSize(const std::string& stream_type,
                              const std::string& client_id,
                              size_t queue_size) override {}
};
}  // namespace thin_replica
}  // namespace concord
