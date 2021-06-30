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

namespace concord::client::concordclient {

void ConcordClient::subscribe(const SubscribeRequest& request,
                              const std::unique_ptr<opentracing::Span>& parent_span,
                              std::function<void(SubscribeResult&&)> callback) {
  if (not stop_subscriber_) {
    LOG_ERROR(logger_, "subscription already in progress - unsubscribe first");
    callback(SubscribeResult{SubscribeError{}});
    return;
  }

  stop_subscriber_ = false;
  subscriber_ = std::thread([&] {
    while (not stop_subscriber_) {
      // Note: The following returns an artificial event group.
      // This will be replaced with the actual thin replica client integration.
      // The thread is in place to simulate the asynchronous subscription.
      EventGroup eg;
      eg.id = request.event_group_id;
      std::string event = std::to_string(request.event_group_id);
      eg.events.push_back({event.begin(), event.end()});
      std::chrono::duration time_now = std::chrono::system_clock::now().time_since_epoch();
      eg.record_time = std::chrono::duration_cast<std::chrono::microseconds>(time_now);
      eg.trace_context = {};

      callback(SubscribeResult{eg});
    }
  });
}

void ConcordClient::unsubscribe() {
  if (stop_subscriber_ == false) {
    LOG_INFO(logger_, "Closing subscription");
    stop_subscriber_ = true;
    subscriber_.join();
  }
}

}  // namespace concord::client::concordclient
