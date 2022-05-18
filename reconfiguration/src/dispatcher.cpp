// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "reconfiguration/dispatcher.hpp"

using namespace concord::messages;

namespace concord::reconfiguration {

#define ADDITIONAL_DATA(resp, x)                                                     \
  {                                                                                  \
    std::ostringstream oss;                                                          \
    oss << (x);                                                                      \
    std::string str = oss.str();                                                     \
    std::copy(str.cbegin(), str.cend(), std::back_inserter((resp).additional_data)); \
  }

ReconfigurationResponse Dispatcher::dispatch(const ReconfigurationRequest& request,
                                             uint64_t sequence_num,
                                             const std::optional<bftEngine::Timestamp>& timestamp) {
  ReconfigurationResponse rresp;
  concord::messages::ReconfigurationErrorMsg error_msg;
  bool valid = false;
  ReconfigurationRequest request_without_sig = request;
  request_without_sig.signature = {};
  std::vector<uint8_t> serialized_cmd;
  concord::messages::serialize(serialized_cmd, request_without_sig);
  auto ser_data = std::string(serialized_cmd.begin(), serialized_cmd.end());
  auto ser_sig = std::string(request.signature.begin(), request.signature.end());
  rresp.success = true;
  auto sender_id = request.sender;
  executions_++;
  try {
    // Run pre-reconfiguration handlers
    for (auto& handler : pre_reconfig_handlers_) {
      // Each reconfiguration handler handles only what it can validate
      if (!handler->verifySignature(sender_id, ser_data, ser_sig)) {
        error_msg.error_msg = "Invalid signature";
        continue;
      }
      error_msg.error_msg.clear();
      valid = true;
      rresp.success &=
          std::visit([&](auto&& arg) { return handleRequest(arg, sequence_num, sender_id, timestamp, rresp, handler); },
                     request.command);
    }

    // Run regular reconfiguration handlers
    for (auto& handler : reconfig_handlers_) {
      // Each reconfiguration handler handles only what it can validate
      if (!handler->verifySignature(sender_id, ser_data, ser_sig)) {
        error_msg.error_msg = "Invalid signature";
        continue;
      }
      error_msg.error_msg.clear();
      valid = true;
      rresp.success &=
          std::visit([&](auto&& arg) { return handleRequest(arg, sequence_num, sender_id, timestamp, rresp, handler); },
                     request.command);
    }

    // Run post-reconfiguration handlers
    for (auto& handler : post_reconfig_handlers_) {
      // Each reconfiguration handler handles only what it can validate
      if (!handler->verifySignature(sender_id, ser_data, ser_sig)) {
        error_msg.error_msg = "Invalid signature";
        continue;
      }
      error_msg.error_msg.clear();
      valid = true;
      rresp.success &=
          std::visit([&](auto&& arg) { return handleRequest(arg, sequence_num, sender_id, timestamp, rresp, handler); },
                     request.command);
    }

    if (!valid) {
      failures_++;
      rresp.success = false;  // If no handler was able to verify the request, it is an invalid request
      LOG_WARN(getLogger(), "Failed to verify the reconfiguration request. " << KVLOG(request.sender, sequence_num));
    }
  } catch (const std::exception& e) {
    failures_++;
    rresp.success = false;
    LOG_ERROR(getLogger(),
              "Reconfiguration request from sender: "
                  << request.sender << " seqnum:" << std::to_string(sequence_num) + " failed, exception: " + e.what());
    ADDITIONAL_DATA(rresp,
                    "Reconfiguration request " + std::to_string(sequence_num) + " failed, exception: " + e.what());
  }
  component_.UpdateAggregator();
  return rresp;
}
Dispatcher::Dispatcher()
    : aggregator_{std::make_shared<concordMetrics::Aggregator>()},
      component_{"reconfiguration_dispatcher", aggregator_},
      executions_{component_.RegisterCounter("executions")},
      failures_{component_.RegisterCounter("failures")},
      auth_failures_{component_.RegisterCounter("auth_failures")} {
  component_.Register();
  component_.UpdateAggregator();
}

}  // namespace concord::reconfiguration
