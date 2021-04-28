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

#include <chrono>
#include <map>
#include <set>
#include <string>
#include <variant>
#include <vector>
#include <optional>

#include "bftclient/base_types.h"
#include "bftclient/quorums.h"
#include "secret_data.h"

using namespace std::chrono_literals;

namespace bft::client {

// This config is based on the parameters to the DynamicUpperLimitWithSimpleFilter.
// The defaults are set to the original defaults in SimpleClientImp.
struct RetryTimeoutConfig {
  // The starting retry timeout before we have any calculations.
  std::chrono::milliseconds initial_retry_timeout = 150ms;

  // The minimum that the timeout can be set to dynamically.
  std::chrono::milliseconds min_retry_timeout = 50ms;

  // The maximum that the timeout can be set to dynamically.
  std::chrono::milliseconds max_retry_timeout = 1s;

  // The number of standard deviations within the average that we expect reply times to fall into.
  uint16_t number_of_standard_deviations_to_tolerate = 2;

  // The number of replies we sample before we attempt to recalculate the rolling average and variance.
  uint16_t samples_per_evaluation = 32;

  // The number of samples before we reset the rolling average and variance to its empty state
  // A negative number will turn off resets.
  int16_t samples_until_reset = 1000;

  // The scaling factor at which the timeout can be increased. A factor of 2 means that the upper
  // limit can be doubled on each evaluation period.
  double max_increasing_factor = 2;

  // The scaling factor at which the timeout can be decreased. A factor of 2 means that the upper
  // limit can be halved on each evaluation period.
  double max_decreasing_factor = 2;
};

// The configuration for a single instance of a client.
struct ClientConfig {
  ClientId id;
  std::set<ReplicaId> all_replicas;
  std::set<ReplicaId> ro_replicas;
  uint16_t f_val;
  uint16_t c_val;
  RetryTimeoutConfig retry_timeout_config;
  std::optional<std::string> transaction_signing_private_key_file_path = std::nullopt;
  std::optional<concord::secretsmanager::SecretData> secrets_manager_config = std::nullopt;
};

// Generic per-request configuration shared by reads and writes.
struct RequestConfig {
  bool pre_execute = false;
  uint64_t sequence_number = 0;
  uint32_t max_reply_size = 64 * 1024;
  std::chrono::milliseconds timeout = 5s;
  std::string correlation_id = "";
  std::string span_context = "";
  bool key_exchange = false;
  bool reconfiguration = false;
};

// The configuration for a single write request.
struct WriteConfig {
  RequestConfig request;
  WriteQuorum quorum;
};

// The configuration for a single read request.
struct ReadConfig {
  RequestConfig request;
  ReadQuorum quorum;
};

struct WriteRequest {
  WriteConfig config;
  Msg request;
};

}  // namespace bft::client
