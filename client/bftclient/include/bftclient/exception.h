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

#include <stdexcept>
#include <string>

#include "bftclient/config.h"

namespace bft::client {

class BftClientException : public std::runtime_error::runtime_error {
 public:
  explicit BftClientException(const std::string& what) : runtime_error(what) {}
};

class BadQuorumConfigException : public BftClientException {
 public:
  BadQuorumConfigException(const std::string& what) : BftClientException(what) {}
};

class InvalidDestinationException : public BftClientException {
 public:
  InvalidDestinationException(ReplicaId replica_id)
      : BftClientException("Replica: " + std::to_string(replica_id.val) + " is not part of the cluster.") {}

  InvalidDestinationException() : BftClientException("MofN quorums must have destinations") {}
};

class TimeoutException : public BftClientException {
 public:
  TimeoutException(uint64_t seq_num, const std::string& cid)
      : BftClientException("Timeout for request sequence number: " + std::to_string(seq_num) +
                           ", and correlation id: " + cid) {}
};

class BatchTimeoutException : public BftClientException {
 public:
  BatchTimeoutException(const std::string& cid)
      : BftClientException("Timeout for a batch request with correlation id: " + cid) {}
};

class InvalidPrivateKeyException : public BftClientException {
 public:
  InvalidPrivateKeyException(std::string& file_path, bool encrypted)
      : BftClientException("Failed to retrive transaction signing private key, path: " + file_path +
                           ", is_encrypted: " + std::to_string(encrypted)) {}
};

}  // namespace bft::client
