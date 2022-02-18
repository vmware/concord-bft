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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#include <stdexcept>

#pragma once

namespace concord::client::concordclient {

// At the moment, we only allow one subscriber at a time. This exception is thrown if the caller subscribes while an
// active subscription is in progress already.
class SubscriptionExists : public std::runtime_error {
 public:
  SubscriptionExists() : std::runtime_error("subscription exists already"){};
};

// An ongoing subscription may request an update that has not yet been added to the blockchain
class UpdateNotFound : public std::runtime_error {
 public:
  UpdateNotFound() : std::runtime_error("requested update does not exist yet"){};
};

// A new subscription may request an out of range update
class OutOfRangeSubscriptionRequest : public std::runtime_error {
 public:
  OutOfRangeSubscriptionRequest() : std::runtime_error("out of range subscription request"){};
};

// An internal error may occur, for example when max_agreeing
class InternalError : public std::runtime_error {
 public:
  InternalError() : std::runtime_error("an internal error occurred"){};
};

// The stream ended
class EndOfStream : public std::runtime_error {
 public:
  EndOfStream() : std::runtime_error("Stream Ended Successfully"){};
};

// An internal error may occur due to service unavailability.
class StreamUnavailable : public std::runtime_error {
 public:
  StreamUnavailable() : std::runtime_error("Stream is not available"){};
};

// An internal error may occur due to service unavailability.
class RequestOverload : public std::runtime_error {
 public:
  RequestOverload() : std::runtime_error("Server overloaded with requests"){};
};

}  // namespace concord::client::concordclient
