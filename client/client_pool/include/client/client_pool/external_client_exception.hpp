// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <exception>
#include <string>

#include <status.hpp>

namespace concord {
namespace external_client {

// A base class for external client exceptions.
class ClientExceptionBase : public std::exception {
 protected:
  ClientExceptionBase(const std::string& msg) : msg_{msg} {}

 public:
  // Returns an explanatory string. Overridden from std::exception .
  const char* what() const noexcept override { return msg_.c_str(); }

  // Returns an explanatory string.
  const std::string& Message() const noexcept { return msg_; }

 private:
  std::string msg_;
};

// An exception thrown on errors when sending client requests. Contains message
// and status fields.
class ClientRequestException : public ClientExceptionBase {
 public:
  ClientRequestException(const concordUtils::Status& status, const std::string& msg)
      : ClientExceptionBase(msg), status_{status} {
    status_msg_ = Message() + ": status: " + status_.toString();
  }

  // Returns a status describing the error that has occurred.
  const concordUtils::Status& Status() const noexcept { return status_; }

  // Returns an explanatory string. Overridden from std::exception .
  const char* what() const noexcept override { return status_msg_.c_str(); }

 private:
  concordUtils::Status status_;
  std::string status_msg_;
};

}  // namespace external_client
}  // namespace concord
