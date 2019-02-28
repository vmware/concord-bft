// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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
#include <assert.h>

namespace SimpleKVBC {
class Status {
 public:
  Status() : code(Code::ok), message() {}

  static Status OK() { return Status(); }

  static Status NotFound(const char* msg) {
    return Status(Code::notFound, msg);
  }

  static Status IllegalBlockSize(const char* msg) {
    return Status(Code::illegalBlockSize, msg);
  }

  static Status InvalidArgument(const char* msg) {
    return Status(Code::invalidArgument, msg);
  }

  static Status IllegalOperation(const char* msg) {
    return Status(Code::illegalOperation, msg);
  }

  static Status UnknownError(const char* msg) {
    return Status(Code::unknownError, msg);
  }

  bool ok() const { return (code == Code::ok); }
  bool IsNotFound() const { return code == Code::notFound; }
  bool IsIllegalBlockSize() const { return code == Code::illegalBlockSize; }
  bool IsInvalidArgument() const { return code == Code::invalidArgument; }
  bool IsUnknownError() const { return code == Code::unknownError; }

 protected:
  enum class Code {
    ok = 0,
    notFound,
    invalidArgument,
    illegalOperation,
    illegalBlockSize,
    unknownError = 100000,
  };

  Code code;
  std::string message;

  Status(Code code, const char* msg) {
    assert(code != Code::ok);
    this->code = code;
    this->message = msg;
  }
};
}  // namespace SimpleKVBC