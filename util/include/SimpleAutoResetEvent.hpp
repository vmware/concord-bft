// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <mutex>
#include <condition_variable>

class SimpleAutoResetEvent {
 private:
  std::mutex _lockMutex;
  std::condition_variable _signal;
  bool _signalled;

 public:
  SimpleAutoResetEvent(const SimpleAutoResetEvent&) = delete;
  SimpleAutoResetEvent& operator=(const SimpleAutoResetEvent&) = delete;
  SimpleAutoResetEvent(const SimpleAutoResetEvent&&) = delete;

  explicit SimpleAutoResetEvent(bool signalled) : _signalled{signalled} {}

  inline void reset() {
    std::lock_guard<std::mutex> lock(_lockMutex);
    _signalled = false;
  }

  inline void set() {
    std::lock_guard<std::mutex> lock(_lockMutex);
    if (!_signalled) {
      _signalled = true;
      _signal.notify_one();
    }
  }

  inline void wait_one() {
    std::unique_lock<std::mutex> lock(_lockMutex);
    _signal.wait(lock, [&] { return _signalled; });
    _signalled = false;
  }
};
