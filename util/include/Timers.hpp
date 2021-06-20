// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <functional>
#include <cstdint>
#include <chrono>
#include <vector>
#include <algorithm>
#include <mutex>

namespace concordUtil {

// A collection of timers backed by a vector.
class Timers {
 public:
  class Handle {
   public:
    // We can only create handles outside timers_.add that are in a default
    // invalid state. Timers will never have id = 0.
    //
    // This is useful for initializing handles as class members when
    // dynamically computing the actual timeout in the constructor of the
    // class containing the handle.
    Handle() : id_(0) {}

   private:
    explicit Handle(uint64_t id) : id_(id) {}

    uint64_t id_;
    friend class Timers;
  };

  class Timer {
   public:
    enum Type {
      ONESHOT,
      RECURRING,
    };

   private:
    Timer(std::chrono::milliseconds duration, Type t, const std::function<void(Handle)>& callback)
        : Timer(duration, t, callback, std::chrono::steady_clock::now()) {}

    Timer(std::chrono::milliseconds d,
          Type t,
          std::function<void(Handle)> cb,
          std::chrono::steady_clock::time_point now)
        : duration_(d), expires_at_(now + d), type_(t), callback_(std::move(cb)) {}

    bool expired(std::chrono::steady_clock::time_point now) const { return now >= expires_at_; }

    bool recurring() const { return type_ == Type::RECURRING; }

    void run_callback(Handle h) { callback_(h); }

    void reset(std::chrono::steady_clock::time_point now) { expires_at_ = now + duration_; }

    void reset(std::chrono::steady_clock::time_point now, std::chrono::milliseconds d) {
      duration_ = d;
      expires_at_ = now + duration_;
    }

    std::chrono::milliseconds duration_;
    std::chrono::steady_clock::time_point expires_at_;
    Type type_;
    uint64_t id_ = 0;
    std::function<void(Handle)> callback_;

    friend class Timers;
  };

 public:
  Timers() : id_counter_(0) {}
  Timers(const Timers& timers) = delete;
  Timers& operator=(const Timers& timers) = delete;
  Timers(Timers&& timers) = delete;
  Timers&& operator=(Timers&& timers) = delete;

  Handle add(std::chrono::milliseconds d, Timer::Type t, const std::function<void(Handle)>& cb) {
    return add(d, t, cb, std::chrono::steady_clock::now());
  }

  Handle add(std::chrono::milliseconds d,
             Timer::Type t,
             const std::function<void(Handle)>& cb,
             std::chrono::steady_clock::time_point now) {
    std::unique_lock<std::recursive_mutex> mlock(lock_);
    timers_.emplace_back(Timer(d, t, cb, now));
    id_counter_ += 1;
    Handle h{id_counter_};
    timers_.back().id_ = h.id_;
    return h;
  }

  std::vector<Timer>::iterator find(Handle handle) {
    std::unique_lock<std::recursive_mutex> mlock(lock_);
    auto it = std::find_if(timers_.begin(), timers_.end(), [&handle](const Timer& t) { return t.id_ == handle.id_; });
    if (it != timers_.end())
      return it;
    else
      throw std::invalid_argument("Invalid timer handle");
  }

  void reset(Handle handle, std::chrono::milliseconds d) { reset(handle, d, std::chrono::steady_clock::now()); }

  void reset(Handle handle, std::chrono::milliseconds d, std::chrono::steady_clock::time_point now) {
    std::unique_lock<std::recursive_mutex> mlock(lock_);
    find(handle)->reset(now, d);
  }

  void cancel(Handle handle) {
    std::unique_lock<std::recursive_mutex> mlock(lock_);
    timers_.erase(find(handle));
  }

  // Run the callbacks for all expired timers, and reschedule them if they are recurring.
  void evaluate() { evaluate(std::chrono::steady_clock::now()); }

  void evaluate(std::chrono::steady_clock::time_point now) {
    std::unique_lock<std::recursive_mutex> mlock(lock_);
    if (timers_.empty()) return;

    // Expired ids must be stored separately since erasing causes iterator invalidation.
    std::vector<Handle> to_cancel;
    for (auto& timer : timers_) {
      if (timer.expired(now)) {
        timer.run_callback(Handle(timer.id_));
        if (timer.recurring()) {
          timer.reset(now);
        } else {
          to_cancel.push_back(Handle(timer.id_));
        }
      }
    }

    for (auto& handle : to_cancel) {
      cancel(handle);
    }
  }

 private:
  std::recursive_mutex lock_;
  std::vector<Timer> timers_;
  uint64_t id_counter_;
};

}  // namespace concordUtil
