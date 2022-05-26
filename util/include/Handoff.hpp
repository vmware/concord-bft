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

#include <queue>
#include <array>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <exception>

#include "Logger.hpp"
#include "kvstream.h"

// uncomment to add debug prints
// #define HANDOFF_DO_DEBUG

#undef DEBUG_PRINT
#undef SET_LOG_LEVEL
#ifdef HANDOFF_DO_DEBUG
#define DEBUG_PRINT(x, y) LOG_INFO(x, y)
#define SET_LOG_LEVEL(x) logger_.setLogLevel(x);
#else
#define DEBUG_PRINT(x, y)
#define SET_LOG_LEVEL(x)
#endif

namespace concord::util {

/**
 * class PriorityQueue
 *
 * This template class implements a simple priority queue. It supports up to <S> priorities.
 * Elements of type T are pushed into the queue with a given priority by calling push(...).
 * To check the next element which is going to be popped, without popping it, the user should call front().
 * Higher (larger) priority elements are processed before lower priority elements. Elements with the same priority are
 * treated FIFO.
 * To get the total number of elements in the priority queue call size().
 * To check if the priority queue is empty call empty().
 *
 * Comments:
 * 1) std::priority_queue is not the same. Working with a priority_queue is similar to working with a heap.
 * It will not be efficient to implement a ?Compare function which makes sure that the elements are popped in the exact
 * same order they were inserted, for any two elements with an equal priority.
 * 2) Calling pop() and front() on an empty queue is treated as an error and throws a runtime_error exception.
 */
template <typename T, size_t S>
class PriorityQueue {
 public:
  PriorityQueue() = delete;
  PriorityQueue(const std::string&& name = "") : num_elements_{0}, name_(std::move(name)) {
    static_assert(S > 1, "PriorityQueue must have at least 2 subqueues!");
  };

  // Insert an element to the end of the queue by priority. Higher priority elements are processed before lower
  // priority elements. Elements with the same priority are treated FIFO.
  void push(const T&& element, size_t priority) {
    if (priority >= pq_.size()) {
      throw std::invalid_argument(name_ + ": Invalid priority " + std::to_string(priority));
    }
    pq_[priority].push(std::forward<decltype(element)>(element));
    ++num_elements_;
  }

  // Pop the next highest priority element (FIFO).
  // Higher priorities are in the back of the array pq_ (e.g priority 1 is popped before priority 0).
  // Calling this function on an empty container triggers an exception.
  void pop() {
    for (int i{S - 1}; i >= 0; --i) {
      if (!pq_[i].empty()) {
        pq_[i].pop();
        --num_elements_;
        return;
      }
    }

    // calling pop on an empty objet is illegal
    throw std::runtime_error(name_ + ": priority queue is empty, pop is prohibited!");
  }

  // Returns a const reference to the next highest priority element. The element stays in queue.
  // Higher priorities are in the back of the array (e.g priority 1 is popped before priority 0).
  // Calling this function on an empty container triggers an exception.
  decltype(auto) front() const {
    for (int i{S - 1}; i >= 0; --i) {
      if (!pq_[i].empty()) {
        const auto& element = pq_[i].front();
        return std::forward<decltype(element)>(element);
      }
    }

    // calling pop on an empty objet is illegal
    throw std::runtime_error(name_ + ": priority queue is empty, front is prohibited!");
  }

  size_t size() const { return num_elements_; }
  bool empty() const { return (num_elements_ == 0); }

 protected:
  std::array<std::queue<T>, S> pq_;
  size_t num_elements_;
  std::string name_;
};

/**
 * Simple handing off to a dedicated thread functionality (supports a single consumer / multiple producers).
 * Implemented by passing a function with a trivial signature void(void).
 * Parameters are bound to an arbitrary function by a caller.
 *
 * Default behavior is asynchronous: caller push the function and may continue doing other things. If caller would like
 * to block, it must call with synchronous_call=true. Synchronous call get higher priority over asynchronous calls.
 */
class Handoff {
 protected:
  using guard = std::lock_guard<std::mutex>;
  using func_t = std::function<void()>;
  struct HandoffElement;

 public:
  Handoff(std::uint16_t replicaId, const std::string&& name = "")
      : tasks_(name + "_PriorityQueue"),
        name_(std::move(name)),
        log_prefix_(name_ + ": "),
        logger_{logging::getLogger("concord.util.handoff")},
        element_id_counter_{0} {
    // keep for debugging
    SET_LOG_LEVEL(log4cplus::TRACE_LOG_LEVEL);

    thread_ = std::thread([this, replicaId] {
      try {
        MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(replicaId));
        MDC_PUT(MDC_THREAD_KEY, "handoff");
        DEBUG_PRINT(logger_, log_prefix_ << "consumer: entering loop...");
        for (;;) {
          HandoffElement element = pop();
          // keep for debug
          DEBUG_PRINT(logger_,
                      std::boolalpha << log_prefix_ << "consumer: before calling id=" << element.id
                                     << " blocked_call:" << (element.blocked_call));
          element.func();
          DEBUG_PRINT(logger_, log_prefix_ << "consumer: after calling id=" << element.id);

          if (element.blocked_call) {
            std::unique_lock<std::mutex> ul(tasks_lock_);
            element.completion_flag = true;
            element.completion_cond.notify_one();
          }
        }
      } catch (ThreadCanceledException& e) {
        LOG_INFO(logger_, log_prefix_ << "thread cancelled:" << KVLOG(thread_id_.load()));
      } catch (const std::exception& e) {
        LOG_FATAL(logger_, log_prefix_ << "exception: " << e.what());
        std::terminate();
      }
    });
    thread_id_.store(thread_.get_id());
    LOG_INFO(logger_, log_prefix_ << "started handoff consumer thread:" << KVLOG(thread_id_.load()));
  }

  ~Handoff() { stop(); }

  void stop() {
    if (stopped_.exchange(true)) {
      return;
    }
    tasks_cond_.notify_one();
    thread_.join();
    LOG_INFO(logger_, log_prefix_ << "thread joined:" << KVLOG(thread_id_.load()));
  }

  // By default, calls are blocked (synchronized)
  void push(func_t&& f, bool blocked_call = true) {
    std::condition_variable completion_cond;
    bool completion_flag{false};

    auto thread_id = thread_id_.load();
    if (blocked_call && (thread_id == std::this_thread::get_id())) {
      // As long as we have a single consumer, it doesn't make sense to push self events & block, this is a sure
      // deadlock. In that case call the function here and return.
      DEBUG_PRINT(logger_, log_prefix_ << "producer: is also consumer and blocked, execute without handoff!");
      f();
      return;
    }

    uint64_t id{};
    {
      guard g(tasks_lock_);
      id = element_id_counter_;
      // keep for debug
      DEBUG_PRINT(logger_, log_prefix_ << "producer: before pushing element id=" << id << " #tasks=" << tasks_.size());
      // a blocking call gets priority 1, while a non blocking call gets priority 0
      tasks_.push(HandoffElement{id, std::move(f), blocked_call, std::ref(completion_cond), std::ref(completion_flag)},
                  blocked_call ? 1 : 0);
      // keep for debug
      DEBUG_PRINT(logger_, log_prefix_ << "producer: after pushing element id=" << id);
      ++element_id_counter_;
    }
    tasks_cond_.notify_one();

    if (blocked_call) {
      std::unique_lock<std::mutex> ul(tasks_lock_);
      DEBUG_PRINT(logger_,
                  log_prefix_ << "producer: before waiting on element id=" << id
                              << " completion_flag address: " << completion_flag);
      completion_cond.wait(ul, [&completion_flag] { return completion_flag; });
      DEBUG_PRINT(logger_, log_prefix_ << "producer: after waiting on element id=" << id);
    }
  }

  size_t size() const {
    guard g(tasks_lock_);
    return tasks_.size();
  }

  bool empty() const {
    guard g(tasks_lock_);
    return tasks_.empty();
  }

 protected:
  class ThreadCanceledException : public std::runtime_error {
   public:
    ThreadCanceledException(const std::string& log_prefix) : std::runtime_error(log_prefix + "thread cancelled") {}
    const char* what() const noexcept override { return std::runtime_error::what(); }
  };  // namespace concord::util

  HandoffElement pop() {
    while (true) {
      std::unique_lock<std::mutex> ul(tasks_lock_);
      DEBUG_PRINT(logger_, log_prefix_ << "consumer: before waiting..");
      tasks_cond_.wait(ul, [this] { return !tasks_.empty() || stopped_; });
      DEBUG_PRINT(logger_, log_prefix_ << "consumer: after waiting..");
      if (!stopped_ || (stopped_ && !tasks_.empty())) {
        auto element = tasks_.front();
        tasks_.pop();
        return element;
      }
      LOG_INFO(logger_, log_prefix_ << "Handoff thread stopped!");
      throw ThreadCanceledException(log_prefix_);
    }
  }

 protected:
  struct HandoffElement {
    HandoffElement() = delete;
    HandoffElement(
        uint64_t _id, func_t _func, bool _blocked_call, std::condition_variable& _cond, bool& _completion_flag)
        : id(_id),
          func(_func),
          blocked_call(_blocked_call),
          completion_cond(_cond),
          completion_flag(_completion_flag) {}
    uint64_t id;
    func_t func;
    bool blocked_call;
    std::condition_variable& completion_cond;
    bool& completion_flag;
  };

  PriorityQueue<HandoffElement, 2> tasks_;
  mutable std::mutex tasks_lock_;
  std::condition_variable tasks_cond_;
  std::atomic_bool stopped_{false};
  std::thread thread_;
  std::atomic<std::thread::id> thread_id_;
  std::string name_ = "handoff";
  std::string log_prefix_;
  logging::Logger logger_;
  uint64_t element_id_counter_;
};  // class Handoff

}  // namespace concord::util
