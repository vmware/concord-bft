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

#include "SimpleThreadPool.hpp"
#include "Logger.hpp"
#include <exception>
#include <iostream>
#include <mutex>

logging::Logger SP = logging::getLogger("thread-pool");
namespace util {

void SimpleThreadPool::start(uint8_t num_of_threads) {
  stopped_ = false;
  guard g(queue_lock_);
  for (auto i = 0; i < num_of_threads; ++i) {
    threads_.emplace_back(std::thread([this, num_of_threads] {
      LOG_DEBUG(SP, "thread start " << std::this_thread::get_id());
      {
        std::unique_lock<std::mutex> ul(threads_startup_lock_);
        num_of_free_threads_++;
        if (num_of_free_threads_ == num_of_threads) threads_startup_cond_.notify_one();
      }
      SimpleThreadPool::Job* j = nullptr;
      while (load(j)) {
        execute(j);
        j->release();
        j = nullptr;
      }
    }));
  }
  std::unique_lock<std::mutex> ul(threads_startup_lock_);
  threads_startup_cond_.wait(ul, [this, num_of_threads] { return (num_of_free_threads_ == num_of_threads); });
}

void SimpleThreadPool::stop(bool executeAllJobs) {
  {
    std::unique_lock<std::mutex> l{queue_lock_};
    stopped_ = true;
  }
  queue_cond_.notify_all();
  for (auto&& t : threads_) {
    auto tid = t.get_id();
    t.join();
    LOG_DEBUG(SP, "thread joined " << tid);
  }
  threads_.clear();
  // no more concurrent threads, can cleanup without locking
  LOG_DEBUG(SP, "will " << (executeAllJobs ? "execute " : "discard ") << job_queue_.size() << " jobs in queue");
  while (!job_queue_.empty()) {
    Job* j = job_queue_.front();
    job_queue_.pop();
    if (executeAllJobs) execute(j);

    j->release();
  }
}

void SimpleThreadPool::add(Job* j) {
  {
    guard g(queue_lock_);
    if (stopped_) return;
    job_queue_.push(j);
  }
  queue_cond_.notify_one();
}

bool SimpleThreadPool::load(Job*& outJob) {
  std::unique_lock<std::mutex> ul(queue_lock_);
  queue_cond_.wait(ul, [this] { return !(job_queue_.empty() && !stopped_); });
  if (!stopped_) {
    outJob = job_queue_.front();
    job_queue_.pop();
  }
  return !stopped_;
}

void SimpleThreadPool::execute(Job* j) {
  try {
    j->execute();
  } catch (std::exception& e) {
    LOG_FATAL(SP, "SimpleThreadPool: exception during execution of " << typeid(*j).name() << " Reason: " << e.what());
    std::terminate();
  } catch (...) {
    LOG_FATAL(SP, "SimpleThreadPool: unknown exception during execution of " << typeid(*j).name());
    std::terminate();
  }
}

}  // namespace util
