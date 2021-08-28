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
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

namespace concord::util {

class SimpleThreadPool {
  typedef std::lock_guard<std::mutex> guard;

 public:
  // TODO change to managed object
  class Job {
   public:
    virtual void execute() = 0;
    virtual void release() = 0;  // TK 'delete this' is not called in all subclasses' release() of Job causing mem leak
   protected:
    ~Job(){};  // should not be deleted directly - use  release()
  };

  SimpleThreadPool() : stopped_(true) {}

  /**
   * starts the thread pool with desired number of threads
   */
  void start(uint8_t num_of_threads = 1);
  /**
   * stops the thread pool
   * @param executeAllJobs - whether to execute remaining jobs
   */
  void stop(bool executeAllJobs = false);
  /**
   * add a job for execution
   * @param j - subclass of Job for execution
   */
  void add(Job* j);
  /**
   * get the number of currently allocated threads in pool
   */
  size_t getNumOfThreads() {
    guard g(queue_lock_);
    return threads_.size();
  }
  /**
   * get the number of jobs in queue
   */
  size_t getNumOfJobs() {
    guard g(queue_lock_);
    return job_queue_.size();
  }

 protected:
  bool load(Job*& outJob);
  void execute(Job*);

 protected:
  std::queue<SimpleThreadPool::Job*> job_queue_;
  std::mutex queue_lock_;
  std::condition_variable queue_cond_;
  bool stopped_;
  int num_of_free_threads_ = 0;
  std::mutex threads_startup_lock_;
  std::condition_variable threads_startup_cond_;
  std::vector<std::thread> threads_;
};

}  // namespace concord::util
