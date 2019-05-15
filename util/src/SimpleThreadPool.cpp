//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#include "SimpleThreadPool.hpp"
#include "Logging.hpp"
#include <iostream>

concordlogger::Logger GLOB = concordlogger::Logger::getLogger(DEFAULT_LOGGER_NAME);
namespace util
{

void SimpleThreadPool::start(uint8_t num_of_threads)
{
  stopped_ = false;
  guard g(queue_lock_);
  for (auto i = 0; i < num_of_threads; ++i)
  {
    threads_.push_back(std::thread([this]{
      LOG_DEBUG_F(GLOB, "thread start [%X]", std::this_thread::get_id());

      SimpleThreadPool::Job *j = nullptr;
      while (load(j))
      {
        execute(j);
        j->release();
        j = nullptr;
      }
    }));
  }
}

void SimpleThreadPool::stop(bool executeAllJobs)
{
  bool test_false = false;
  if (!stopped_.compare_exchange_strong(test_false, true))
  {
    // stop has already been called // TODO(TK) throw?
    LOG_ERROR(GLOB, "SimpleThreadPool::stop called more than once");
    return;
  }
  queue_cond_.notify_all();
  for (auto && t: threads_)
  {
    auto tid = t.get_id();
    t.join();
    LOG_DEBUG_F(GLOB, "thread joined [%X]", tid);
  }
  threads_.clear();
  // no more concurrent threads, can cleanup without locking
  LOG_DEBUG_F(GLOB, "will %s %u jobs in queue" , executeAllJobs?"execute":"discard", job_queue_.size());
  while (!job_queue_.empty())
  {
    Job* j = job_queue_.front();
    job_queue_.pop();
    if (executeAllJobs)
      execute(j);

    j->release();
  }
}

void SimpleThreadPool::add(Job* j)
{
  if (stopped_) return; // TODO(TK) throw?

  {
    guard g(queue_lock_);
    job_queue_.push(j);
  }

  queue_cond_.notify_one();
}

bool SimpleThreadPool::load(Job*& outJob)
{
  std::unique_lock<std::mutex> ul(queue_lock_);
  queue_cond_.wait(ul, [this]{return !(job_queue_.empty() && !stopped_);});
  if (!stopped_)
  {
    outJob = job_queue_.front();
    job_queue_.pop();
  }
  return !stopped_;
}

void SimpleThreadPool::execute(Job* j)
{
  try
  {
    j->execute();
  }catch(std::exception& e){
    LOG_ERROR_F(GLOB, "SimpleThreadPool: exception during execution of %s : %s"   , typeid(*j).name(), e.what());
  }catch(...){
    LOG_ERROR_F(GLOB, "SimpleThreadPool: unknown exception during execution of %s", typeid(*j).name());
  }
}

}
