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

#include <queue>

using namespace std;

// TODO(GG): this class should be rewritten (use standard C++11 classes)

namespace bftEngine {
namespace impl {

struct InternalDataOfSimpleThreadPool;

class SimpleThreadPool {
 public:
  class Job {
   public:
    virtual void execute() = 0;
    virtual void release() = 0;

   protected:
    ~Job(){};  // should not be deleted directly - use  release()
  };

  class Controller {
   public:
    virtual ~Controller() {
    }  // TODO(GG): use release() (and make the destructor protected)
    virtual void onThreadBegin() = 0;
    virtual void onThreadEnd() = 0;
  };

  SimpleThreadPool();
  SimpleThreadPool(unsigned char numOfThreads);

  ~SimpleThreadPool();

  void start(Controller* c = nullptr);

  void stop(bool executeAllJobs = false);

  void add(Job* j);

  void waitForCompletion();

  // protected:

  const unsigned char numberOfThreads;

  InternalDataOfSimpleThreadPool* p = nullptr;

  bool load(Job*& outJob);
};

}  // namespace impl
}  // namespace bftEngine
