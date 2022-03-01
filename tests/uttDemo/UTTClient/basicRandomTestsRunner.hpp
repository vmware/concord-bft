// Concord
//
// Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
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

#include "Logger.hpp"
#include "simpleKVBTestsBuilder.hpp"
#include "KVBCInterfaces.h"

namespace concord::kvbc::test {

class BasicRandomTestsRunner {
 public:
  BasicRandomTestsRunner(IClient *client) : client_(client) {
    ConcordAssert(!client_->isRunning());
    client_->start();
  }
  ~BasicRandomTestsRunner() { client_->stop(); }
  void run(size_t numOfOperations);

 private:
  static void sleep(int ops);

  BlockId getInitialLastBlockId();

 private:
  logging::Logger logger_ = logging::getLogger("concord.kvbc.tests.simple_kvbc");
  std::unique_ptr<concord::kvbc::IClient> client_;
  std::unique_ptr<TestsBuilder> testsBuilder_;
};

}  // namespace concord::kvbc::test
