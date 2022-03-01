// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "basicRandomTestsRunner.hpp"
#include "simpleKVBTestsBuilder.hpp"
#include "assertUtils.hpp"
#include <chrono>
#include <unistd.h>

using std::chrono::seconds;
using std::holds_alternative;
using namespace skvbc::messages;
namespace concord::kvbc::test {

void BasicRandomTestsRunner::sleep(int ops) {
  if (ops % 100 == 0) usleep(100 * 1000);
}

void BasicRandomTestsRunner::run(size_t numOfOperations) {
  testsBuilder_ = std::make_unique<TestsBuilder>(getInitialLastBlockId());
  testsBuilder_->createRandomTest(numOfOperations, 1111);
  ConcordAssert(testsBuilder_->getRequests().size() == testsBuilder_->getReplies().size());

  int ops = 0;
  while (!testsBuilder_->getRequests().empty()) {
    sleep(ops);
    SKVBCRequest request = testsBuilder_->getRequests().front();
    SKVBCReply expectedReply = testsBuilder_->getReplies().front();
    testsBuilder_->getRequests().pop_front();
    testsBuilder_->getReplies().pop_front();

    bool readOnly = !holds_alternative<SKVBCWriteRequest>(request.request);
    vector<uint8_t> serialized_request;
    vector<uint8_t> serialized_reply;
    skvbc::messages::serialize(serialized_request, request);
    skvbc::messages::serialize(serialized_reply, expectedReply);
    size_t expectedReplySize = serialized_reply.size();
    uint32_t actualReplySize = 0;
    LOG_INFO(logger_, "invoking: " << KVLOG(ops, readOnly));
    static_assert(
        (sizeof(*(serialized_request.data())) == sizeof(char)) && (sizeof(*(serialized_reply.data())) == sizeof(char)),
        "Byte pointer type used by concord::kvbc::IClient interface is incompatible with byte pointer type used by "
        "CMF.");
    auto res = client_->invokeCommandSynch(reinterpret_cast<char*>(serialized_request.data()),
                                           serialized_request.size(),
                                           readOnly,
                                           seconds(0),
                                           expectedReplySize,
                                           reinterpret_cast<char*>(serialized_reply.data()),
                                           &actualReplySize);

    SKVBCReply actualReply;
    deserialize(serialized_reply, actualReply);
    if ((holds_alternative<SKVBCWriteRequest>(request.request) or
         holds_alternative<SKVBCReadRequest>(request.request) or
         holds_alternative<SKVBCGetLastBlockRequest>(request.request)) and
        (expectedReply == actualReply)) {
      ops++;
    } else {
      LOG_FATAL(logger_, "*** Test failed: actual reply != expected reply.");
      ConcordAssert(0);
    }
  }
  sleep(1);
  LOG_INFO(logger_, "\n*** Test completed. " << ops << " messages have been handled.");
}

BlockId BasicRandomTestsRunner::getInitialLastBlockId() {
  SKVBCRequest request;
  request.request = SKVBCGetLastBlockRequest();
  vector<uint8_t> serialized_request;
  skvbc::messages::serialize(serialized_request, request);

  SKVBCReply reply;
  reply.reply = SKVBCGetLastBlockReply();
  vector<uint8_t> serialized_reply;
  skvbc::messages::serialize(serialized_reply, reply);
  size_t expected_reply_size = serialized_reply.size();
  uint32_t actual_reply_size = 0;

  static_assert(
      (sizeof(*(serialized_request.data())) == sizeof(char)) && (sizeof(*(serialized_reply.data())) == sizeof(char)),
      "Byte pointer type used by concord::kvbc::IClient interface is not compatible with byte pointer type used by "
      "CMF.");
  auto res = client_->invokeCommandSynch(reinterpret_cast<char*>(serialized_request.data()),
                                         serialized_request.size(),
                                         true,
                                         seconds(5),
                                         expected_reply_size,
                                         reinterpret_cast<char*>(serialized_reply.data()),
                                         &actual_reply_size);
  ConcordAssert(res.isOK());

  LOG_INFO(logger_, "Actual reply size = " << actual_reply_size << ", expected reply size = " << expected_reply_size);
  ConcordAssert(actual_reply_size == expected_reply_size);
  skvbc::messages::deserialize(serialized_reply, reply);
  ConcordAssert(holds_alternative<SKVBCGetLastBlockReply>(reply.reply));
  LOG_INFO(logger_, "last block id: " << (std::get<SKVBCGetLastBlockReply>(reply.reply)).latest_block);
  return (std::get<SKVBCGetLastBlockReply>(reply.reply)).latest_block;
}

}  // namespace concord::kvbc::test
