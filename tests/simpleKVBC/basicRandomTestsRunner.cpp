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
#include "assertUtils.hpp"
#include <chrono>
#include <unistd.h>

using std::chrono::seconds;
using std::holds_alternative;
using std::list;

using concord::kvbc::IClient;
using skvbc::messages::SKVBCGetLastBlockRequest;
using skvbc::messages::SKVBCReadRequest;
using skvbc::messages::SKVBCReply;
using skvbc::messages::SKVBCRequest;
using skvbc::messages::SKVBCWriteRequest;

namespace BasicRandomTests {

BasicRandomTestsRunner::BasicRandomTestsRunner(logging::Logger& logger, IClient& client, size_t numOfOperations)
    : logger_(logger), client_(client), numOfOperations_(numOfOperations) {
  // We have to start the client here, since construction of the TestsBuilder
  // uses the client.
  ConcordAssert(!client_.isRunning());
  client_.start();
  testsBuilder_ = new TestsBuilder(logger_, client);
}

void BasicRandomTestsRunner::sleep(int ops) {
  if (ops % 100 == 0) usleep(100 * 1000);
}

void BasicRandomTestsRunner::run() {
  testsBuilder_->createRandomTest(numOfOperations_, 1111);

  list<SKVBCRequest> requests = testsBuilder_->getRequests();
  list<SKVBCReply> expectedReplies = testsBuilder_->getReplies();
  ConcordAssert(requests.size() == expectedReplies.size());

  int ops = 0;
  while (!requests.empty()) {
    sleep(ops);
    SKVBCRequest request = requests.front();
    SKVBCReply expectedReply = expectedReplies.front();
    requests.pop_front();
    expectedReplies.pop_front();

    bool readOnly = !holds_alternative<SKVBCWriteRequest>(request.request);
    vector<uint8_t> serialized_request;
    vector<uint8_t> serialized_reply;
    serialize(serialized_request, request);
    serialize(serialized_reply, expectedReply);
    size_t expectedReplySize = serialized_reply.size();
    uint32_t actualReplySize = 0;

    static_assert(
        (sizeof(*(serialized_request.data())) == sizeof(char)) && (sizeof(*(serialized_reply.data())) == sizeof(char)),
        "Byte pointer type used by concord::kvbc::IClient interface is incompatible with byte pointer type used by "
        "CMF.");
    auto res = client_.invokeCommandSynch(reinterpret_cast<char*>(serialized_request.data()),
                                          serialized_request.size(),
                                          readOnly,
                                          seconds(0),
                                          expectedReplySize,
                                          reinterpret_cast<char*>(serialized_reply.data()),
                                          &actualReplySize);
    ConcordAssert(res.isOK());

    if (isReplyCorrect(request, expectedReply, serialized_reply, expectedReplySize, actualReplySize)) ops++;
  }
  sleep(1);
  LOG_INFO(logger_, "\n*** Test completed. " << ops << " messages have been handled.");
  client_.stop();
}

bool BasicRandomTestsRunner::isReplyCorrect(const SKVBCRequest& request,
                                            const SKVBCReply& expectedReply,
                                            const vector<uint8_t>& serialized_reply,
                                            size_t expectedReplySize,
                                            uint32_t actualReplySize) {
  if (actualReplySize != expectedReplySize) {
    LOG_ERROR(logger_, "*** Test failed: actual reply size != expected");
    ConcordAssert(0);
  }
  SKVBCReply actual_reply;
  deserialize(serialized_reply, actual_reply);
  if (holds_alternative<SKVBCWriteRequest>(request.request) || holds_alternative<SKVBCReadRequest>(request.request) ||
      holds_alternative<SKVBCGetLastBlockRequest>(request.request)) {
    if (expectedReply == actual_reply) {
      return true;
    }
  }

  LOG_ERROR(logger_, "*** Test failed: actual reply != expected.");
  ConcordAssert(0);
  return false;
}

}  // namespace BasicRandomTests
