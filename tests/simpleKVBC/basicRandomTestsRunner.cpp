// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
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

#ifndef _WIN32
#include <unistd.h>
#endif

using std::chrono::seconds;

using concord::kvbc::IClient;

namespace BasicRandomTests {

BasicRandomTestsRunner::BasicRandomTestsRunner(logging::Logger &logger, IClient &client, size_t numOfOperations)
    : logger_(logger), client_(client), numOfOperations_(numOfOperations) {
  // We have to start the client here, since construction of the TestsBuilder
  // uses the client.
  ConcordAssert(!client_.isRunning());
  client_.start();
  testsBuilder_ = new TestsBuilder(logger_, client);
}

void BasicRandomTestsRunner::sleep(int ops) {
#ifndef _WIN32
  if (ops % 100 == 0) usleep(100 * 1000);
#endif
}

void BasicRandomTestsRunner::run() {
  testsBuilder_->createRandomTest(numOfOperations_, 1111);

  RequestsList requests = testsBuilder_->getRequests();
  RepliesList expectedReplies = testsBuilder_->getReplies();
  ConcordAssert(requests.size() == expectedReplies.size());

  int ops = 0;
  while (!requests.empty()) {
    sleep(ops);
    SimpleRequest *request = requests.front();
    SimpleReply *expectedReply = expectedReplies.front();
    requests.pop_front();
    expectedReplies.pop_front();

    bool readOnly = (request->type != COND_WRITE);
    size_t requestSize = TestsBuilder::sizeOfRequest(request);
    size_t expectedReplySize = TestsBuilder::sizeOfReply(expectedReply);
    uint32_t actualReplySize = 0;

    std::vector<char> reply(expectedReplySize);

    auto res = client_.invokeCommandSynch(
        (char *)request, requestSize, readOnly, seconds(0), expectedReplySize, reply.data(), &actualReplySize);
    ConcordAssert(res.isOK());

    if (isReplyCorrect(request->type, expectedReply, reply.data(), expectedReplySize, actualReplySize)) ops++;
  }
  sleep(1);
  LOG_INFO(logger_, "\n*** Test completed. " << ops << " messages have been handled.");
  client_.stop();
}

bool BasicRandomTestsRunner::isReplyCorrect(RequestType requestType,
                                            const SimpleReply *expectedReply,
                                            const char *reply,
                                            size_t expectedReplySize,
                                            uint32_t actualReplySize) {
  if (actualReplySize != expectedReplySize) {
    LOG_ERROR(logger_, "*** Test failed: actual reply size != expected");
    ConcordAssert(0);
  }
  std::ostringstream error;
  switch (requestType) {
    case COND_WRITE:
      if (((SimpleReply_ConditionalWrite *)expectedReply)->isEquiv(*(SimpleReply_ConditionalWrite *)reply, error))
        return true;
      break;
    case READ:
      if (((SimpleReply_Read *)expectedReply)->isEquiv(*(SimpleReply_Read *)reply, error)) return true;
      break;
    case GET_LAST_BLOCK:
      if (((SimpleReply_GetLastBlock *)expectedReply)->isEquiv(*(SimpleReply_GetLastBlock *)reply, error)) return true;
      break;
    default:;
  }

  LOG_ERROR(logger_, "*** Test failed: actual reply != expected; error: " << error.str());
  ConcordAssert(0);
  return false;
}

}  // namespace BasicRandomTests
