// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these sub-components is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "RequestHandler.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <cstdio>
#include <utility>

namespace bftEngine {

std::shared_ptr<IRequestsHandler> IRequestsHandler::createRequestsHandler(
    std::shared_ptr<IRequestsHandler> userReqHandler,
    const std::shared_ptr<concord::cron::CronTableRegistry> &cronTableRegistry) {
  auto reqHandler = new bftEngine::RequestHandler();
  reqHandler->setUserRequestHandler(userReqHandler);
  reqHandler->setCronTableRegistry(cronTableRegistry);
  return std::shared_ptr<IRequestsHandler>(reqHandler);
}

}  // namespace bftEngine
