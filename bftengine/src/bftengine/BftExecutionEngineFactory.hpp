// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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
#include "BftExecutionEngineBase.hpp"

namespace bftEngine::impl {
class BftExecutionEngineFactory {
 public:
  enum TYPE { SYNC, ASYNC, SKIP };
  static std::unique_ptr<BftExecutionEngineBase> create(
      std::shared_ptr<IRequestsHandler> requests_handler,
      std::shared_ptr<ClientsManager> client_manager,
      std::shared_ptr<ReplicasInfo> reps_info,
      std::shared_ptr<PersistentStorage> ps,
      std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager);
};
}  // namespace bftEngine::impl