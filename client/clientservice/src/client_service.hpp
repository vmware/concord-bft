// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <memory>
#include <string>

#include "event_service.hpp"
#include "request_service.hpp"
#include "Logger.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

class ClientService {
 public:
  ClientService(std::unique_ptr<concord::client::concordclient::ConcordClient> client)
      : logger_(logging::getLogger("concord.client.clientservice")),
        client_(std::move(client)),
        event_service_(std::make_unique<EventServiceImpl>(client_)),
        request_service_(std::make_unique<RequestServiceImpl>(client_)){};

  void start(const std::string& addr);

  const std::string kRequestService{"vmware.concord.client.v1.RequestService"};
  const std::string kEventService{"vmware.concord.client.v1.EventService"};

 private:
  logging::Logger logger_;
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
  std::unique_ptr<EventServiceImpl> event_service_;
  std::unique_ptr<RequestServiceImpl> request_service_;
};

}  // namespace concord::client::clientservice
