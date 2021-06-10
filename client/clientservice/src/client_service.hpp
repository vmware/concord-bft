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

namespace concord::client::clientservice {

class ClientService {
 public:
  ClientService();
  void start(const std::string& addr);

  const std::string kRequestService{"vmware.concord.client.v1.RequestService"};
  const std::string kEventService{"vmware.concord.client.v1.EventService"};

 private:
  std::unique_ptr<EventServiceImpl> event_service_;
  std::unique_ptr<RequestServiceImpl> request_service_;
};

}  // namespace concord::client::clientservice
