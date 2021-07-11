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

#include <grpcpp/grpcpp.h>
#include <concord_client.grpc.pb.h>
#include <memory>

#include "Logger.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

class EventServiceImpl final : public vmware::concord::client::v1::EventService::Service {
 public:
  EventServiceImpl(std::shared_ptr<concord::client::concordclient::ConcordClient> client)
      : logger_(logging::getLogger("concord.client.clientservice.event")), client_(client){};
  grpc::Status StreamEventGroups(grpc::ServerContext* context,
                                 const vmware::concord::client::v1::StreamEventGroupsRequest* request,
                                 grpc::ServerWriter<vmware::concord::client::v1::EventGroup>* stream) override;

 private:
  logging::Logger logger_;
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
};

}  // namespace concord::client::clientservice
