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

#include <iostream>

#include "event_service.hpp"

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerWriter;

using vmware::concord::client::v1::StreamEventGroupsRequest;
using vmware::concord::client::v1::EventGroup;

namespace concord::client::clientservice {

Status EventServiceImpl::StreamEventGroups(ServerContext* context,
                                           const StreamEventGroupsRequest* request,
                                           ServerWriter<EventGroup>* stream) {
  LOG_INFO(logger_, "EventServiceImpl::StreamEventGroups called");
  return grpc::Status::OK;
}

}  // namespace concord::client::clientservice
