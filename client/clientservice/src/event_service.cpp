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
#include <google/protobuf/util/time_util.h>
#include <opentracing/tracer.h>

#include "event_service.hpp"
#include "client/concordclient/concord_client.hpp"

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerWriter;

using google::protobuf::util::TimeUtil;

using vmware::concord::client::v1::StreamEventGroupsRequest;
using vmware::concord::client::v1::EventGroup;

namespace cc = concord::client::concordclient;

namespace concord::client::clientservice {

Status EventServiceImpl::StreamEventGroups(ServerContext* context,
                                           const StreamEventGroupsRequest* proto_request,
                                           ServerWriter<EventGroup>* stream) {
  LOG_INFO(logger_, "EventServiceImpl::StreamEventGroups called");

  cc::SubscribeRequest request;
  request.event_group_id = proto_request->event_group_id();

  auto callback = [this, stream](cc::SubscribeResult event) {
    if (not std::holds_alternative<cc::EventGroup>(event)) {
      LOG_INFO(logger_, "Subscribe returned error");
      return;
    }
    auto eg = std::get<cc::EventGroup>(event);
    EventGroup event_group;
    event_group.set_id(eg.id);
    for (cc::Event e : eg.events) {
      *event_group.add_events() = std::string(e.begin(), e.end());
    }
    *event_group.mutable_record_time() = TimeUtil::MicrosecondsToTimestamp(eg.record_time.count());
    *event_group.mutable_trace_context() = {eg.trace_context.begin(), eg.trace_context.end()};

    stream->Write(event_group);
  };

  auto span = opentracing::Tracer::Global()->StartSpan("stream_event_groups", {});
  client_->subscribe(request, span, callback);

  // TODO: Consider all gRPC return error codes as described in concord_client.proto
  while (!context->IsCancelled())
    ;

  client_->unsubscribe();
  return grpc::Status::OK;
}

}  // namespace concord::client::clientservice
