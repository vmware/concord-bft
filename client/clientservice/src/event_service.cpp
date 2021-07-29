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

#include <chrono>
#include <iostream>
#include <google/protobuf/util/time_util.h>
#include <opentracing/tracer.h>
#include <thread>

#include "client/clientservice/event_service.hpp"
#include "client/concordclient/concord_client.hpp"

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerWriter;

using google::protobuf::util::TimeUtil;

using vmware::concord::clientservice::v1::SubscribeRequest;
using vmware::concord::clientservice::v1::SubscribeResponse;
using vmware::concord::clientservice::v1::Event;
using vmware::concord::clientservice::v1::EventGroup;
using vmware::concord::clientservice::v1::Events;

using namespace std::chrono_literals;

namespace cc = concord::client::concordclient;

namespace concord::client::clientservice {

Status EventServiceImpl::Subscribe(ServerContext* context,
                                   const SubscribeRequest* proto_request,
                                   ServerWriter<SubscribeResponse>* stream) {
  cc::SubscribeRequest request;

  if (proto_request->has_event_groups()) {
    cc::EventGroupRequest eg_request;
    eg_request.event_group_id = proto_request->event_groups().event_group_id();
    request.request = eg_request;

  } else if (proto_request->has_events()) {
    cc::LegacyEventRequest ev_request;
    ev_request.block_id = proto_request->events().block_id();
    request.request = ev_request;

  } else {
    // No other request type is possible; the gRPC server should catch this.
    ConcordAssert(false);
  }

  auto callback = [this, stream](cc::SubscribeResult&& subscribe_result) {
    SubscribeResponse response;

    if (std::holds_alternative<cc::SubscribeError>(subscribe_result)) {
      // TODO: Map subscribe errors to gRPC errors
      LOG_ERROR(logger_, "TODO: Communicate error to gRPC");
      return;
    } else if (std::holds_alternative<cc::EventGroup>(subscribe_result)) {
      auto event_group = std::get<cc::EventGroup>(subscribe_result);
      EventGroup proto_event_group;
      proto_event_group.set_id(event_group.id);
      for (cc::Event e : event_group.events) {
        *proto_event_group.add_events() = std::string(e.begin(), e.end());
      }
      *proto_event_group.mutable_record_time() = TimeUtil::MicrosecondsToTimestamp(event_group.record_time.count());
      *proto_event_group.mutable_trace_context() = {event_group.trace_context.begin(), event_group.trace_context.end()};

      *response.mutable_event_group() = proto_event_group;

    } else if (std::holds_alternative<cc::LegacyEvent>(subscribe_result)) {
      auto result = std::get<cc::LegacyEvent>(subscribe_result);
      Events proto_events;
      proto_events.set_block_id(result.block_id);
      for (auto& [key, value] : result.events) {
        Event proto_event;
        *proto_event.mutable_event_key() = std::move(key);
        *proto_event.mutable_event_value() = std::move(value);
        *proto_events.add_events() = proto_event;
      }
      proto_events.set_correlation_id(result.correlation_id);
      // TODO: Set trace context

      *response.mutable_events() = proto_events;

    } else {
      // Should be unreachable
      LOG_ERROR(logger_, "Subscribe returned with unexpected type");
      return;
    }

    stream->Write(response);
  };

  auto span = opentracing::Tracer::Global()->StartSpan("subscribe", {});
  try {
    client_->subscribe(request, span, callback);
  } catch (cc::ConcordClient::SubscriptionExists& e) {
    return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, e.what());
  }

  // TODO: Consider all gRPC return error codes as described in clientservice.proto
  while (!context->IsCancelled()) {
    std::this_thread::sleep_for(10ms);
  }

  client_->unsubscribe();
  return grpc::Status::OK;
}

}  // namespace concord::client::clientservice
