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
#include <opentracing/tracer.h>
#include <thread>

#include "client/clientservice/event_service.hpp"
#include "client/concordclient/concord_client.hpp"

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerWriter;

using vmware::concord::client::event::v1::SubscribeRequest;
using vmware::concord::client::event::v1::SubscribeResponse;
using vmware::concord::client::event::v1::Event;
using vmware::concord::client::event::v1::EventGroup;
using vmware::concord::client::event::v1::Events;

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

  auto span = opentracing::Tracer::Global()->StartSpan("subscribe", {});
  std::shared_ptr<cc::UpdateQueue> update_queue = std::make_shared<cc::BasicUpdateQueue>();
  try {
    client_->subscribe(request, update_queue, span);
  } catch (cc::ConcordClient::SubscriptionExists& e) {
    return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, e.what());
  } catch (cc::ConcordClient::UpdateNotFound& e) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, e.what());
  } catch (cc::ConcordClient::OutOfRangeSubscriptionRequest& e) {
    return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, e.what());
  } catch (cc::ConcordClient::InternalError& e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }

  // TODO: Consider all gRPC return error codes as described in event.proto
  while (!context->IsCancelled()) {
    SubscribeResponse response;
    auto update = update_queue->tryPop();
    if (not update) {
      // We need to check if the client cancelled the subscription.
      // Therefore, we cannot block via pop(). Can we do bettern than sleep?
      std::this_thread::sleep_for(10ms);
      continue;
    }

    if (std::holds_alternative<cc::EventGroup>(*update)) {
      auto& event_group_in = std::get<cc::EventGroup>(*update);
      EventGroup proto_event_group;
      proto_event_group.set_id(event_group_in.id);
      for (const auto& event : event_group_in.events) {
        *proto_event_group.add_events() = std::string(event.begin(), event.end());
      }
      *proto_event_group.mutable_record_time() = event_group_in.record_time;
      *proto_event_group.mutable_trace_context() = {event_group_in.trace_context.begin(),
                                                    event_group_in.trace_context.end()};

      *response.mutable_event_group() = proto_event_group;
      stream->Write(response);

    } else if (std::holds_alternative<cc::Update>(*update)) {
      auto& legacy_event_in = std::get<cc::Update>(*update);
      Events proto_events;
      proto_events.set_block_id(legacy_event_in.block_id);
      for (auto& [key, value] : legacy_event_in.kv_pairs) {
        Event proto_event;
        *proto_event.mutable_event_key() = std::move(key);
        *proto_event.mutable_event_value() = std::move(value);
        *proto_events.add_events() = proto_event;
      }
      proto_events.set_correlation_id(legacy_event_in.correlation_id_);
      // TODO: Set trace context

      *response.mutable_events() = proto_events;
      stream->Write(response);

    } else {
      LOG_ERROR(logger_, "Got unexpected update type from TRC. This should never happen!");
    }
  }

  client_->unsubscribe();
  return grpc::Status::OK;
}

}  // namespace concord::client::clientservice
