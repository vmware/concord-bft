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
using concord::client::concordclient::EventVariant;
using concord::client::concordclient::UpdateNotFound;
using concord::client::concordclient::OutOfRangeSubscriptionRequest;
using concord::client::concordclient::SubscriptionExists;
using concord::client::concordclient::InternalError;

using namespace std::chrono_literals;

namespace cc = concord::client::concordclient;

namespace concord::client::clientservice {

namespace eventservice {

void EventServiceCallData::proceed() {
  if (state_ == FINISH) {
    delete this;
    return;
  }

  try {
    if (state_ == CREATE) {
      state_ = SUBSCRIBE;
      // Put us (this) on the completion queue if the stream was cancelled
      ctx_.AsyncNotifyWhenDone(this);
      // Request to handle an incoming `Send` RPC -> will put an event on the cq if ready
      service_->RequestSubscribe(&ctx_, &request_, &stream_, cq_, cq_, this);

    } else if (state_ == SUBSCRIBE) {
      if (ctx_.IsCancelled()) {
        populateResult(grpc::Status(grpc::StatusCode::CANCELLED, "CANCELLED"));
        return;
      }
      state_ = READ_FROM_QUEUE;
      // We are handling an incoming `Subscribe` right now, let's make sure we handle the next one too
      new eventservice::EventServiceCallData(service_, cq_, client_, aggregator_);
      // Forward request to concord client (non-blocking)
      subscribeToConcordClient();
      // The subscription started and the queue will be filled.
      // Now, we need to transition to the next state by putting an event on the completion queue.
      alarm_.Set(cq_, gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME), this);

    } else if (state_ == READ_FROM_QUEUE) {
      if (ctx_.IsCancelled()) {
        populateResult(grpc::Status(grpc::StatusCode::CANCELLED, "CANCELLED"));
        return;
      }
      // Read from the update queue and write results to stream, note:
      // * Writing will put a signal on the completion queue and we have to wait for it before we continue writing
      // * state_ can change inside readFromQueue based on the update
      readFromQueueAndWrite();

    } else if (state_ == PROCESS_RESULT) {
      state_ = FINISH;
      // Once the stream is done, an event will be put on the cq for cleanup
      stream_.Finish(return_status_, this);
    } else {
      // Unreachable - all states are handled above
      ConcordAssert(false);
    }
  } catch (std::exception& e) {
    LOG_ERROR(logger_, "Unexpected exception: " << e.what());
    state_ = FINISH;
    auto status = grpc::Status(grpc::StatusCode::INTERNAL, "Unexpected exception occured");
    stream_.Finish(status, this);
  }
}

void EventServiceCallData::populateResult(grpc::Status status) {
  // Push an event onto the completion queue so that PROCESS_CALLBACK_RESULT is handled
  client_->unsubscribe();
  state_ = PROCESS_RESULT;
  return_status_ = std::move(status);
  ConcordAssertNE(cq_, nullptr);
  alarm_.Set(cq_, gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME), this);
}

void EventServiceCallData::subscribeToConcordClient() {
  cc::SubscribeRequest request;
  if (request_.has_event_groups()) {
    cc::EventGroupRequest eg_request;
    eg_request.event_group_id = request_.event_groups().event_group_id();
    request.request = eg_request;
  } else if (request_.has_events()) {
    cc::LegacyEventRequest ev_request;
    ev_request.block_id = request_.events().block_id();
    request.request = ev_request;
  } else {
    // No other request type is possible; the gRPC server should catch this.
    ConcordAssert(false);
  }
  auto span = opentracing::Tracer::Global()->StartSpan("subscribe", {});
  client_->subscribe(request, queue_, span);
}

void EventServiceCallData::readFromQueueAndWrite() {
  // TODO: Return UNAVAILABLE as documented in event.proto if ConcordClient is unhealthy
  auto status = grpc::Status::OK;
  std::chrono::steady_clock::time_point start_aggregator_timer = std::chrono::steady_clock::now();

  std::unique_ptr<EventVariant> update;
  try {
    // We need to check if the client cancelled the subscription. Therefore, we cannot block via pop().
    update = queue_->popTill(1ms);
  } catch (const UpdateNotFound& e) {
    status = grpc::Status(grpc::StatusCode::NOT_FOUND, e.what());
  } catch (const OutOfRangeSubscriptionRequest& e) {
    status = grpc::Status(grpc::StatusCode::OUT_OF_RANGE, e.what());
  } catch (const InternalError& e) {
    status = grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  } catch (const SubscriptionExists& e) {
    status = grpc::Status(grpc::StatusCode::ALREADY_EXISTS, e.what());
  } catch (...) {
    status = grpc::Status(grpc::StatusCode::UNKNOWN, "Unknown error");
  }

  if (not status.ok()) {
    this->populateResult(status);
    return;
  }

  // If there is no update available then let the current gRPC server thread go back to the completion queue
  if (not update) return;

  std::chrono::steady_clock::time_point start_processing = std::chrono::steady_clock::now(), end_processing;

  SubscribeResponse response;
  if (std::holds_alternative<cc::EventGroup>(*update)) {
    auto& event_group_in = std::get<cc::EventGroup>(*update);
    EventGroup proto_event_group;
    proto_event_group.set_id(event_group_in.id);
    for (auto& event : event_group_in.events) {
      proto_event_group.add_events(std::move(event));
    }
    *proto_event_group.mutable_record_time() = event_group_in.record_time;
    *proto_event_group.mutable_trace_context() = {event_group_in.trace_context.begin(),
                                                  event_group_in.trace_context.end()};

    *response.mutable_event_group() = proto_event_group;
    std::chrono::steady_clock::time_point start_write = std::chrono::steady_clock::now();
    this->stream_.Write(response, this);
    this->metrics_.total_num_writes++;
    // update write duration metric
    end_processing = std::chrono::steady_clock::now();
    auto duration_write = std::chrono::duration_cast<std::chrono::microseconds>(end_processing - start_write);
    this->metrics_.write_dur.Get().Set(duration_write.count());
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
    std::chrono::steady_clock::time_point start_write = std::chrono::steady_clock::now();
    this->stream_.Write(response, this);
    this->metrics_.total_num_writes++;
    // update write duration metric
    end_processing = std::chrono::steady_clock::now();
    auto duration_write = std::chrono::duration_cast<std::chrono::microseconds>(end_processing - start_write);
    this->metrics_.write_dur.Get().Set(duration_write.count());
  } else {
    LOG_ERROR(logger_, "Got unexpected update type from TRC. This should never happen!");
    ConcordAssert(false);
  }
  // update processing duration metric
  auto update_processing_dur = std::chrono::duration_cast<std::chrono::microseconds>(end_processing - start_processing);
  this->metrics_.update_processing_dur.Get().Set(update_processing_dur.count());

  // update metrics aggregator every second
  auto metrics_aggregator_dur =
      std::chrono::duration_cast<std::chrono::seconds>(end_processing - start_aggregator_timer);
  if (metrics_aggregator_dur >= std::chrono::seconds(1)) {
    metrics_.updateAggregator();
    start_aggregator_timer = std::chrono::steady_clock::now();
  }
}

}  // namespace eventservice

}  // namespace concord::client::clientservice
