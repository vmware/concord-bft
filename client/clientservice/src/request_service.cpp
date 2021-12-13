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
#include <future>
#include <iostream>
#include <opentracing/tracer.h>

#include "client/clientservice/request_service.hpp"
#include "client/concordclient/concord_client.hpp"

using grpc::Status;
using grpc::ServerContext;

using vmware::concord::client::request::v1::Request;
using vmware::concord::client::request::v1::Response;

namespace cc = concord::client::concordclient;

namespace concord::client::clientservice {

Status RequestServiceImpl::Send(ServerContext* context, const Request* proto_request, Response* proto_response) {
  bft::client::Msg msg(proto_request->request().begin(), proto_request->request().end());

  auto seconds = std::chrono::seconds{proto_request->timeout().seconds()};
  auto nanos = std::chrono::nanoseconds{proto_request->timeout().nanos()};
  auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(seconds + nanos);

  bft::client::RequestConfig req_config;
  req_config.pre_execute = proto_request->pre_execute();
  req_config.timeout = timeout;
  req_config.correlation_id = proto_request->correlation_id();

  std::promise<grpc::Status> status;
  auto status_future = status.get_future();

  auto callback = [&](cc::SendResult&& send_result) {
    if (not std::holds_alternative<bft::client::Reply>(send_result)) {
      LOG_INFO(logger_, "Send returned error");
      switch (std::get<int>(send_result)) {
        case (concord_client_pool::Overloaded):
          status.set_value(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "All clients occupied"));
          break;
        case (concord_client_pool::InvalidArgument):
          status.set_value(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid argument"));
          break;
        case (concord_client_pool::TimedOut):
          status.set_value(grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Timeout"));
          break;
        case (concord_client_pool::ClientUnavailable):
          status.set_value(grpc::Status(grpc::StatusCode::UNAVAILABLE, "No clients connected to the replicas"));
          break;
        default:
          status.set_value(grpc::Status(grpc::StatusCode::INTERNAL, "Internal error"));
          break;
      }
      return;
    }
    auto reply = std::get<bft::client::Reply>(send_result);
    // TODO: Can we use set_allocated_response instead of copying? (vector<uint8_t> vs string)
    proto_response->set_response({reply.matched_data.begin(), reply.matched_data.end()});
    status.set_value(grpc::Status::OK);
  };

  if (proto_request->read_only()) {
    bft::client::ReadConfig config;
    config.request = req_config;
    auto span = opentracing::Tracer::Global()->StartSpan("send_ro", {});
    std::ostringstream carrier;
    opentracing::Tracer::Global()->Inject(span->context(), carrier);
    config.request.span_context = carrier.str();
    client_->send(config, std::move(msg), callback);
  } else {
    bft::client::WriteConfig config;
    config.request = req_config;
    auto span = opentracing::Tracer::Global()->StartSpan("send", {});
    std::ostringstream carrier;
    opentracing::Tracer::Global()->Inject(span->context(), carrier);
    config.request.span_context = carrier.str();
    client_->send(config, std::move(msg), callback);
  }

  return status_future.get();
}

}  // namespace concord::client::clientservice
