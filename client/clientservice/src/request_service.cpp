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
#include <opentracing/tracer.h>

#include "client/clientservice/request_service.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

namespace requestservice {

void RequestServiceCallData::proceed() {
  if (state_ == FINISH) {
    delete this;
    return;
  }

  try {
    if (state_ == CREATE) {
      state_ = SEND_TO_CONCORDCLIENT;
      // Request to handle an incoming `Send` RPC -> will put an event on the cq if ready
      service_->RequestSend(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (state_ == SEND_TO_CONCORDCLIENT) {
      // We are handling an incoming `Send` right now, let's make sure we handle the next one too
      new requestservice::RequestServiceCallData(service_, cq_, client_);
      // Forward request to concord client (non-blocking)
      sendToConcordClient();
      // Note: The next state transition happens in `populateResult`
    } else if (state_ == PROCESS_CALLBACK_RESULT) {
      state_ = FINISH;
      // Once the response is sent, an event will be put on the cq for cleanup
      if (return_status_.ok()) {
        responder_.Finish(response_, return_status_, this);
      } else {
        responder_.FinishWithError(return_status_, this);
      }
    } else {
      // Unreachable - all states are handled above
      ConcordAssert(false);
    }
  } catch (std::exception& e) {
    LOG_ERROR(logger_, "Unexpected exception (cid=" << request_.correlation_id() << "): " << e.what());
    state_ = FINISH;
    auto status = grpc::Status(grpc::StatusCode::INTERNAL, "Unexpected exception occured");
    responder_.FinishWithError(status, this);
  }
}

void RequestServiceCallData::populateResult(grpc::Status status) {
  // Push an event onto the completion queue so that PROCESS_CALLBACK_RESULT is handled
  state_ = PROCESS_CALLBACK_RESULT;
  return_status_ = std::move(status);
  ConcordAssertNE(cq_, nullptr);
  callback_alarm_.Set(cq_, gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME), this);
}

void RequestServiceCallData::sendToConcordClient() {
  bool is_any_request_type = false;
  bft::client::Msg msg;
  if (request_.has_typed_request()) {
    google::protobuf::Any app_request = request_.typed_request();
    size_t request_size = app_request.ByteSizeLong();
    std::string request(request_size, '\0');
    app_request.SerializeToArray(request.data(), request_size);
    msg = bft::client::Msg(request.begin(), request.end());
    is_any_request_type = true;
  } else {
    msg = bft::client::Msg(request_.raw_request().begin(), request_.raw_request().end());
  }

  auto seconds = std::chrono::seconds{request_.timeout().seconds()};
  auto nanos = std::chrono::nanoseconds{request_.timeout().nanos()};
  auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(seconds + nanos);

  bft::client::RequestConfig req_config;
  req_config.pre_execute = request_.pre_execute();
  req_config.timeout = timeout;
  req_config.correlation_id = request_.correlation_id();

  auto callback = [this, req_config, is_any_request_type](concord::client::concordclient::SendResult&& send_result) {
    grpc::Status status;
    auto logger = logging::getLogger("concord.client.clientservice.request.callback");
    if (not std::holds_alternative<bft::client::Reply>(send_result)) {
      switch (std::get<uint32_t>(send_result)) {
        case (static_cast<uint32_t>(bftEngine::OperationResult::INVALID_REQUEST)):
          LOG_INFO(logger, "Request failed with INVALID_ARGUMENT error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid argument");
          break;
        case (static_cast<uint32_t>(bftEngine::OperationResult::NOT_READY)):
          LOG_INFO(logger, "Request failed with NOT_READY error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "No clients connected to the replicas");
          break;
        case (static_cast<uint32_t>(bftEngine::OperationResult::TIMEOUT)):
          LOG_INFO(logger, "Request failed with TIMEOUT error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Timeout");
          break;
        case (static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_TOO_LARGE)):
          LOG_INFO(logger, "Request failed with EXEC_DATA_TOO_LARGE error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::INTERNAL, "Execution data too large");
          break;
        case (static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_EMPTY)):
          LOG_INFO(logger, "Request failed with EXEC_DATA_EMPTY error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::INTERNAL, "Execution data is empty");
          break;
        case (static_cast<uint32_t>(bftEngine::OperationResult::CONFLICT_DETECTED)):
          LOG_INFO(logger, "Request failed with CONFLICT_DETECTED error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::ABORTED, "Aborted");
          break;
        case (static_cast<uint32_t>(bftEngine::OperationResult::OVERLOADED)):
          LOG_INFO(logger, "Request failed with OVERLOADED error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "All clients occupied");
          break;
        default:
          LOG_INFO(logger, "Request failed with INTERNAL error for cid=" << req_config.correlation_id);
          status = grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
          break;
      }
      this->populateResult(status);
      return;
    }
    auto reply = std::get<bft::client::Reply>(send_result);
    // We need to copy because there is no implicit conversion between vector<uint8> and std::string
    std::string data(reply.matched_data.begin(), reply.matched_data.end());

    // Check if the application response is of Any Type then set it to Any response.
    if (is_any_request_type) {
      google::protobuf::Any* app_response = this->response_.mutable_typed_response();
      if (!app_response->ParseFromArray(data.c_str(), data.size())) {
        status = grpc::Status(grpc::StatusCode::INTERNAL, "Internal error in parsing typed response");
        this->populateResult(status);
        return;
      }
    } else {
      this->response_.set_raw_response(std::move(data));
    }

    this->populateResult(grpc::Status::OK);
  };

  if (request_.read_only()) {
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
}
}  // namespace requestservice

}  // namespace concord::client::clientservice
