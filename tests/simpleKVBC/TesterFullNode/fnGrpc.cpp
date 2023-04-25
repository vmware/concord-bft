// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "fnGrpc.hpp"

constexpr size_t OUT_BUFFER_SIZE = 1024000;

void RequestServiceImpl::sendToInternalHandler(const bft::client::RequestConfig req_config,
                                               const bft::client::Msg&& msg,
                                               const bftEngine::MsgFlag flags,
                                               vmware::concord::client::request::v1::Response* response) {
  // send to internal command handler

  concordUtils::SpanWrapper span_wrp_;

  bftEngine::IRequestsHandler::ExecutionRequestsQueue requests;
  std::string req(msg.begin(), msg.end());

  reply_buffer_ = std::string(OUT_BUFFER_SIZE, 0);
  requests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{6,
                                                                   1,
                                                                   req_config.correlation_id,
                                                                   flags,
                                                                   static_cast<uint32_t>(req.size()),
                                                                   req.c_str(),
                                                                   "",
                                                                   static_cast<uint32_t>(reply_buffer_.size()),
                                                                   reply_buffer_.data()});

  cmdHandler_->execute(requests, std::nullopt, "1", span_wrp_);

  bftEngine::IRequestsHandler::ExecutionRequest& single_request = requests.back();

  LOG_INFO(GL, "Request Executed ");
  LOG_INFO(GL, "Status " << single_request.outExecutionStatus);
  LOG_INFO(GL, "Reply Size " << single_request.outActualReplySize);
  LOG_INFO(GL, "Block ID " << single_request.blockId);
  LOG_INFO(GL, "Reply String " << single_request.outReply);

  std::string ex_response(single_request.outReply, single_request.outActualReplySize);
  response->set_raw_response(std::move(ex_response));
}

grpc::Status RequestServiceImpl::Send(grpc::ServerContext* context,
                                      const vmware::concord::client::request::v1::Request* request,
                                      vmware::concord::client::request::v1::Response* response) {
  /*auto tracer = opentracing::MakeNoopTracer();
  auto raw_span = tracer->StartSpan("dummy");
  auto span_wrapper_ = concordUtils::startChildSpanFromContext(raw_span->context(), "dummy_1");*/

  LOG_INFO(GL, "Received Request");

  // send to bft client
  bft::client::Msg msg;
  msg = bft::client::Msg(request->raw_request().begin(), request->raw_request().end());

  auto seconds = std::chrono::seconds{request->timeout().seconds()};
  auto nanos = std::chrono::nanoseconds{request->timeout().nanos()};
  auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(seconds + nanos);

  bft::client::RequestConfig req_config;
  req_config.pre_execute = request->pre_execute();
  req_config.timeout = timeout;
  req_config.correlation_id = request->correlation_id();
  req_config.primary_only = request->primary_only();

  if (request->read_only()) {
    if (fn_execute_enable_) {
      sendToInternalHandler(req_config, std::move(msg), bftEngine::MsgFlag::READ_ONLY_FLAG, response);
    } else {
      bft::client::ReadConfig config;
      config.request = req_config;

      LOG_INFO(GL, "Sending Read Request to Validator Network");

      bft::client::Reply reply;
      reply = bft_client_->send(config, std::move(msg));

      LOG_INFO(GL, "Received Read Reply" << reply.result);

      std::string data(reply.matched_data.begin(), reply.matched_data.end());

      response->set_raw_response(std::move(data));
    }

  } else {
    if (fn_execute_enable_) {
      sendToInternalHandler(req_config, std::move(msg), bftEngine::MsgFlag::EMPTY_FLAGS, response);
    } else {
      bft::client::WriteConfig config;
      config.request = req_config;

      LOG_INFO(GL, "Sending Write Request to Validator Network");

      bft::client::Reply reply;
      reply = bft_client_->send(config, std::move(msg));

      LOG_INFO(GL, "Received Write Reply" << reply.result);

      std::string data(reply.matched_data.begin(), reply.matched_data.end());

      response->set_raw_response(std::move(data));
    }
  }

  auto status = grpc::Status(grpc::StatusCode::OK, "Received in Tester FullNode");

  return status;
}