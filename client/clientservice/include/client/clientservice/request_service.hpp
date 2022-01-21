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

#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <request.grpc.pb.h>
#include <memory>

#include "Logger.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

namespace requestservice {

// Every `Send` request is represented by this class.
// It follows a simple state machine `RpcState`:
// CREATE - Register to handle incoming `Send` requests
// SEND_TO_CONCORDCLIENT - forward request to concord client
// PROCESS_CALLBACK_RESULT - respond to caller
// FINISH - clean-up
class RequestServiceCallData {
 public:
  RequestServiceCallData(vmware::concord::client::request::v1::RequestService::AsyncService* service,
                         grpc::ServerCompletionQueue* cq,
                         std::shared_ptr<concord::client::concordclient::ConcordClient> client)
      : logger_(logging::getLogger("concord.client.clientservice.requestservice")),
        service_(service),
        cq_(cq),
        responder_(&ctx_),
        state_(CREATE),
        client_(client) {
    proceed();
  }

  // Walk through the state machine
  void proceed();
  // Callback invoked by concord client after request processing is done
  void populateResult(grpc::Status);
  // Forward request to concord client
  void sendToConcordClient();

 private:
  logging::Logger logger_;

  vmware::concord::client::request::v1::RequestService::AsyncService* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncResponseWriter<vmware::concord::client::request::v1::Response> responder_;

  grpc::Alarm callback_alarm_;
  grpc::Status return_status_;

  vmware::concord::client::request::v1::Request request_;
  vmware::concord::client::request::v1::Response response_;

  enum RpcState { CREATE, SEND_TO_CONCORDCLIENT, PROCESS_CALLBACK_RESULT, FINISH };
  RpcState state_;

  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
};

}  // namespace requestservice

}  // namespace concord::client::clientservice
