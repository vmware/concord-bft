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

#include "client/clientservice/client_service.hpp"

#include <grpcpp/grpcpp.h>

namespace concord::client::clientservice {

void ClientService::start(const std::string& addr, int num_async_threads, uint64_t max_receive_msg_size) {
  grpc::EnableDefaultHealthCheckService(true);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.SetMaxReceiveMessageSize(max_receive_msg_size);

  // Register synchronous services
  builder.RegisterService(event_service_.get());

  // Register asynchronous services
  builder.RegisterService(&request_service_);

  for (int i = 0; i < num_async_threads; ++i) {
    cqs_.emplace_back(builder.AddCompletionQueue());
  }

  auto clientservice_server = std::unique_ptr<grpc::Server>(builder.BuildAndStart());

  // From the "C++ Performance Notes" in the gRPC documentation:
  // "Right now, the best performance trade-off is having numcpu's threads and one completion queue per thread."
  for (int i = 0; i < num_async_threads; ++i) {
    server_threads_.emplace_back(std::thread([this, i] { this->handleRpcs(i); }));
  }

  auto health = clientservice_server->GetHealthCheckService();
  health->SetServingStatus(kRequestService, true);
  health->SetServingStatus(kEventService, true);

  clientservice_server->Wait();

  LOG_INFO(logger_, "Wait for async gRPC threads to finish");
  for (auto& t : server_threads_) {
    t.join();
  }
}

void ClientService::handleRpcs(int thread_idx) {
  // Note: Memory is freed in `proceed`
  new requestservice::RequestServiceCallData(&request_service_, cqs_[thread_idx].get(), client_);

  void* tag;  // uniquely identifies a request.
  bool ok = false;
  while (true) {
    // Block waiting to read the next event from the completion queue. The
    // event is uniquely identified by its tag, which in this case is the
    // memory address of a RequestServiceCallData instance (see `proceed`).
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or cqs_ is shutting down.
    if (not cqs_[thread_idx]->Next(&tag, &ok)) {
      // The queue is drained and shutdown, no need to stay around.
      LOG_INFO(logger_, "Completion queue drained and shutdown, stop processing.");
      return;
    }
    // We expect a successful event or an alarm to be expired (alarm cancelled is false)
    if (not ok) {
      LOG_WARN(logger_, "Got unsuccessful event from completion queue with tag " << tag);
    }
    static_cast<requestservice::RequestServiceCallData*>(tag)->proceed();
  }
}

}  // namespace concord::client::clientservice
