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

#include <chrono>
#include <grpcpp/grpcpp.h>

#include "client/clientservice/call_data.hpp"

using namespace std::literals::chrono_literals;

namespace concord::client::clientservice {

void ClientService::start(const std::string& addr, unsigned num_async_threads, uint64_t max_receive_msg_size) {
  grpc::EnableDefaultHealthCheckService(true);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.SetMaxReceiveMessageSize(max_receive_msg_size);

  // Register synchronous services
  builder.RegisterService(state_snapshot_service_.get());

  // Register asynchronous services
  builder.RegisterService(&event_service_);
  builder.RegisterService(&request_service_);

  for (unsigned i = 0; i < num_async_threads; ++i) {
    cqs_.emplace_back(builder.AddCompletionQueue());
  }

  clientservice_server_ = std::unique_ptr<grpc::Server>(builder.BuildAndStart());
  // clientservice_server_ now points to a running gRPC server which is ready to process calls

  // From the "C++ Performance Notes" in the gRPC documentation:
  // "Right now, the best performance trade-off is having numcpu's threads and one completion queue per thread."
  for (unsigned i = 0; i < num_async_threads; ++i) {
    server_threads_.emplace_back(std::thread([this, i] { this->handleRpcs(i); }));
  }

  auto health = clientservice_server_->GetHealthCheckService();
  health->SetServingStatus(kRequestService, true);
  health->SetServingStatus(kEventService, true);
}

void ClientService::wait() {
  ConcordAssertNE(clientservice_server_, nullptr);
  clientservice_server_->Wait();
}

void ClientService::shutdown() {
  if (clientservice_server_) {
    LOG_INFO(logger_, "Shutting down clientservice");
    clientservice_server_->Shutdown(std::chrono::system_clock::now() + 1s);

    LOG_INFO(logger_, "Shutting down and emptying completion queues");
    std::for_each(cqs_.begin(), cqs_.end(), [](auto& cq) { cq->Shutdown(); });
    for (auto& cq : cqs_) {
      void* tag;
      bool ok;
      while (cq->Next(&tag, &ok))
        ;
    }

    LOG_INFO(logger_, "Waiting for async gRPC threads to return");
    std::for_each(server_threads_.begin(), server_threads_.end(), [](auto& t) { t.join(); });
  } else {
    LOG_INFO(logger_, "Clientservice is not running.");
  }
}

void ClientService::handleRpcs(unsigned thread_idx) {
  // Note: Memory is freed in `proceed`
  new requestservice::RequestServiceCallData(&request_service_, cqs_[thread_idx].get(), client_);
  new eventservice::EventServiceCallData(
      &event_service_, cqs_[thread_idx].get(), client_, aggregator_, eventservice_max_batch_size_);

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
    // We expect a successful event or an alarm to be expired - `ok` is true in either case.
    // If the alarm was cancelled `ok` will be false - we don't cancel alarms.
    // An unsuccessful event either means GRPC_QUEUE_SHUTDOWN or GRPC_QUEUE_TIMEOUT.
    // The former will be signaled on the following Next() call and handled.
    // The latter means that there is no event present.
    if (not ok) {
      continue;
    }
    auto function_ptr = static_cast<CallData::Tag_t*>(tag);
    if (*function_ptr) (*function_ptr)();
  }
}

}  // namespace concord::client::clientservice
