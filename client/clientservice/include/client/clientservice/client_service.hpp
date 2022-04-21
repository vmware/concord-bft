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

#pragma once

#include <memory>
#include <string>

#include "event_service.hpp"
#include "request_service.hpp"
#include "state_snapshot_service.hpp"
#include "Logger.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

class ClientService {
 public:
  ClientService(std::unique_ptr<concord::client::concordclient::ConcordClient> client,
                std::shared_ptr<concordMetrics::Aggregator> aggregator)
      : logger_(logging::getLogger("concord.client.clientservice")),
        client_(std::move(client)),
        event_service_(std::make_unique<EventServiceImpl>(client_, aggregator)),
        state_snapshot_service_(std::make_unique<StateSnapshotServiceImpl>(client_)){};

  void start(const std::string& addr, unsigned num_async_threads, uint64_t max_receive_msg_size);

  // Blocks waiting for all work to complete
  // Assumes that the caller has started the clientservice prior to this call
  void wait();

  // This method does the following -
  // 1. Shuts down the clientservice gRPC server without a deadline and with forced cancellation
  // 2. Shuts down all the completion queues associated with the server, and drains them using repeated next() calls
  // 3. Waits for async gRPC server threads to return
  // The above order is important. Reference: https://grpc.github.io/grpc/cpp/classgrpc_1_1_server_interface.html
  void shutdown();

  const std::string kRequestService{"vmware.concord.client.request.v1.RequestService"};
  const std::string kEventService{"vmware.concord.client.event.v1.EventService"};
  const std::string kStateSnapshotService{"vmware.concord.client.statesnapshot.v1.StateSnapshotService"};

 private:
  // Handler for asynchronous services
  void handleRpcs(unsigned thread_idx);

  logging::Logger logger_;
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;

  // Synchronous services
  std::unique_ptr<EventServiceImpl> event_service_;
  std::unique_ptr<StateSnapshotServiceImpl> state_snapshot_service_;

  // Asynchronous services
  vmware::concord::client::request::v1::RequestService::AsyncService request_service_;

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::thread> server_threads_;
  std::unique_ptr<grpc::Server> clientservice_server_;
};

}  // namespace concord::client::clientservice
