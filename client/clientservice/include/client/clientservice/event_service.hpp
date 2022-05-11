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
#include <event.grpc.pb.h>
#include <memory>
#include <thread>

#include "Logger.hpp"
#include "client/concordclient/concord_client.hpp"
#include "client/clientservice/call_data.hpp"

namespace concord::client::clientservice {

namespace eventservice {

// EventService metrics
struct EventServiceMetrics {
  EventServiceMetrics()
      : metrics_component_{"EventService", std::make_shared<concordMetrics::Aggregator>()},
        total_num_writes{metrics_component_.RegisterCounter("total_num_writes", 0)},
        update_processing_dur{metrics_component_.RegisterGauge("update_processing_dur", 0)},
        write_dur{metrics_component_.RegisterGauge("write_dur", 0)} {
    metrics_component_.Register();
  }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    metrics_component_.SetAggregator(aggregator);
  }

  void updateAggregator() { metrics_component_.UpdateAggregator(); }

 private:
  concordMetrics::Component metrics_component_;

 public:
  // total number of updates written to the gRPC stream
  concordMetrics::CounterHandle total_num_writes;
  // time taken to process an update (time between when an update is received by the event service from the update queue
  // until it is written to the stream)
  concordMetrics::GaugeHandle update_processing_dur;
  // time taken to write an update to the gRPC stream
  concordMetrics::GaugeHandle write_dur;
};

class EventServiceCallData final : public clientservice::CallData {
 public:
  EventServiceCallData(vmware::concord::client::event::v1::EventService::AsyncService* service,
                       grpc::ServerCompletionQueue* cq,
                       std::shared_ptr<concord::client::concordclient::ConcordClient> client,
                       std::shared_ptr<concordMetrics::Aggregator> aggregator)
      : logger_(logging::getLogger("concord.client.clientservice.eventservice")),
        service_(service),
        cq_(cq),
        stream_(&ctx_),
        state_(CREATE),
        client_(client),
        aggregator_(aggregator),
        queue_(std::make_shared<concord::client::concordclient::BasicEventUpdateQueue>()) {
    metrics_.setAggregator(aggregator_);
    proceed = [&]() { proceedImpl(); };
    done = [&]() { doneImpl(); };
    proceedImpl();
  }

  // Walk through the state machine
  void proceedImpl();
  // Invoked after subscription stream is done
  void populateResult(grpc::Status);
  // Forward request to concord client
  void subscribeToConcordClient();
  // Read from the queue and write to the gRPC stream
  void readFromQueueAndWrite();
  // The gRPC server informs us when we should cancel
  void doneImpl();

  CallData::Tag_t proceed;
  CallData::Tag_t done;

 private:
  logging::Logger logger_;

  vmware::concord::client::event::v1::EventService::AsyncService* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncWriter<vmware::concord::client::event::v1::SubscribeResponse> stream_;

  grpc::Alarm alarm_;
  grpc::Status return_status_;

  vmware::concord::client::event::v1::SubscribeRequest request_;

  enum RpcState { CREATE, SUBSCRIBE, READ_FROM_QUEUE, PROCESS_RESULT, FINISH };
  RpcState state_;

  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  EventServiceMetrics metrics_;

  std::shared_ptr<concord::client::concordclient::EventUpdateQueue> queue_;

  std::atomic<bool> delete_me_{false};
  std::uint8_t num_pending_writes_{0};
};

}  // namespace eventservice
}  // namespace concord::client::clientservice
