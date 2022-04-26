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

#include <grpcpp/grpcpp.h>
#include <event.grpc.pb.h>
#include <memory>

#include "Logger.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

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

class EventServiceImpl final : public vmware::concord::client::event::v1::EventService::Service {
 public:
  EventServiceImpl(std::shared_ptr<concord::client::concordclient::ConcordClient> client,
                   std::shared_ptr<concordMetrics::Aggregator> aggregator)
      : logger_(logging::getLogger("concord.client.clientservice.event")), client_(client), metrics_() {
    metrics_.setAggregator(aggregator);
  };
  grpc::Status Subscribe(grpc::ServerContext* context,
                         const vmware::concord::client::event::v1::SubscribeRequest* request,
                         grpc::ServerWriter<vmware::concord::client::event::v1::SubscribeResponse>* stream) override;

 private:
  logging::Logger logger_;
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
  EventServiceMetrics metrics_;
};

}  // namespace concord::client::clientservice
