// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "Replica.hpp"
#include "ReplicaConfig.hpp"
#include "TimeServiceResPageClient.hpp"
#include "assertUtils.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "serialize.hpp"
#include "Metrics.hpp"
#include <cstdlib>
#include <limits>
#include <memory>

namespace bftEngine::impl {

template <typename ClockT = std::chrono::system_clock>
class TimeServiceManager {
 public:
  TimeServiceManager(const std::shared_ptr<concordMetrics::Aggregator>& aggregator)
      : metrics_component_{concordMetrics::Component("time_service", aggregator)},
        soft_limit_reached_counter_{metrics_component_.RegisterCounter("soft_limit_reached_counter")},
        hard_limit_reached_counter_{metrics_component_.RegisterCounter("hard_limit_reached_counter")},
        new_time_is_less_or_equal_to_previous_{
            metrics_component_.RegisterCounter("new_time_is_less_or_equal_to_previous")},
        ill_formed_preprepare_{metrics_component_.RegisterCounter("ill_formed_preprepare")} {
    metrics_component_.Register();
  }
  ~TimeServiceManager() = default;
  TimeServiceManager(const TimeServiceManager&) = delete;

  // Loads timestamp from reserved pages, to be called once ST is done
  void load() {
    client_.load();
    LOG_INFO(TS_MNGR, "Loaded time data from reserved pages");
  }

  // Checks if the new time is less or equal to the one reserved pages,
  // if this is the case, returns reserved pages time + epsilon
  // otherwise, returns the new time
  [[nodiscard]] ConsensusTime compareAndSwap(ConsensusTime new_time) {
    auto last_timestamp = client_.getLastTimestamp();
    if (new_time > last_timestamp) {
      client_.setLastTimestamp(new_time);
      return new_time;
    }

    const auto& config = ReplicaConfig::instance();
    LOG_INFO(TS_MNGR,
             "New time(" << new_time.count() << "ms since epoch) is less or equal to reserved ("
                         << last_timestamp.count() << "), new time will be "
                         << (last_timestamp + config.timeServiceEpsilonMillis).count());
    new_time_is_less_or_equal_to_previous_++;
    metrics_component_.UpdateAggregator();
    last_timestamp += config.timeServiceEpsilonMillis;
    client_.setLastTimestamp(last_timestamp);
    return last_timestamp;
  }

  // Returns a client request message with timestamp (current system clock time)
  [[nodiscard]] std::unique_ptr<impl::ClientRequestMsg> createClientRequestMsg() const {
    const auto& config = ReplicaConfig::instance();
    const auto now = std::chrono::duration_cast<ConsensusTime>(ClockT::now().time_since_epoch());
    const auto& serialized = concord::util::serialize(now);
    return std::make_unique<impl::ClientRequestMsg>(config.replicaId,
                                                    MsgFlag::TIME_SERVICE_FLAG,
                                                    0U,
                                                    serialized.size(),
                                                    serialized.data(),
                                                    std::numeric_limits<uint64_t>::max(),
                                                    "TIME_SERVICE");
  }

  [[nodiscard]] bool hasTimeRequest(const impl::PrePrepareMsg& msg) const {
    if (msg.numberOfRequests() < 2) {
      LOG_ERROR(TS_MNGR, "PrePrepare with Time Service on, cannot have less than 2 messages");
      ill_formed_preprepare_++;
      metrics_component_.UpdateAggregator();
      return false;
    }
    auto it = impl::RequestsIterator(&msg);
    char* requestBody = nullptr;
    ConcordAssertEQ(it.getCurrent(requestBody), true);

    ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
    if (req.flags() != MsgFlag::TIME_SERVICE_FLAG) {
      LOG_ERROR(GL, "Time Service is on but first CR in PrePrepare is not TS request");
      ill_formed_preprepare_++;
      metrics_component_.UpdateAggregator();
      return false;
    }
    return true;
  }

  [[nodiscard]] bool isPrimarysTimeWithinBounds(const impl::PrePrepareMsg& msg) const {
    ConcordAssertGE(msg.numberOfRequests(), 1);

    auto it = impl::RequestsIterator(&msg);
    char* requestBody = nullptr;
    ConcordAssertEQ(it.getCurrent(requestBody), true);

    ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
    return isPrimarysTimeWithinBounds(req);
  }

  [[nodiscard]] bool isPrimarysTimeWithinBounds(impl::ClientRequestMsg& msg) const {
    ConcordAssert((msg.flags() & MsgFlag::TIME_SERVICE_FLAG) != 0 &&
                  "TimeServiceManager supports only messages with TIME_SERVICE_FLAG");
    const auto t = concord::util::deserialize<ConsensusTime>(msg.requestBuf(), msg.requestBuf() + msg.requestLength());
    const auto now = std::chrono::duration_cast<ConsensusTime>(ClockT::now().time_since_epoch());

    const auto& config = ReplicaConfig::instance();
    auto min = now - config.timeServiceHardLimitMillis;
    auto max = now + config.timeServiceHardLimitMillis;
    if (min > t || t > max) {
      LOG_ERROR(TS_MNGR,
                "Current primary's time reached hard limit, requests will be ignored. Please synchronize local clocks! "
                    << "Primary's time: " << t.count() << ", local time: " << now.count()
                    << ", difference: " << (t - now).count() << ", time limits: +/-"
                    << config.timeServiceHardLimitMillis.count() << ". Time is presented as ms since epoch");
      hard_limit_reached_counter_++;
      metrics_component_.UpdateAggregator();
      return false;
    }

    min = now - config.timeServiceSoftLimitMillis;
    max = now + config.timeServiceSoftLimitMillis;
    if (min > t || t > max) {
      LOG_WARN(TS_MNGR,
               "Current primary's time reached soft limit, please synchronize local clocks! "
                   << "Primary's time: " << t.count() << ", local time: " << now.count()
                   << ", difference: " << (t - now).count() << ", time limits: +/-"
                   << config.timeServiceSoftLimitMillis.count() << ". Time is presented as ms since epoch");
      soft_limit_reached_counter_++;
      metrics_component_.UpdateAggregator();
    }
    return true;
  }

 private:
  TimeServiceResPageClient client_;
  mutable concordMetrics::Component metrics_component_;
  mutable concordMetrics::CounterHandle soft_limit_reached_counter_;
  mutable concordMetrics::CounterHandle hard_limit_reached_counter_;
  mutable concordMetrics::CounterHandle new_time_is_less_or_equal_to_previous_;
  mutable concordMetrics::CounterHandle ill_formed_preprepare_;
};  // namespace bftEngine::impl
}  // namespace bftEngine::impl
