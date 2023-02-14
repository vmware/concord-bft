// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef THIN_REPLICA_IMPL_HPP_
#define THIN_REPLICA_IMPL_HPP_

#include <google/protobuf/util/time_util.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/status.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#ifdef USE_OPENTRACING
#include <opentracing/tracer.h>
#endif
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <fstream>

#include "log/logger.hpp"
#include "util/Metrics.hpp"
#include "db_interfaces.h"
#include "kv_types.hpp"
#include "kvbc_app_filter/kvbc_app_filter.h"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "crypto/openssl/certificates.hpp"

#include "thin_replica.grpc.pb.h"
#include "subscription_buffer.hpp"
#include "trs_metrics.hpp"
#include "util/filesystem.hpp"
#include "util/hex_tools.hpp"

using google::protobuf::util::TimeUtil;
using namespace std::chrono_literals;

namespace concord {
namespace thin_replica {

// Configuration for Thin Replica Server
struct ThinReplicaServerConfig {
  // is_insecure_trs if false indicates that TRS would communicate
  // with TRC using TLS, else the communication will be insecure.
  const bool is_insecure_trs;
  // tls_trs_cert_path indicates the path of the certificates
  // used by the TRS if it communicates securely with TRC.
  const std::string tls_trs_cert_path;
  // read-only storage is a concord key-value blockchain's read interface
  const concord::kvbc::IReader* rostorage;
  // subscriber_list is a list of subscribers where each subscriber is an spsc
  // queue. The updates are produced by the commands handler (single producer).
  // and consumed by the subscribers (single consumer) in the subscriber_list.
  // TRS acts as an intermediary, it waits for updates, filters them and sends
  // them to the subscribers.
  SubBufferList& subscriber_list;
  // the set of client IDs known to the TRS, used to authorize prospective
  // clients.
  std::unordered_set<std::string> client_id_set;
  // the threshold after which metrics aggregator is updated
  const uint16_t update_metrics_aggregator_thresh;
  const bool use_unified_certs = false;
  // the time duration the TRS waits before printing warning logs when
  // subscription status for live updates is not ok
  std::chrono::seconds no_live_subscription_warn_duration;

  ThinReplicaServerConfig(const bool is_insecure_trs_,
                          const std::string& tls_trs_cert_path_,
                          const concord::kvbc::IReader* rostorage_,
                          SubBufferList& subscriber_list_,
                          std::unordered_set<std::string>& client_id_set_,
                          const uint16_t update_metrics_aggregator_thresh_ = 100,
                          bool use_unified_certs_ = false,
                          std::chrono::seconds no_live_subscription_warn_duration_ = kNoLiveSubscriptionWarnDuration)
      : is_insecure_trs(is_insecure_trs_),
        tls_trs_cert_path(tls_trs_cert_path_),
        rostorage(rostorage_),
        subscriber_list(subscriber_list_),
        client_id_set(client_id_set_),
        update_metrics_aggregator_thresh(update_metrics_aggregator_thresh_),
        use_unified_certs(use_unified_certs_),
        no_live_subscription_warn_duration(no_live_subscription_warn_duration_) {}

 private:
  static constexpr std::chrono::seconds kNoLiveSubscriptionWarnDuration = 60s;
};

class ThinReplicaImpl {
 private:
  class StreamClosed : public std::runtime_error {
   public:
    StreamClosed(const std::string& msg) : std::runtime_error(msg){};
  };
  class StreamCancelled : public std::runtime_error {
   public:
    StreamCancelled(const std::string& msg) : std::runtime_error(msg){};
  };

  class UpdatePruned : public std::runtime_error {
   public:
    UpdatePruned(const std::string& msg) : std::runtime_error(msg){};
  };

  using KvbAppFilterPtr = std::shared_ptr<kvbc::KvbAppFilter>;
  static constexpr size_t kSubUpdateBufferSize{1000u};
  const std::chrono::milliseconds kWaitForUpdateTimeout{100};
  const std::string kCorrelationIdTag = "cid";
  // last timestamp when subscription status for live updates was not ok
  std::optional<std::chrono::steady_clock::time_point> last_failed_subscribe_status_time;
  uint16_t update_aggregator_counter{0};

 public:
  ThinReplicaImpl(std::unique_ptr<ThinReplicaServerConfig> config,
                  std::shared_ptr<concordMetrics::Aggregator> aggregator)
      : logger_(logging::getLogger("concord.thin_replica")), config_(std::move(config)), aggregator_(aggregator) {}

  ThinReplicaImpl(const ThinReplicaImpl&) = delete;
  ThinReplicaImpl(ThinReplicaImpl&&) = delete;
  ThinReplicaImpl& operator=(const ThinReplicaImpl&) = delete;
  ThinReplicaImpl& operator=(ThinReplicaImpl&&) = delete;

  template <typename ServerContextT, typename ServerWriterT>
  grpc::Status ReadState(ServerContextT* context,
                         const com::vmware::concord::thin_replica::ReadStateRequest* request,
                         ServerWriterT* stream) {
    auto [status, kvb_filter] = createKvbFilter(context, request);
    if (!status.ok()) {
      return status;
    }

    ThinReplicaServerMetrics metrics("data", getClientId(context));
    metrics.setAggregator(aggregator_);

    kvbc::BlockId start = 1;
    // E.L saw race condition where subscribe to live updates got a block that
    // wasn't added yet
    kvbc::BlockId end = (config_->rostorage)->getLastBlockId();
    metrics.num_storage_reads++;

    if (end == 0) {
      std::string msg{"No blocks available"};
      LOG_WARN(logger_, msg);
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, msg);
    }

    LOG_DEBUG(logger_, "ReadState start " << start << " end " << end);

    try {
      readFromKvbAndSendData(logger_, context, stream, start, end, kvb_filter, metrics);
    } catch (StreamCancelled& error) {
      return grpc::Status(grpc::StatusCode::CANCELLED, error.what());
    } catch (std::exception& error) {
      LOG_ERROR(logger_, "Failed to read and send state: " << error.what());
      return grpc::Status(grpc::StatusCode::UNKNOWN, "Failed to read and send state");
    }
    return grpc::Status::OK;
  }

  template <typename ServerContextT>
  grpc::Status ReadStateHash(ServerContextT* context,
                             const com::vmware::concord::thin_replica::ReadStateHashRequest* request,
                             com::vmware::concord::thin_replica::Hash* hash) {
    auto [status, kvb_filter] = createKvbFilter(context, request);
    if (!status.ok()) {
      return status;
    }

    ThinReplicaServerMetrics metrics("hash", getClientId(context));
    metrics.setAggregator(aggregator_);

    std::stringstream msg;
    if (isRequestOutOfRange(request, kvb_filter, metrics))
      return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, msg.str());
    if (isUpdatePruned(request, msg, kvb_filter, metrics)) return grpc::Status(grpc::StatusCode::NOT_FOUND, msg.str());

    LOG_DEBUG(logger_, "ReadStateHash");

    if (request->has_events()) {
      kvbc::BlockId block_id_start = 1;
      kvbc::BlockId block_id_end = request->events().block_id();

      if (block_id_end < block_id_start) {
        std::string msg{"Invalid block range"};
        msg += " [" + std::to_string(block_id_start) + "," + std::to_string(block_id_end) + "]";
        LOG_WARN(logger_, msg);
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, msg);
      }

      std::string kvb_hash;
      try {
        kvb_hash = kvb_filter->readBlockRangeHash(block_id_start, block_id_end);
        metrics.num_storage_reads += kvb_filter->num_storage_reads;
        metrics.updateAggregator();
      } catch (std::exception& error) {
        LOG_ERROR(logger_, error.what());
        std::stringstream msg;
        msg << "Reading StateHash for block " << block_id_end << " failed";
        return grpc::Status(grpc::StatusCode::UNKNOWN, msg.str());
      }

      hash->mutable_events()->set_block_id(block_id_end);
      hash->mutable_events()->set_hash(kvb_hash);

      return grpc::Status::OK;
    }
    // Request is an event_group request
    kvbc::EventGroupId event_group_id_start = 1;
    kvbc::EventGroupId event_group_id_end = request->event_groups().event_group_id();

    if (event_group_id_end < event_group_id_start) {
      std::string msg{"Invalid event group range"};
      msg += " [" + std::to_string(event_group_id_start) + "," + std::to_string(event_group_id_end) + "]";
      LOG_WARN(logger_, msg);
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, msg);
    }

    std::string kvb_eg_hash;
    try {
      kvb_eg_hash = kvb_filter->readEventGroupRangeHash(event_group_id_start);
      metrics.num_storage_reads += kvb_filter->num_storage_reads;
      metrics.updateAggregator();
    } catch (std::exception& error) {
      LOG_ERROR(logger_, error.what());
      std::stringstream msg;
      msg << "Reading StateHash for event_group_id " << event_group_id_end << " failed";
      return grpc::Status(grpc::StatusCode::UNKNOWN, msg.str());
    }

    hash->mutable_event_group()->set_event_group_id(event_group_id_end);
    hash->mutable_event_group()->set_hash(kvb_eg_hash);

    return grpc::Status::OK;
  }

  template <typename ServerContextT>
  grpc::Status AckUpdate(ServerContextT* context,
                         const com::vmware::concord::thin_replica::BlockId* block_id,
                         google::protobuf::Empty* empty) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "AckUpdate");
  }

  template <typename ServerContextT, typename ServerWriterT, typename DataT>
  grpc::Status SubscribeToUpdates(ServerContextT* context,
                                  const com::vmware::concord::thin_replica::SubscriptionRequest* request,
                                  ServerWriterT* stream) {
    std::string stream_type;
    if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
      stream_type = "data";
    } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
      stream_type = "hash";
    }

    auto [kvb_status, kvb_filter] = createKvbFilter(context, request);
    if (!kvb_status.ok()) {
      LOG_WARN(logger_,
               "SubscribeToUpdates createKvbFilter failed (" << kvb_status.error_code()
                                                             << "): " << kvb_status.error_message());
      return kvb_status;
    }

    // TRS metrics
    ThinReplicaServerMetrics metrics(stream_type, getClientId(context));
    metrics.setAggregator(aggregator_);
    update_aggregator_counter = 0;

    std::stringstream msg;
    if (isRequestOutOfRange(request, kvb_filter, metrics))
      return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, msg.str());
    if (isUpdatePruned(request, msg, kvb_filter, metrics)) return grpc::Status(grpc::StatusCode::NOT_FOUND, msg.str());

    auto [subscribe_status, live_updates] = subscribeToLiveUpdates(request, getClientId(context), kvb_filter);
    if (!subscribe_status.ok()) {
      auto current_time = std::chrono::steady_clock::now();
      std::stringstream msg;
      msg << "SubscribeToUpdates subscribeToLiveUpdates failed: (" << subscribe_status.error_code() << ") "
          << subscribe_status.error_message();
      if (!last_failed_subscribe_status_time.has_value()) {
        LOG_WARN(logger_, msg.str());
        last_failed_subscribe_status_time = current_time;
      } else {
        auto time_since_last_log =
            std::chrono::duration_cast<std::chrono::seconds>(current_time - last_failed_subscribe_status_time.value());
        if (time_since_last_log.count() >= config_->no_live_subscription_warn_duration.count()) {
          LOG_WARN(logger_, msg.str());
          last_failed_subscribe_status_time = current_time;
        }
      }
      return subscribe_status;
    }

    metrics.subscriber_list_size.Get().Set(config_->subscriber_list.Size());

#define CLEANUP_SUBSCRIPTION()                                               \
  {                                                                          \
    config_->subscriber_list.removeBuffer(live_updates);                     \
    live_updates->removeAllUpdates();                                        \
    live_updates->removeAllEventGroupUpdates();                              \
    metrics.subscriber_list_size.Get().Set(config_->subscriber_list.Size()); \
    metrics.num_storage_reads += kvb_filter->num_storage_reads;              \
    metrics.updateAggregator();                                              \
  }

    // If legacy event request then mark whether we need to transition into event groups
    bool is_event_group_transition = false;

    kvbc::BlockId start_block_id;
    if (request->has_events()) {
      start_block_id = request->events().block_id();
      if (auto opt = kvb_filter->getOldestEventGroupBlockId()) {
        if (start_block_id >= opt.value()) {
          is_event_group_transition = true;
        }
      }
    }

    // If last_known + 1 update requested keep waiting until we have at least one live update
    // Note that TRS considers last_known + 1 update request as valid, even if the requested
    // update doesn't exist in storage yet.
    if (is_event_group_transition) {
      // For an event group transition we need to check if the first/oldest external event group is available.
      // If not then we wait because it is similar to waiting for X+1 whereby X is 0.
      auto oldest_eg_id = kvb_filter->oldestExternalEventGroupId();
      while (not oldest_eg_id) {
        auto has_update = live_updates->waitForEventGroupUntilNonEmpty(kWaitForUpdateTimeout);
        if (context->IsCancelled()) {
          LOG_INFO(logger_, "StreamCancelled while waiting for the first live event group.");
          CLEANUP_SUBSCRIPTION();
          return grpc::Status::CANCELLED;
        }
        if (has_update) {
          // If there was an update for us then we must have updated storage
          oldest_eg_id = kvb_filter->oldestExternalEventGroupId();
        }
      }
    } else if (request->has_event_groups()) {
      auto requested_eg_id = request->event_groups().event_group_id();
      auto newest_eg_id = kvb_filter->newestExternalEventGroupId();
      // We already know that the request is valid hence the requested id can only be newest + 1 or less
      while (newest_eg_id < requested_eg_id) {
        auto has_update = live_updates->waitForEventGroupUntilNonEmpty(kWaitForUpdateTimeout);
        if (context->IsCancelled()) {
          LOG_INFO(logger_, "StreamCancelled while waiting for the next live event group.");
          CLEANUP_SUBSCRIPTION();
          return grpc::Status::CANCELLED;
        }
        if (has_update) {
          // If there was an update for us then we must have updated storage
          newest_eg_id = kvb_filter->newestExternalEventGroupId();
        }
      }
    } else {
      ConcordAssert(request->has_events());
      auto last_block_id = (config_->rostorage)->getLastBlockId();
      metrics.num_storage_reads++;
      if (request->events().block_id() == last_block_id + 1) {
        // `last_block_id + 1` is an acceptable requested block ID (not flagged with OUT_OF_RANGE exception),
        // even though it can not have been written to storage at the time of the request.
        // To ensure TRS doesn't try to read a non-existent block ID from storage, it must wait on the
        // live update queue until a block with ID greater than or equal to last_block_id + 1 gets added.
        // Note that, before a block gets pushed to the live update queue, it must be written to storage.
        bool is_update_available = false;
        while (not is_update_available) {
          if (context->IsCancelled()) {
            LOG_DEBUG(logger_, "StreamCancelled while waiting for the next live update.");
            CLEANUP_SUBSCRIPTION();
            return grpc::Status::CANCELLED;
          }
          is_update_available = live_updates->waitUntilNonEmpty(kWaitForUpdateTimeout);
          if (is_update_available && live_updates->oldestBlockId() < last_block_id + 1) {
            SubUpdate update;
            live_updates->Pop(update);
            LOG_DEBUG(logger_,
                      "Dropping block ID: " << update.block_id << " from live_updates, requested block ID: "
                                            << request->events().block_id());
            is_update_available = false;
          }
        }
      }
    }

    if (request->has_events() && !is_event_group_transition) {
      // Requested block ID received by the TRS must always be greater than zero.
      // Currently TRS expects TRC to take care of requests with block ID zero
      // TODO: Replace the assert with INVALID_ARGUMENT error thrown when block ID zero is requested
      ConcordAssertGT(start_block_id, 0);
      try {
        syncAndSend<ServerContextT, ServerWriterT, DataT>(
            context, start_block_id, live_updates, stream, kvb_filter, metrics);
      } catch (concord::kvbc::NoLegacyEvents& error) {
        LOG_WARN(logger_, error.what());
        is_event_group_transition = true;
      } catch (StreamCancelled& error) {
        CLEANUP_SUBSCRIPTION();
        return grpc::Status(grpc::StatusCode::CANCELLED, error.what());
      } catch (UpdatePruned& error) {
        LOG_WARN(logger_, "Requested update pruned in syncAndSend: " << error.what());
        CLEANUP_SUBSCRIPTION();
        return grpc::Status(grpc::StatusCode::NOT_FOUND, error.what());
      } catch (StreamClosed& error) {
        LOG_INFO(logger_, "StreamClosed in syncAndSend: " << error.what());
        CLEANUP_SUBSCRIPTION();
        return grpc::Status(grpc::StatusCode::CANCELLED, error.what());
      } catch (std::exception& error) {
        LOG_ERROR(logger_, error.what());
        CLEANUP_SUBSCRIPTION();
        std::stringstream msg;
        msg << "Couldn't transition from block id " << start_block_id << " to new blocks";
        return grpc::Status(grpc::StatusCode::UNKNOWN, msg.str());
      }
      // Read, filter, and send live updates
      SubUpdate update;
      try {
        while (!context->IsCancelled() && !is_event_group_transition) {
          metrics.queue_size.Get().Set(live_updates->Size());
          bool is_update_available = false;
          is_update_available = live_updates->TryPop(update, kWaitForUpdateTimeout);
          if (not is_update_available) {
            continue;
          }
          auto kvb_update =
              kvbc::KvbUpdate{update.block_id, update.correlation_id, std::move(update.immutable_kv_pairs)};
          const auto& filtered_update = kvb_filter->filterUpdate(kvb_update);
          const auto num_events = filtered_update.kv_pairs.size();
          const auto block_id = filtered_update.block_id;
          const auto client_id = getClientId(context);
          if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
            LOG_DEBUG(logger_, "Sending updates (live, data)" << KVLOG(client_id, block_id, num_events));
            auto correlation_id = filtered_update.correlation_id;
            if (update.parent_span) {
              sendData(stream, filtered_update, {*update.parent_span});
            } else {
              std::string propagated_span_context;
#ifdef USE_OPENTRACING
              auto span = opentracing::Tracer::Global()->StartSpan(
                  "trs_stream_update", {opentracing::SetTag{kCorrelationIdTag, correlation_id}});
              std::ostringstream context;
              const opentracing::Span& span_to_serialize = *span;
              span_to_serialize.tracer().Inject(span_to_serialize.context(), context);
              propagated_span_context = context.str();
#endif
              sendData(stream, filtered_update, {propagated_span_context});
            }
          } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
            LOG_DEBUG(logger_, "Sending updates (live, hash)" << KVLOG(client_id, block_id, num_events));
            sendHash(stream, update.block_id, kvb_filter->hashUpdate(filtered_update));
          }
          metrics.last_sent_block_id.Get().Set(update.block_id);
          metrics.num_storage_reads += kvb_filter->num_storage_reads;
          if (++update_aggregator_counter == config_->update_metrics_aggregator_thresh) {
            metrics.updateAggregator();
            update_aggregator_counter = 0;
          }
        }
      } catch (std::exception& error) {
        LOG_INFO(logger_, "Subscription stream closed: " << error.what());
      }

      // Clean up if we need to return
      if (not is_event_group_transition || context->IsCancelled()) {
        CLEANUP_SUBSCRIPTION();
        if (context->IsCancelled()) {
          LOG_INFO(logger_, "Subscription cancelled");
          return grpc::Status::CANCELLED;
        }
        return grpc::Status::OK;
      }
    }

    // A legacy event request might transition into an event group stream
    uint64_t event_group_id;
    if (is_event_group_transition) {
      ConcordAssert(request->has_events());
      // We assume that the caller wants updates but we cannot determine the event group id the caller is looking for.
      // Therefore, we start at the beginning.
      event_group_id = kvb_filter->oldestExternalEventGroupId();
      // If an event group transition is happening then we already confirmed that event groups are available.
      ConcordAssertNE(event_group_id, 0);
      LOG_INFO(logger_, "Legacy event request will receive event groups starting at id " << event_group_id);
    } else {
      ConcordAssert(request->has_event_groups());
      event_group_id = request->event_groups().event_group_id();
    }

    // Requested event group ID must always be greater than 0
    ConcordAssertGT(event_group_id, 0);
    try {
      syncAndSendEventGroups<ServerContextT, ServerWriterT, DataT>(
          context, event_group_id, live_updates, stream, kvb_filter, metrics);
    } catch (StreamCancelled& error) {
      LOG_WARN(logger_, "StreamCancelled in syncAndSendEventGroups: " << error.what());
      CLEANUP_SUBSCRIPTION();
      return grpc::Status(grpc::StatusCode::CANCELLED, error.what());
    } catch (UpdatePruned& error) {
      LOG_WARN(logger_, "Requested update pruned in syncAndSendEventGroups: " << error.what());
      CLEANUP_SUBSCRIPTION();
      return grpc::Status(grpc::StatusCode::NOT_FOUND, error.what());
    } catch (StreamClosed& error) {
      LOG_INFO(logger_, "StreamClosed in syncAndSendEventGroups: " << error.what());
      CLEANUP_SUBSCRIPTION();
      return grpc::Status(grpc::StatusCode::CANCELLED, error.what());
    } catch (std::exception& error) {
      LOG_ERROR(logger_, "Exception in syncAndSendEventGroups: " << error.what());
      CLEANUP_SUBSCRIPTION();
      std::stringstream msg;
      msg << "Couldn't transition from event_group_id " << event_group_id << " to new event groups";
      return grpc::Status(grpc::StatusCode::UNKNOWN, msg.str());
    }

    // Read, filter, and send live updates
    SubEventGroupUpdate sub_eg_update;
    try {
      while (not context->IsCancelled()) {
        metrics.queue_size.Get().Set(live_updates->SizeEventGroupQueue());
        bool is_update_available = false;
        is_update_available = live_updates->TryPopEventGroup(sub_eg_update, kWaitForUpdateTimeout);
        if (not is_update_available) {
          continue;
        }
        const auto& [last_ext_eg_id_read, last_global_eg_id_read] = kvb_filter->getLastEgIdsRead();
        // Event group read from live update queue should always be greater than last global event group ID read and
        // sent
        ConcordAssertGT(sub_eg_update.event_group_id, last_global_eg_id_read);

        auto eg_update = kvbc::EgUpdate{sub_eg_update.event_group_id, std::move(sub_eg_update.event_group)};

        auto next_ext_eg_id = last_ext_eg_id_read + 1;
        // TODO (Shruti):
        // We read and filter the first event group for the client from the live update queue twice.
        // Once in syncAndSendEventGroups() and once here. Do this only once.
        auto filtered_eg_update = kvb_filter->filterEventGroupUpdate(eg_update);
        if (!filtered_eg_update) {
          metrics.num_skipped_event_groups++;
          continue;
        }
        // Overwrite event group ID in the filtered update to external event group ID
        // We don't want to expose the global event group ID to the client
        filtered_eg_update.value().event_group_id = next_ext_eg_id;

        if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
          sendEventGroupData(stream, filtered_eg_update.value(), sub_eg_update.parent_span);
        } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
          sendEventGroupHash(stream,
                             filtered_eg_update.value().event_group_id,
                             kvb_filter->hashEventGroupUpdate(filtered_eg_update.value()));
        }

        kvb_filter->setLastEgIdsRead(next_ext_eg_id, sub_eg_update.event_group_id);

        metrics.last_sent_event_group_id.Get().Set(filtered_eg_update.value().event_group_id);
        metrics.num_storage_reads += kvb_filter->num_storage_reads;
        if (++update_aggregator_counter == config_->update_metrics_aggregator_thresh) {
          metrics.updateAggregator();
          update_aggregator_counter = 0;
        }
      }
    } catch (std::exception& error) {
      LOG_INFO(logger_, "Subscription stream closed: " << error.what());
    }

    CLEANUP_SUBSCRIPTION();
#undef CLEANUP_SUBSCRIPTION

    if (context->IsCancelled()) {
      LOG_INFO(logger_, "Subscription cancelled");
      return grpc::Status::CANCELLED;
    }
    return grpc::Status::OK;
  }

  template <typename ServerContextT>
  grpc::Status Unsubscribe(ServerContextT* context,
                           const google::protobuf::Empty* request,
                           google::protobuf::Empty* response) {
    // Note: In order to unsubscribe in a separate gRPC call, we need to connect
    // the sub buffer with the thin replica client id.
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Unsubscribe");
  }

  template <typename RequestT>
  bool isRequestOutOfRange(RequestT* request, KvbAppFilterPtr kvb_filter, ThinReplicaServerMetrics& metrics) {
    // A request is considered out of range iff the requested update ID is greater than
    // `last_known_update_id + 1`, i.e.; TRS allows an application to subscribe to
    // `last_known_update_id + 1` without throwing an error, with the expectation that
    // TRC would resubscribe.
    if (request->has_events()) {
      // Determine latest block available
      auto last_block_id = (config_->rostorage)->getLastBlockId();
      metrics.num_storage_reads++;
      if (request->events().block_id() > last_block_id + 1) {
        return true;
      }
    } else {
      // Determine latest event group available
      auto last_eg_id = kvb_filter->newestExternalEventGroupId();
      if (request->event_groups().event_group_id() > last_eg_id + 1) {
        return true;
      }
    }
    return false;
  }

  template <typename RequestT>
  bool isUpdatePruned(RequestT* request,
                      std::stringstream& msg,
                      KvbAppFilterPtr kvb_filter,
                      ThinReplicaServerMetrics& metrics) {
    if (request->has_events()) {
      // Determine oldest block available (pruning)
      auto first_block_id = (config_->rostorage)->getGenesisBlockId();
      metrics.num_storage_reads++;
      if (request->events().block_id() < first_block_id) {
        msg << "Block ID " << request->events().block_id() << " has been pruned."
            << " First block_id is " << first_block_id;
        LOG_WARN(logger_, msg.str());
        return true;
      }
    } else {
      // Determine oldest event group available (pruning)
      auto first_eg_id = kvb_filter->oldestExternalEventGroupId();
      auto last_eg_id = kvb_filter->newestExternalEventGroupId();
      // When handling requests after pruning, TRS must take into account the following two scenarios:
      // 1. New event groups have been added for the participant
      // 2. No new event group has been added for the participant
      // For e.g., assume 1-10 event groups have been pruned (event group IDs here are external event group IDs)
      // In scenario 1, assume one more event group has been added. Hence, first_eg_id is now 11, any request for eg
      // ID < 11 is invalid due to pruning.
      // In scenario 2, first_eg_id is set to 0 after pruning and has not yet updated
      // as there are no new event groups for the participant. we must therefore check against last_eg_id (10) to
      // identify whether the requested eg ID has been pruned. i.e., any requested eg ID <= 10 is invalid due to pruning
      // iff first_eg_id == 0.
      if (request->event_groups().event_group_id() < first_eg_id ||
          (!first_eg_id && request->event_groups().event_group_id() <= last_eg_id)) {
        msg << "Event group ID " << request->event_groups().event_group_id() << " has been pruned."
            << " First event_group_id is " << first_eg_id << " and last event_group_id is " << last_eg_id;
        LOG_WARN(logger_, msg.str());
        return true;
      }
    }
    return false;
  }

  // Parses the value of given delimiter field i.e., the client id from the subject
  // string
  static std::string parseClientIdFromSubject(const std::string& subject_str, const std::string& delimiter) {
    size_t start = subject_str.find(delimiter) + delimiter.length();
    size_t end = subject_str.find(',', start);
    std::string raw_str = subject_str.substr(start, end - start);
    size_t fstart = 0;
    size_t fend = raw_str.length();
    // remove surrounding whitespaces and newlines
    if (raw_str.find_first_not_of(' ') != std::string::npos) fstart = raw_str.find_first_not_of(' ');
    if (raw_str.find_last_not_of(' ') != std::string::npos) fend = raw_str.find_last_not_of(' ');
    raw_str.erase(std::remove(raw_str.begin(), raw_str.end(), '\n'), raw_str.end());
    return raw_str.substr(fstart, fend - fstart + 1);
  }

  template <typename ServerContextT>
  std::string getClientIdFromClientCert(ServerContextT* context) {
    std::string client_id;
    // get certificate from the client
    // on the client side `GRPC_X509_PEM_CERT_PROPERTY_NAME` returns the full certificate chain
    auto cert_prop_vec = context->auth_context()->FindPropertyValues(GRPC_X509_PEM_CERT_PROPERTY_NAME);
    if (cert_prop_vec.empty()) {
      LOG_FATAL(logger_, "No certificate chain received from the client");
      return client_id;
    }
    // cert_prop_vec is of type std::vector<grpc::string_ref>, the cert is the first element in the vector
    std::string cert_str = cert_prop_vec.front().data();
    const char* data = (const char*)cert_str.c_str();

    // encode the certificate received in string format to x509 certificate
    BIO* bio;
    X509* certificate;

    // A BIO is an I/O stream abstraction used by openssl
    bio = BIO_new(BIO_s_mem());  // returns a new BIO
    if (!bio) {
      throw std::runtime_error(
          "Failed to read certificate received from client for client "
          "authorization - BIO_new() failed!");
    }
    // attempts to write a null terminated string to BIO
    BIO_puts(bio, data);

    // read a certificate in PEM format from a BIO
    certificate = PEM_read_bio_X509(bio, NULL, NULL, NULL);
    if (!certificate) {
      BIO_free(bio);
      throw std::runtime_error(
          "Failed to encode certificate received from client for client "
          "authorization - PEM_read_bio_X509() failed!");
    }

    // get the subject from the certificate
    char* subj = X509_NAME_oneline(X509_get_subject_name(certificate), NULL, 0);
    std::string result(subj);

    LOG_DEBUG(logger_, "TRS Subject: " << result);
    // parse the O field i.e., the client_id from the certificate when use_unified_certs is enabled
    // else parse OU field
    std::string delim = (config_->use_unified_certs) ? "O=" : "OU=";
    size_t start = result.find(delim) + delim.length();
    size_t end = result.find('/', start);
    client_id = result.substr(start, end - start);
    BIO_free(bio);
    X509_free(certificate);
    OPENSSL_free(subj);
    return client_id;
  }

  static void getClientIdFromRootCert(logging::Logger logger_,
                                      const std::string& root_cert_path,
                                      std::unordered_set<std::string>& cert_ou_field_set,
                                      bool use_unified_certs) {
    auto field_name = (use_unified_certs ? "O" : "OU");
    auto attribute_list = crypto::getSubjectFieldListByName(root_cert_path, field_name);
    for (auto& val : attribute_list) {
      cert_ou_field_set.emplace(std::move(val));
    }
  }

  static void getClientIdSetFromRootCert(logging::Logger logger_,
                                         const std::string& root_cert_path,
                                         std::unordered_set<std::string>& cert_ou_field_set) {
    for (auto& p : fs::recursive_directory_iterator(root_cert_path)) {
      if ((p.path().filename().string()).compare("node.cert") == 0) {
        getClientIdFromRootCert(logger_, p.path().string(), cert_ou_field_set, true);
      }
    }
  }

  static std::string getClientIdFromCertificate(const std::string& client_cert_path, bool use_unified_certs = false) {
    auto field_name = (use_unified_certs ? "O" : "OU");
    return crypto::getSubjectFieldByName(client_cert_path, field_name);
  }

 private:
  template <typename ServerContextT, typename ServerWriterT>
  void readFromKvbAndSendData(logging::Logger logger,
                              ServerContextT* context,
                              ServerWriterT* stream,
                              kvbc::BlockId start,
                              kvbc::BlockId end,
                              std::shared_ptr<kvbc::KvbAppFilter> kvb_filter,
                              ThinReplicaServerMetrics& metrics) {
    boost::lockfree::spsc_queue<kvbc::KvbFilteredUpdate> queue{10};
    std::atomic_bool close_stream = false;
    auto kvb_reader = std::async(std::launch::async,
                                 &kvbc::KvbAppFilter::readBlockRange,
                                 kvb_filter,
                                 start,
                                 end,
                                 std::ref(queue),
                                 std::ref(close_stream));

    kvbc::KvbFilteredUpdate kvb_update;
    while (kvb_reader.wait_for(0s) != std::future_status::ready || !queue.empty()) {
      if (context->IsCancelled()) {
        close_stream = true;
        while (!queue.empty()) {
          queue.pop(kvb_update);
        }
        // update metrics before throwing the error
        metrics.num_storage_reads += kvb_filter->num_storage_reads;
        metrics.updateAggregator();
        throw StreamCancelled("Kvb data stream cancelled");
      }
      while (queue.pop(kvb_update)) {
        try {
          auto block_id = kvb_update.block_id;
          auto num_events = kvb_update.kv_pairs.size();
          auto client_id = getClientId(context);
          LOG_DEBUG(logger_, "Sending updates (history, data)" << KVLOG(client_id, block_id, num_events));
          sendData(stream, kvb_update);
          metrics.last_sent_block_id.Get().Set(block_id);
          metrics.num_storage_reads += kvb_filter->num_storage_reads;
          if (++update_aggregator_counter == config_->update_metrics_aggregator_thresh) {
            metrics.updateAggregator();
            update_aggregator_counter = 0;
          }
        } catch (StreamClosed& error) {
          LOG_WARN(logger, "Data stream closed at block " << kvb_update.block_id);

          // Stop kvb_reader and empty queue
          close_stream = true;
          while (!queue.empty()) {
            queue.pop(kvb_update);
          }
          // update metrics before throwing the error
          metrics.num_storage_reads += kvb_filter->num_storage_reads;
          metrics.updateAggregator();
          throw;
        }
      }
    }
    ConcordAssert(queue.empty());

    // Throws exception if something goes wrong
    kvb_reader.get();
  }

  template <typename ServerContextT, typename ServerWriterT>
  void readEventGroupsFromKvbAndSendData(logging::Logger logger,
                                         ServerContextT* context,
                                         ServerWriterT* stream,
                                         kvbc::EventGroupId start,
                                         std::shared_ptr<kvbc::KvbAppFilter> kvb_filter) {
    boost::lockfree::spsc_queue<kvbc::KvbFilteredEventGroupUpdate> queue{10};
    std::atomic_bool close_stream = false;

    auto kvb_reader = std::async(std::launch::async,
                                 &kvbc::KvbAppFilter::readEventGroupRange,
                                 kvb_filter,
                                 start,
                                 std::ref(queue),
                                 std::ref(close_stream));

    kvbc::KvbFilteredEventGroupUpdate kvb_eg_update;
    while (kvb_reader.wait_for(0s) != std::future_status::ready || !queue.empty()) {
      if (context->IsCancelled()) {
        close_stream = true;
        while (!queue.empty()) {
          queue.pop(kvb_eg_update);
        }
        throw StreamCancelled("Kvb event group data stream cancelled");
      }
      while (queue.pop(kvb_eg_update)) {
        try {
          sendEventGroupData(stream, kvb_eg_update);
        } catch (StreamClosed& error) {
          LOG_WARN(logger, "Data stream closed at event group id " << kvb_eg_update.event_group_id);

          // Stop kvb_reader and empty queue
          close_stream = true;
          while (!queue.empty()) {
            queue.pop(kvb_eg_update);
          }
          throw;
        }
      }
    }
    ConcordAssert(queue.empty());

    // Throws exception if something goes wrong
    kvb_reader.get();
  }

  template <typename ServerContextT, typename ServerWriterT>
  void readFromKvbAndSendHashes(logging::Logger logger,
                                ServerContextT* context,
                                ServerWriterT* stream,
                                kvbc::BlockId start,
                                kvbc::BlockId end,
                                std::shared_ptr<kvbc::KvbAppFilter> kvb_filter) {
    for (auto block_id = start; block_id <= end; ++block_id) {
      if (context->IsCancelled()) {
        throw StreamCancelled("Kvb hash stream cancelled");
      }
      std::string hash = kvb_filter->readBlockHash(block_id);
      sendHash(stream, block_id, hash);
    }
  }

  template <typename ServerContextT, typename ServerWriterT>
  void readEventGroupsFromKvbAndSendHashes(logging::Logger logger,
                                           ServerContextT* context,
                                           ServerWriterT* stream,
                                           kvbc::EventGroupId start,
                                           kvbc::EventGroupId end,
                                           std::shared_ptr<kvbc::KvbAppFilter> kvb_filter) {
    for (auto event_group_id = start; event_group_id <= end; ++event_group_id) {
      if (context->IsCancelled()) {
        throw StreamCancelled("Kvb event group hash stream cancelled");
      }
      std::string hash = kvb_filter->readEventGroupHash(event_group_id);
      sendEventGroupHash(stream, event_group_id, hash);
    }
  }

  // Read from KVB and send to the given stream depending on the data type
  template <typename ServerContextT, typename ServerWriterT, typename DataT>
  inline void readAndSend(logging::Logger logger,
                          ServerContextT* context,
                          ServerWriterT* stream,
                          kvbc::BlockId start,
                          kvbc::BlockId end,
                          std::shared_ptr<kvbc::KvbAppFilter> kvb_filter,
                          ThinReplicaServerMetrics& metrics) {
    static_assert(std::is_same<DataT, com::vmware::concord::thin_replica::Data>() ||
                      std::is_same<DataT, com::vmware::concord::thin_replica::Hash>(),
                  "We expect either a Data or Hash type");
    if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
      readFromKvbAndSendData(logger, context, stream, start, end, kvb_filter, metrics);
    } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
      readFromKvbAndSendHashes(logger, context, stream, start, end, kvb_filter);
    }
  }

  // Read event groups from KVB and send to the given stream depending on the data type
  template <typename ServerContextT, typename ServerWriterT, typename DataT>
  inline void readAndSendEventGroups(logging::Logger logger,
                                     ServerContextT* context,
                                     ServerWriterT* stream,
                                     kvbc::EventGroupId start,
                                     kvbc::EventGroupId end,
                                     std::shared_ptr<kvbc::KvbAppFilter> kvb_filter) {
    static_assert(std::is_same<DataT, com::vmware::concord::thin_replica::Data>() ||
                      std::is_same<DataT, com::vmware::concord::thin_replica::Hash>(),
                  "We expect either a Data or Hash type");
    if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
      readEventGroupsFromKvbAndSendData(logger, context, stream, start, kvb_filter);
    } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
      readEventGroupsFromKvbAndSendHashes(logger, context, stream, start, end, kvb_filter);
    }
  }

  // Read from KVB until we are in sync with the live updates. This function
  // returns when the next update can be taken from the given live updates.
  // We assume that the Commands handler is already filling the queue. Also, we
  // don't care if the queue fills up. In that case, the caller won't be able to
  // use the queue as soon as they consume it.
  template <typename ServerContextT, typename ServerWriterT, typename DataT>
  void syncAndSend(ServerContextT* context,
                   kvbc::BlockId start,
                   std::shared_ptr<SubUpdateBuffer>& live_updates,
                   ServerWriterT* stream,
                   std::shared_ptr<kvbc::KvbAppFilter>& kvb_filter,
                   ThinReplicaServerMetrics& metrics) {
    kvbc::BlockId first_block_id = (config_->rostorage)->getGenesisBlockId();
    metrics.num_storage_reads++;
    if (start < first_block_id) {
      std::stringstream msg;
      msg << "Requested block ID: " << start << " has been pruned, the current oldest block ID is: " << first_block_id;
      LOG_ERROR(logger_, msg.str());
      throw UpdatePruned(msg.str());
    }
    kvbc::BlockId end = (config_->rostorage)->getLastBlockId();
    metrics.num_storage_reads++;
    ConcordAssertLE(start, end);

    // Let's not wait for a live update yet due to there might be lots of
    // history we have to catch up with first
    LOG_INFO(logger_, "Sync reading from KVB [" << start << ", " << end << "]");
    readAndSend<ServerContextT, ServerWriterT, DataT>(logger_, context, stream, start, end, kvb_filter, metrics);

    // Let's wait until we have at least one live update
    bool is_update_available = false;
    while (not is_update_available) {
      is_update_available = live_updates->waitUntilNonEmpty(kWaitForUpdateTimeout);
      if (context->IsCancelled()) {
        throw StreamCancelled("StreamCancelled while waiting for the first live update");
      }
      // If event groups are enabled then all live updates will be event groups but they won't show up in the legacy
      // event queue. Therefore, let's query storage directly.
      if (kvb_filter->getOldestEventGroupBlockId()) {
        throw concord::kvbc::NoLegacyEvents();
      }
    }

    // We are in sync already
    if (live_updates->oldestBlockId() == (end + 1)) {
      return;
    }

    // Gap:
    // If the first live update is not the follow-up to the last read block from
    // KVB then we need to fill the gap. Let's read from KVB starting at end + 1
    // up to updates that are part of the live updates already. Thereby, we
    // create an overlap between what we read from KVB and what is currently in
    // the live updates.
    if (live_updates->oldestBlockId() > (end + 1)) {
      start = end + 1;
      end = live_updates->newestBlockId();

      LOG_INFO(logger_, "Sync filling gap [" << start << ", " << end << "]");
      readAndSend<ServerContextT, ServerWriterT, DataT>(logger_, context, stream, start, end, kvb_filter, metrics);
    }

    // Overlap:
    // If we read updates from KVB that were added to the live updates already
    // then we just need to drop the overlap and return
    ConcordAssertLE(live_updates->oldestBlockId(), end);
    SubUpdate update;
    do {
      live_updates->Pop(update);
      LOG_DEBUG(logger_, "Sync dropping " << update.block_id);
    } while (update.block_id < end);
  }

  // Read from KVB until we are in sync with the live updates. This function
  // returns when the next update can be taken from the given live updates.
  // We assume that the Commands handler is already filling the queue. Also, we
  // don't care if the queue fills up. In that case, the caller won't be able to
  // use the queue as soon as they consume it.
  template <typename ServerContextT, typename ServerWriterT, typename DataT>
  void syncAndSendEventGroups(ServerContextT* context,
                              kvbc::EventGroupId start,
                              std::shared_ptr<SubUpdateBuffer>& live_updates,
                              ServerWriterT* stream,
                              std::shared_ptr<kvbc::KvbAppFilter>& kvb_filter,
                              ThinReplicaServerMetrics& metrics) {
    auto start_ext_storage_eg_id = kvb_filter->oldestExternalEventGroupId();
    auto end_ext_storage_eg_id = kvb_filter->newestExternalEventGroupId();
    if (start < start_ext_storage_eg_id || (end_ext_storage_eg_id && !start_ext_storage_eg_id)) {
      std::stringstream msg;
      msg << "Requested event group ID: " << start
          << " has been pruned, the current oldest event group ID is: " << start_ext_storage_eg_id;
      LOG_ERROR(logger_, msg.str());
      throw UpdatePruned(msg.str());
    }
    if (!end_ext_storage_eg_id) {
      std::stringstream msg;
      msg << "No event group exists in KVB yet for client: " << getClientId(context);
      LOG_ERROR(logger_, msg.str());
    }
    ConcordAssertLE(start_ext_storage_eg_id, end_ext_storage_eg_id);
    ConcordAssertLE(start, end_ext_storage_eg_id);

    // Let's not wait for a live update yet as there might be lots of
    // history we have to catch up with first
    LOG_INFO(logger_, "Sync reading event groups from KVB [" << start << ", " << end_ext_storage_eg_id << "]");
    readAndSendEventGroups<ServerContextT, ServerWriterT, DataT>(
        logger_, context, stream, start, end_ext_storage_eg_id, kvb_filter);

    bool is_update_available = false;
    auto next_global_eg_id_to_read = kvb_filter->getLastEgIdsRead().second + 1;
    while (not is_update_available) {
      is_update_available = live_updates->waitForEventGroupUntilNonEmpty(kWaitForUpdateTimeout);
      if (context->IsCancelled()) {
        throw StreamCancelled("StreamCancelled while waiting for the first live update");
      }
      if (not is_update_available) continue;

      // Drop all live updates with global_eg_id < next_global_eg_id_to_read, because TRS has already read and sent
      // these updates from storage
      if (live_updates->oldestEventGroupId() < next_global_eg_id_to_read) {
        SubEventGroupUpdate sub_eg_update;
        live_updates->PopEventGroup(sub_eg_update);
        LOG_DEBUG(logger_, "Sync dropping " << sub_eg_update.event_group_id);
        is_update_available = false;
        continue;
      }

      auto ext_eg_id = end_ext_storage_eg_id + 1;  // next external event group ID to be read

      // At this point, all event groups in the live update queue are newer than anything we read from storage earlier.
      // If the oldest live update is not for the requesting client, keep reading from the live update queue
      // until the first relevant live update is reached. Drop all non-relevant live updates read along the way.
      if (live_updates->oldestEventGroupId() == next_global_eg_id_to_read) {
        auto filtered_eg_update = kvb_filter->filterEventGroupUpdate(live_updates->oldestEventGroup());
        if (!filtered_eg_update) {
          SubEventGroupUpdate sub_eg_update;
          live_updates->PopEventGroup(sub_eg_update);
          LOG_DEBUG(logger_, "Sync dropping upon filtering " << sub_eg_update.event_group_id);
          is_update_available = false;
          next_global_eg_id_to_read += 1;
          metrics.num_skipped_event_groups++;
          continue;
        }
        // The next event group in the live update queue has events for this client.
        return;
      }

      // Fill the gap:
      // Gap exists between next_global_eg_id_to_read and live_updates->oldestEventGroupId()
      // Let's read from data table directly, filter and send updates, until next_global_eg_id_to_read equals
      // live_updates->oldestEventGroupId()
      LOG_INFO(logger_,
               "Sync filling gap [" << next_global_eg_id_to_read << ", " << live_updates->oldestEventGroupId() << "]");
      while (live_updates->oldestEventGroupId() > next_global_eg_id_to_read) {
        auto event_group = kvb_filter->getEventGroup(next_global_eg_id_to_read);
        if (event_group.events.empty()) {
          std::stringstream msg;
          msg << "EventGroup empty/doesn't exist for global event group " << next_global_eg_id_to_read;
          throw kvbc::KvbReadError(msg.str());
        }
        auto eg_update_from_storage = kvbc::EgUpdate{event_group.id, std::move(event_group)};
        auto filtered_eg_update = kvb_filter->filterEventGroupUpdate(eg_update_from_storage);
        if (!filtered_eg_update) {
          next_global_eg_id_to_read++;
          metrics.num_skipped_event_groups++;
          continue;
        }
        // Overwrite event group ID in the filtered update to external event group ID:
        // Every client has their own event group ID counter (denoted as external event group ID).
        // For privacy and correctness, we don't want to expose to the client, the global event group ID corresponding
        // to the external event group ID
        filtered_eg_update.value().event_group_id = ext_eg_id;

        kvb_filter->setLastEgIdsRead(ext_eg_id++, next_global_eg_id_to_read++);

        if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
          sendEventGroupData(stream, filtered_eg_update.value());
        } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
          sendEventGroupHash(stream,
                             filtered_eg_update.value().event_group_id,
                             kvb_filter->hashEventGroupUpdate(filtered_eg_update.value()));
        }
      }
    }
    ConcordAssertEQ(next_global_eg_id_to_read, live_updates->oldestEventGroupId());
  }

  // Send* prepares the response object and puts it on the stream
  template <typename ServerWriterT>
  void sendData(ServerWriterT* stream,
                const kvbc::KvbFilteredUpdate& update,
                const std::optional<std::string>& span = std::nullopt) {
    com::vmware::concord::thin_replica::Data data;
    LOG_DEBUG(logger_, "sendData for block " << update.block_id);
    data.mutable_events()->set_block_id(update.block_id);
    data.mutable_events()->set_correlation_id(update.correlation_id);

    for (const auto& [key, value] : update.kv_pairs) {
      com::vmware::concord::thin_replica::KVPair* kvp_out = data.mutable_events()->add_data();
      kvp_out->set_key(key.data(), key.length());
      kvp_out->set_value(value.data(), value.length());
    }

    if (span) {
      data.mutable_events()->set_span_context(*span);
    }
    if (!stream->Write(data)) {
      throw StreamClosed("Data stream closed");
    }
  }

  // Send* prepares the response object and puts it on the stream
  template <typename ServerWriterT>
  void sendEventGroupData(ServerWriterT* stream,
                          const kvbc::KvbFilteredEventGroupUpdate& eg_update,
                          const std::optional<std::string>& span = std::nullopt) {
    com::vmware::concord::thin_replica::Data data;
    LOG_DEBUG(logger_, "sendEventGroupData for id " << eg_update.event_group_id);
    data.mutable_event_group()->set_id(eg_update.event_group_id);

    for (const auto& event : eg_update.event_group.events) {
      // TODO: Move don't copy.
      data.mutable_event_group()->add_events(event.data);
    }
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    TimeUtil::FromString(eg_update.event_group.record_time, timestamp);
    data.mutable_event_group()->set_allocated_record_time(timestamp);
    if (span) {
      data.mutable_event_group()->set_trace_context(*span);
    }
    if (!stream->Write(data)) {
      throw StreamClosed("Data event group stream closed");
    }
  }

  template <typename ServerWriterT>
  void sendHash(ServerWriterT* stream, kvbc::BlockId block_id, const std::string& update_hash) {
    com::vmware::concord::thin_replica::Hash hash;
    hash.mutable_events()->set_block_id(block_id);
    hash.mutable_events()->set_hash(update_hash);
    concordUtils::HexPrintBuffer update_hash_buff{update_hash.data(), update_hash.size()};
    LOG_DEBUG(logger_, "COMPARE SendHash block_id " << block_id << " update_hash " << update_hash_buff);
    if (!stream->Write(hash)) {
      throw StreamClosed("Hash stream closed");
    }
  }

  template <typename ServerWriterT>
  void sendEventGroupHash(ServerWriterT* stream, kvbc::EventGroupId event_group_id, const std::string& update_hash) {
    com::vmware::concord::thin_replica::Hash hash;
    hash.mutable_event_group()->set_event_group_id(event_group_id);
    hash.mutable_event_group()->set_hash(update_hash);
    LOG_DEBUG(logger_, "COMPARE SendHash event group id " << event_group_id << " update_hash " << update_hash);
    if (!stream->Write(hash)) {
      throw StreamClosed("Hash event group stream closed");
    }
  }

  // Get client_id from metadata if using insecure TRS
  // Get client_id from the client certs if using secure TRS
  // and compare the client_id with the client_id in known root cert
  template <typename ServerContextT>
  std::string getClientId(ServerContextT* context) {
    if (config_->is_insecure_trs) {
      auto metadata = context->client_metadata();
      auto client_id = metadata.find("client_id");
      if (client_id != metadata.end()) {
        return std::string(client_id->second.data(), client_id->second.length());
      }
      throw std::invalid_argument("client_id metadata is missing");
    } else {
      return getAuthorizedClientId(context);
    }
  }

  // Get client_id from the client certs if using secure TRS
  // and compare the client_id with the client_id in known root cert
  template <typename ServerContextT>
  std::string getAuthorizedClientId(ServerContextT* context) {
    if (context->auth_context() && context->auth_context()->IsPeerAuthenticated()) {
      std::string client_id = getClientIdFromClientCert(context);
      if (!client_id.empty()) {
        if (config_->client_id_set.find(client_id) != config_->client_id_set.end()) {
          return client_id;
        } else {
          LOG_FATAL(logger_,
                    "Client is not authorized, given client_id " << client_id << " doesn't match any known client IDs");
          throw std::runtime_error("Client is not authorized!");
        }
      }
      throw std::invalid_argument("client_id is missing in the client certificates");
    }
    throw std::runtime_error("Client is not authenticated!");
  }

  template <typename ServerContextT, typename RequestT>
  std::tuple<grpc::Status, KvbAppFilterPtr> createKvbFilter(ServerContextT* context, const RequestT* request) {
    KvbAppFilterPtr kvb_filter;
    try {
      kvb_filter = std::make_shared<kvbc::KvbAppFilter>(config_->rostorage, getClientId(context));
    } catch (std::exception& error) {
      std::stringstream msg;
      msg << "Failed to set up filter: " << error.what();
      LOG_ERROR(logger_, msg.str());
      return {grpc::Status(grpc::StatusCode::UNKNOWN, msg.str()), nullptr};
    }
    return {grpc::Status::OK, kvb_filter};
  }

  template <typename RequestT>
  std::tuple<grpc::Status, std::shared_ptr<SubUpdateBuffer>> subscribeToLiveUpdates(
      RequestT* request, const std::string& client_id, std::shared_ptr<kvbc::KvbAppFilter>& kvb_filter) {
    auto live_updates = std::make_shared<SubUpdateBuffer>(kSubUpdateBufferSize);
    bool success = config_->subscriber_list.addBuffer(live_updates);
    if (!success) {
      std::stringstream msg;
      msg << "Failed to add subscriber queue";
      LOG_ERROR(logger_, msg.str());
      return {grpc::Status(grpc::StatusCode::INTERNAL, msg.str()), live_updates};
    }
    return {grpc::Status::OK, live_updates};
  }

 private:
  logging::Logger logger_;
  std::unique_ptr<ThinReplicaServerConfig> config_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
};
}  // namespace thin_replica
}  // namespace concord

#endif /* end of include guard: THIN_REPLICA_IMPL_HPP_ */
