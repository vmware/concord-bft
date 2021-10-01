// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/thin-replica-client/thin_replica_client.hpp"

#include <opentracing/propagation.h>
#include <opentracing/span.h>
#include <opentracing/tracer.h>
#include <memory>
#include <numeric>
#include <sstream>

#include "client/thin-replica-client/trace_contexts.hpp"
#include "client/thin-replica-client/trc_hash.hpp"
#include "client/thin-replica-client/trs_connection.hpp"

using com::vmware::concord::thin_replica::BlockId;
using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::KVPair;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;
using concord::client::concordclient::EventVariant;
using concord::client::concordclient::EventGroup;
using concord::client::concordclient::Update;
using std::atomic_bool;
using std::list;
using std::logic_error;
using std::make_pair;
using std::pair;
using std::runtime_error;
using std::string;
using std::stringstream;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using std::chrono::steady_clock;

namespace client::thin_replica_client {

const string LogCid::cid_key_ = "cid";
atomic_bool LogCid::cid_set_ = false;

LogCid::LogCid(const std::string& cid) {
  bool expected_cid_set_state_ = false;
  if (!cid_set_.compare_exchange_strong(expected_cid_set_state_, true)) {
    throw logic_error("Attempting to add CID to a logging context that already has a CID.");
  }
  MDC_PUT(cid_key_, cid);
}

LogCid::~LogCid() {
  MDC_REMOVE(cid_key_);
  cid_set_ = false;
}

void ThinReplicaClient::recordCollectedHash(size_t update_source,
                                            bool is_event_group,
                                            uint64_t id,
                                            const string& update_hash,
                                            HashRecordMap& server_indexes_by_reported_update,
                                            size_t& maximal_agreeing_subset_size,
                                            HashRecord& maximally_agreed_on_update) {
  HashRecord update_record;
  if (is_event_group) {
    update_record.type = HashRecord::Type::EventGroup;
    update_record.id = id;
    update_record.hash = update_hash;
  } else {
    update_record.type = HashRecord::Type::LegacyEvent;
    update_record.id = id;
    update_record.hash = update_hash;
  }

  if (server_indexes_by_reported_update.count(update_record) < 1) {
    server_indexes_by_reported_update.emplace(update_record, unordered_set<size_t>());
  }
  server_indexes_by_reported_update[update_record].emplace(update_source);
  size_t update_agreement = server_indexes_by_reported_update[update_record].size();
  if (update_agreement > maximal_agreeing_subset_size) {
    maximal_agreeing_subset_size = update_agreement;
    maximally_agreed_on_update = update_record;
  }
}

void ThinReplicaClient::readUpdateHashFromStream(size_t server_index,
                                                 HashRecordMap& server_indexes_by_reported_update,
                                                 size_t& maximal_agreeing_subset_size,
                                                 HashRecord& maximally_agreed_on_update) {
  Hash hash;
  LOG_DEBUG(logger_, "Read hash from " << server_index);

  TrsConnection::Result read_result = config_->trs_conns[server_index]->readHash(&hash);
  if (read_result == TrsConnection::Result::kTimeout) {
    LOG_DEBUG(logger_, "Hash stream " << server_index << " timed out.");
    metrics_.read_timeouts_per_update++;
    return;
  }
  if (read_result == TrsConnection::Result::kFailure) {
    LOG_DEBUG(logger_, "Hash stream " << server_index << " read failed.");
    metrics_.read_failures_per_update++;
    return;
  }
  ConcordAssert(read_result == TrsConnection::Result::kSuccess);

  uint64_t hash_id;
  string hash_string;
  if (hash.has_event_group()) {
    hash_id = hash.event_group().event_group_id();
    ConcordAssert(latest_verified_event_group_id_ <
                  std::numeric_limits<decltype(latest_verified_event_group_id_)>::max());
    if (hash_id < latest_verified_event_group_id_) {
      LOG_WARN(logger_,
               "Hash stream " << server_index << " gave an update with decreasing event group number: " << hash_id);
      metrics_.read_ignored_per_update++;
      return;
    }
    hash_string = hash.event_group().hash();

    if (hash.event_group().hash().length() > kThinReplicaHashLength) {
      LOG_WARN(logger_,
               "Hash stream " << server_index << " gave an update (event_group " << hash_id
                              << ") with an unexpectedly long hash: " << hash.events().hash().length());
      metrics_.read_ignored_per_update++;
      return;
    }
  } else {
    ConcordAssert(hash.has_events());
    hash_id = hash.events().block_id();
    if (hash_id < latest_verified_block_id_) {
      LOG_WARN(logger_, "Hash stream " << server_index << " gave an update with decreasing update number: " << hash_id);
      metrics_.read_ignored_per_update++;
      return;
    }
    hash_string = hash.events().hash();

    if (hash.events().hash().length() > kThinReplicaHashLength) {
      LOG_WARN(logger_,
               "Hash stream " << server_index << " gave an update (block " << hash_id
                              << ") with an unexpectedly long hash: " << hash.events().hash().length());
      metrics_.read_ignored_per_update++;
      return;
    }
  }

  LOG_DEBUG(logger_, "Record hash for update " << hash_id);
  ConcordAssert(hash_string.length() <= kThinReplicaHashLength);
  hash_string.resize(kThinReplicaHashLength, '\0');

  recordCollectedHash(server_index,
                      hash.has_event_group(),
                      hash_id,
                      hash_string,
                      server_indexes_by_reported_update,
                      maximal_agreeing_subset_size,
                      maximally_agreed_on_update);
}

std::pair<bool, ThinReplicaClient::SpanPtr> ThinReplicaClient::readBlock(Data& update_in,
                                                                         HashRecordMap& agreeing_subset_members,
                                                                         size_t& most_agreeing,
                                                                         HashRecord& most_agreed_block,
                                                                         unique_ptr<LogCid>& cid) {
  if (!config_->trs_conns[data_conn_index_]->hasDataStream()) {
    // It may be the case that there is no data stream open after the data
    // stream was opened or rotated because the initial SubscribeToUpdates call
    // failed or timed out; in this case the ThinReplicaClient should rotate the
    // data stream in the same way as if the first read on that stream didn't
    // succeed; therefore readBlock treats not having a data stream when it is
    // called the same as a read failure.
    return {false, nullptr};
  }

  TrsConnection::Result read_result = config_->trs_conns[data_conn_index_]->readData(&update_in);
  if (read_result == TrsConnection::Result::kTimeout) {
    LOG_DEBUG(logger_, "Data stream " << data_conn_index_ << " timed out");
    metrics_.read_timeouts_per_update++;
    return {false, nullptr};
  }
  if (read_result == TrsConnection::Result::kFailure) {
    LOG_DEBUG(logger_, "Data stream " << data_conn_index_ << " read failed");
    metrics_.read_failures_per_update++;
    return {false, nullptr};
  }
  ConcordAssert(read_result == TrsConnection::Result::kSuccess);

  uint64_t id;  // block id or event group id
  SpanPtr span;
  if (update_in.has_event_group()) {
    id = update_in.event_group().id();
    // TODO: Event Group traces
    span.reset();
    cid.reset(new LogCid(to_string(id)));
    ConcordAssert(latest_verified_event_group_id_ <
                  std::numeric_limits<decltype(latest_verified_event_group_id_)>::max());
    id = update_in.event_group().id();
    if (id < latest_verified_event_group_id_) {
      LOG_WARN(logger_, "Data stream " << data_conn_index_ << " gave an update with decreasing event group id: " << id);
      metrics_.read_ignored_per_update++;
      cid.reset(nullptr);
      return {false, nullptr};
    }
  } else {
    ConcordAssert(update_in.has_events());
    span = TraceContexts::CreateChildSpanFromBinary(
        update_in.events().span_context(), "trc_read_block", update_in.events().correlation_id(), logger_);
    cid.reset(new LogCid(update_in.events().correlation_id()));
    id = update_in.events().block_id();
    if (id < latest_verified_block_id_) {
      LOG_WARN(logger_, "Data stream " << data_conn_index_ << " gave an update with decreasing block number: " << id);
      metrics_.read_ignored_per_update++;
      cid.reset(nullptr);
      return {false, nullptr};
    }
  }

  string update_data_hash = hashUpdate(update_in);
  recordCollectedHash(data_conn_index_,
                      update_in.has_event_group(),
                      id,
                      update_data_hash,
                      agreeing_subset_members,
                      most_agreeing,
                      most_agreed_block);
  return {true, std::move(span)};
}

TrsConnection::Result ThinReplicaClient::startHashStreamWith(size_t server_index) {
  ConcordAssert(server_index != data_conn_index_);
  config_->trs_conns[server_index]->cancelHashStream();

  SubscriptionRequest request;
  if (is_event_group_request_) {
    request.mutable_event_groups()->set_event_group_id(latest_verified_event_group_id_ + 1);
  } else {
    request.mutable_events()->set_block_id(latest_verified_block_id_ + 1);
  }
  return config_->trs_conns[server_index]->openHashStream(request);
}

void ThinReplicaClient::findBlockHashAgreement(std::vector<bool>& servers_tried,
                                               HashRecordMap& agreeing_subset_members,
                                               size_t& most_agreeing,
                                               HashRecord& most_agreed_block,
                                               SpanPtr& parent_span) {
  SpanPtr span = nullptr;
  if (parent_span) {
    span = opentracing::Tracer::Global()->StartSpan("trclient_verify_hash_against_additional_servers",
                                                    {opentracing::ChildOf(&parent_span->context())});
  }

  // Create a list of server indexes so that we start iterating over the ones
  // that have an open stream already. If we cannot find agreement amongst them
  // then we keep going and try the other servers too.
  std::vector<size_t> sorted_servers(config_->trs_conns.size());
  std::iota(sorted_servers.begin(), sorted_servers.end(), 0);
  std::stable_sort(sorted_servers.begin(), sorted_servers.end(), [this](auto a, auto b) {
    return config_->trs_conns[a]->hasHashStream() > config_->trs_conns[b]->hasHashStream();
  });

  for (auto server_index : sorted_servers) {
    ConcordAssertNE(config_->trs_conns[server_index], nullptr);
    if (servers_tried[server_index]) {
      continue;
    }
    if (stop_subscription_thread_) {
      return;
    }

    if (!config_->trs_conns[server_index]->hasHashStream()) {
      LOG_DEBUG(logger_, "Additionally asking " << server_index);
      TrsConnection::Result stream_open_status = startHashStreamWith(server_index);

      // Assert the possible TrsConnection::Result values have not changed
      // without updating the following code.
      ConcordAssert(stream_open_status == TrsConnection::Result::kSuccess ||
                    stream_open_status == TrsConnection::Result::kTimeout ||
                    stream_open_status == TrsConnection::Result::kFailure);

      if (stream_open_status == TrsConnection::Result::kTimeout) {
        LOG_DEBUG(logger_, "Opening a hash stream to server " << server_index << " timed out.");
        metrics_.read_timeouts_per_update++;
      }
      if (stream_open_status == TrsConnection::Result::kFailure) {
        LOG_DEBUG(logger_, "Opening a hash stream to server " << server_index << " failed.");
        metrics_.read_failures_per_update++;
      }
      if (stream_open_status != TrsConnection::Result::kSuccess) {
        servers_tried[server_index] = true;
        continue;
      }
    }

    readUpdateHashFromStream(server_index, agreeing_subset_members, most_agreeing, most_agreed_block);
    servers_tried[server_index] = true;

    if (most_agreeing >= (config_->max_faulty + 1)) {
      return;
    }
  }
}

TrsConnection::Result ThinReplicaClient::resetDataStreamTo(size_t server_index) {
  ConcordAssertNE(config_->trs_conns[server_index], nullptr);
  config_->trs_conns[server_index]->cancelDataStream();
  config_->trs_conns[server_index]->cancelHashStream();
  config_->trs_conns[data_conn_index_]->cancelDataStream();
  config_->trs_conns[data_conn_index_]->cancelHashStream();

  SubscriptionRequest request;

  if (is_event_group_request_) {
    request.mutable_event_groups()->set_event_group_id(latest_verified_event_group_id_ + 1);
  } else {
    request.mutable_events()->set_block_id(latest_verified_block_id_ + 1);
  }

  TrsConnection::Result result = config_->trs_conns[server_index]->openDataStream(request);

  data_conn_index_ = server_index;
  return result;
}

void ThinReplicaClient::closeAllHashStreams() {
  for (size_t i = 0; i < config_->trs_conns.size(); ++i) {
    if (i != data_conn_index_) {
      config_->trs_conns[i]->cancelHashStream();
    }
  }
}

bool ThinReplicaClient::rotateDataStreamAndVerify(Data& update_in,
                                                  HashRecordMap& agreeing_subset_members,
                                                  HashRecord& most_agreed_block,
                                                  SpanPtr& parent_span,
                                                  unique_ptr<LogCid>& cid) {
  SpanPtr span = nullptr;
  if (parent_span) {
    span = opentracing::Tracer::Global()->StartSpan("trclient_rotate_server_and_verify_hash",
                                                    {opentracing::ChildOf(&parent_span->context())});
  }

  for (const auto server_index : agreeing_subset_members[most_agreed_block]) {
    ConcordAssert(server_index < config_->trs_conns.size());
    if (stop_subscription_thread_) {
      return false;
    }

    TrsConnection::Result open_stream_result = resetDataStreamTo(server_index);

    TrsConnection::Result read_result = TrsConnection::Result::kUnknown;
    if (open_stream_result == TrsConnection::Result::kSuccess) {
      read_result = config_->trs_conns[data_conn_index_]->readData(&update_in);
    }
    if (open_stream_result == TrsConnection::Result::kTimeout || read_result == TrsConnection::Result::kTimeout) {
      LOG_DEBUG(logger_, "Read timed out on a data subscription stream (to server index " << server_index << ").");
      metrics_.read_timeouts_per_update++;
      continue;
    }
    if (open_stream_result == TrsConnection::Result::kFailure || read_result == TrsConnection::Result::kFailure) {
      LOG_DEBUG(logger_, "Read failed on a data subscription stream (to server index " << server_index << ").");
      metrics_.read_failures_per_update++;
      continue;
    }
    ConcordAssert(open_stream_result == TrsConnection::Result::kSuccess &&
                  read_result == TrsConnection::Result::kSuccess);

    string correlation_id;
    uint64_t update_id;  // Block id or event group id
    if (update_in.has_event_group()) {
      update_id = update_in.event_group().id();
      correlation_id = to_string(update_id);
    } else {
      ConcordAssert(update_in.has_events());
      update_id = update_in.events().block_id();
      correlation_id = update_in.events().correlation_id();
    }
    cid.reset();
    cid.reset(new LogCid(correlation_id));
    if (update_id != most_agreed_block.id) {
      LOG_WARN(logger_,
               "Data stream " << server_index << " gave an update with id (" << update_id
                              << ") in "
                                 "disagreement with the consensus and "
                                 "contradicting its own hash update.");
      metrics_.read_ignored_per_update++;
      cid.reset();
      continue;
    }

    string update_data_hash = hashUpdate(update_in);
    if (update_data_hash != most_agreed_block.hash) {
      LOG_WARN(logger_,
               "Data stream " << server_index
                              << " gave an update hashing to a value "
                                 "in disagreement with the consensus on the "
                                 "hash for this update ("
                              << update_id
                              << ") and contradicting the "
                                 "server's own hash update.");
      metrics_.read_ignored_per_update++;
      cid.reset();
      continue;
    }

    return true;
  }
  return false;
}

void ThinReplicaClient::logDataStreamResetResult(const TrsConnection::Result& result, size_t server_index) {
  // Assert the possible TrsConnection::Result values have not changed without
  // updating the following code.
  ConcordAssert(result == TrsConnection::Result::kSuccess || result == TrsConnection::Result::kTimeout ||
                result == TrsConnection::Result::kFailure);

  if (result == TrsConnection::Result::kTimeout) {
    LOG_DEBUG(logger_, "Opening a data stream to server " << server_index << " timed out.");
    metrics_.read_timeouts_per_update++;
  }
  if (result == TrsConnection::Result::kFailure) {
    LOG_DEBUG(logger_, "Opening a data stream to server " << server_index << " failed.");
    metrics_.read_failures_per_update++;
  }
}

void ThinReplicaClient::receiveUpdates() {
  ConcordAssert(config_->trs_conns.size() > 0);

  if (stop_subscription_thread_) {
    LOG_WARN(logger_, "Need to stop receiving updates");
    return;
  }

  // Set initial data stream
  logDataStreamResetResult(resetDataStreamTo(0), 0);

  // last timestamp when responsive agreeing servers were less than config_->max_faulty + 1
  std::optional<std::chrono::steady_clock::time_point> last_non_agreement_time;

  // Main subscription-driving loop; one iteration of this outer loop
  // corresponds to receiving, validating, and returning one update.
  // We break out of this loop only if the application sets the flag.
  while (!stop_subscription_thread_) {
    // For each loop of the outer iteration, we need to find at least
    // (max_faulty_ + 1) responsive agreeing servers (we count the server that
    // gave us the actual data for this update as one of the agreeing, so we
    // need it plus max_faulty_ servers giving agreeing hashes) in order to
    // validate and return an update.
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    Data update_in;
    unique_ptr<LogCid> update_cid;
    SpanPtr span = nullptr;
    vector<bool> servers_tried(config_->trs_conns.size(), false);

    HashRecordMap agreeing_subset_members;
    HashRecord most_agreed_block;
    size_t most_agreeing = 0;
    bool has_data = false;
    bool has_verified_data = false;

    // First, we collect updates from all subscription streams we have which
    // are already open, starting with the data stream and followed by any hash
    // streams.
    LOG_DEBUG(logger_, "Read from data stream " << data_conn_index_);
    std::tie(has_data, span) =
        readBlock(update_in, agreeing_subset_members, most_agreeing, most_agreed_block, update_cid);
    servers_tried[data_conn_index_] = true;

    uint64_t update_id = update_in.has_event_group() ? update_in.event_group().id() : update_in.events().block_id();
    LOG_DEBUG(logger_,
              "Find hash agreement amongst all servers for update " << (has_data ? to_string(update_id) : "n/a"));
    findBlockHashAgreement(servers_tried, agreeing_subset_members, most_agreeing, most_agreed_block, span);
    if (stop_subscription_thread_) {
      break;
    }

    // At this point we need to have agreeing servers.
    if (most_agreeing < (config_->max_faulty + 1)) {
      // print the warning every minute to avoid flooding with logs
      std::string msg = "Couldn't find agreement amongst all servers. Try again.";
      auto current_time = std::chrono::steady_clock::now();
      if (!last_non_agreement_time.has_value()) {
        LOG_WARN(logger_, msg);
        last_non_agreement_time = current_time;
      } else {
        auto time_since_last_log =
            std::chrono::duration_cast<std::chrono::seconds>(current_time - last_non_agreement_time.value());
        if (time_since_last_log.count() >= config_->no_agreement_warn_duration.count()) {
          LOG_WARN(logger_, msg);
          last_non_agreement_time = current_time;
        }
      }

      // We need to force re-subscription on at least one of the f+1 open
      // streams otherwise we might skip an update. By closing all streams here
      // we do exactly what the algorithm would do in the next iteration of this
      // loop anyways.
      closeAllHashStreams();
      size_t new_data_index = (data_conn_index_ + 1) % config_->trs_conns.size();
      logDataStreamResetResult(resetDataStreamTo(new_data_index), new_data_index);
      continue;
    }

    // If we have data, check whether its hash is the agreement.
    if (has_data && agreeing_subset_members[most_agreed_block].count(data_conn_index_) > 0) {
      has_verified_data = true;
    }

    // We have enough agreeing servers but, if the existing data stream is not
    // among them then let's rotate the data stream to one of the servers
    // within the agreeing set.
    if (!has_verified_data) {
      has_verified_data =
          rotateDataStreamAndVerify(update_in, agreeing_subset_members, most_agreed_block, span, update_cid);
      if (!has_verified_data) {
        LOG_WARN(logger_, "Couldn't get data from agreeing servers. Try again.");
        // We need to force re-subscription on at least one of the f+1 open
        // streams otherwise we might skip an update. By closing all streams
        // here we do exactly what the algorithm would do in the next iteration
        // of this loop anyways.
        closeAllHashStreams();
        size_t new_data_index = (data_conn_index_ + 1) % config_->trs_conns.size();
        logDataStreamResetResult(resetDataStreamTo(new_data_index), new_data_index);
        continue;
      }
    }

    ConcordAssert(has_verified_data);
    LOG_DEBUG(logger_, "Read and verified data for update " << update_id);

    ConcordAssertNE(config_->update_queue, nullptr);

    auto update = std::make_unique<EventVariant>();
    if (update_in.has_event_group()) {
      EventGroup event_group;
      event_group.id = update_in.event_group().id();
      for (auto& event : update_in.event_group().events()) {
        event_group.events.push_back(event);
      }
      event_group.record_time = update_in.event_group().record_time();
      // TODO: Set trace context
      latest_verified_event_group_id_ = event_group.id;
      update->emplace<EventGroup>(std::move(event_group));
      // If we started with a legacy request then the transition has happened now
      is_event_group_request_ = true;
    } else {
      ConcordAssert(update_in.has_events());
      Update legacy_event;
      legacy_event.block_id = update_in.events().block_id();
      legacy_event.correlation_id_ = update_in.events().correlation_id();
      for (const auto& kvp_in : update_in.events().data()) {
        legacy_event.kv_pairs.push_back(make_pair(kvp_in.key(), kvp_in.value()));
      }
      latest_verified_block_id_ = legacy_event.block_id;
      update->emplace<Update>(std::move(legacy_event));
      TraceContexts::InjectSpan(span, *update);
    }

    if (metrics_.read_timeouts_per_update.Get().Get() > 0 || metrics_.read_failures_per_update.Get().Get() > 0 ||
        metrics_.read_ignored_per_update.Get().Get() > 0) {
      LOG_WARN(logger_,
               metrics_.read_timeouts_per_update.Get().Get()
                   << " timeouts, " << metrics_.read_failures_per_update.Get().Get() << " failures, and "
                   << metrics_.read_ignored_per_update.Get().Get() << " ignored while retrieving update " << update_id);
    }

    // Push update to update queue for consumption before receiving next update
    pushUpdateToUpdateQueue(std::move(update), start, update_in.has_event_group());

    // Cleanup before the next update

    // The main subscription loop should not be leaving any more than
    // (max_faulty_ + 1) subscription streams open before ending each iteration;
    // the fact it shouldn't may or not be used as a simplifying assumption in
    // the loop's implementation.
    for (size_t trsc = 0; trsc < config_->trs_conns.size(); ++trsc) {
      if (agreeing_subset_members[most_agreed_block].count(trsc) < 1 && config_->trs_conns[trsc]->hasHashStream()) {
        LOG_DEBUG(logger_, "Close hash stream " << trsc << " after update " << update_id);
        config_->trs_conns[trsc]->cancelHashStream();
      }
    }
  }

  stop_subscription_thread_ = true;
}

void ThinReplicaClient::pushUpdateToUpdateQueue(std::unique_ptr<EventVariant> update,
                                                const std::chrono::steady_clock::time_point& start,
                                                bool is_event_group) {
  // update current queue size metric before pushing to the update_queue
  metrics_.current_queue_size.Get().Set(config_->update_queue->size());

  // push update to the update queue for consumption by the application using
  // TRC
  config_->update_queue->push(std::move(update));

  // update metrics
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  metrics_.update_dur_ms.Get().Set((uint64_t)(duration.count()));

  if (is_event_group) {
    metrics_.last_verified_event_group_id.Get().Set(latest_verified_event_group_id_);
  } else {
    metrics_.last_verified_block_id.Get().Set(latest_verified_block_id_);
  }

  // Reset read timeout, failure and ignored metrics before the next update
  resetMetricsBeforeNextUpdate();
}

void ThinReplicaClient::resetMetricsBeforeNextUpdate() {
  metrics_.updateAggregator();
  metrics_.read_timeouts_per_update.Get().Set(0);
  metrics_.read_failures_per_update.Get().Set(0);
  metrics_.read_ignored_per_update.Get().Set(0);
}

ThinReplicaClient::~ThinReplicaClient() {
  stop_subscription_thread_ = true;
  if (subscription_thread_) {
    ConcordAssert(subscription_thread_->joinable());
    subscription_thread_->join();
  }
}

void ThinReplicaClient::Subscribe() {
  ConcordAssert(config_->trs_conns.size() > 0);
  // XXX: The following implementation does not achieve Subscribe's specified
  //      interface and behavior (see the comments with Subscribe's declaration
  //      in the Thin Replica Client Library header file for documentation of
  //      that interface); this implementation is intended to establish minimal
  //      end-to-end connectivity with a non-faulty Thin Replica Server in order
  //      to preserve the general behavior of the example Thin Replica Client
  //      application (which at this time just connects to a server and checks
  //      the status returned for a Block read from data streamStateRequest).

  // Stop any existing subscription before trying to start a new one.
  stop_subscription_thread_ = true;
  if (subscription_thread_) {
    ConcordAssert(subscription_thread_->joinable());
    subscription_thread_->join();
    subscription_thread_.reset();
  }
  is_event_group_request_ = false;

  bool has_verified_state = false;
  size_t data_server_index = 0;
  list<unique_ptr<EventVariant>> state;
  uint64_t block_id = 0;

  while (!has_verified_state && (data_server_index < config_->trs_conns.size())) {
    state.clear();
    block_id = 0;
    list<string> update_hashes;
    bool received_state_invalid = false;

    LOG_DEBUG(logger_, "Read state from " << data_server_index);
    ReadStateRequest request;
    TrsConnection::Result stream_open_result = config_->trs_conns[data_server_index]->openStateStream(request);
    if (stream_open_result == TrsConnection::Result::kTimeout) {
      LOG_WARN(logger_,
               "While trying to fetch initial state for a subscription, "
               "ThinReplicaClient timed out an attempt to open a stream "
               "to read the initial state from a server (server index "
                   << data_server_index << ").");
      received_state_invalid = true;
    }
    if (stream_open_result == TrsConnection::Result::kFailure) {
      LOG_WARN(logger_,
               "While trying to fetch initial state for a subscription, "
               "ThinReplicaClient failed to open a stream to read the "
               "initial state from a server (server index "
                   << data_server_index << ").");
      received_state_invalid = true;
    }
    ConcordAssert(stream_open_result == TrsConnection::Result::kSuccess || received_state_invalid);

    Data response;
    TrsConnection::Result read_result = TrsConnection::Result::kUnknown;
    while (!received_state_invalid && (read_result = config_->trs_conns[data_server_index]->readState(&response)) ==
                                          TrsConnection::Result::kSuccess) {
      // ReadState is supported for legacy events only
      ConcordAssert(response.has_events());
      if ((state.size() > 0) && (response.events().block_id() < block_id)) {
        LOG_WARN(logger_,
                 "While trying to fetch initial state for a "
                 "subscription, ThinReplicaClient received an update "
                 "with a decreasing Block ID from a server (server index "
                     << data_server_index << ").");
        received_state_invalid = true;
      } else {
        block_id = response.events().block_id();
        auto update = std::make_unique<EventVariant>();
        auto& legacy_event = std::get<Update>(*update);
        legacy_event.block_id = block_id;
        legacy_event.correlation_id_ = response.events().correlation_id();
        for (int i = 0; i < response.events().data_size(); ++i) {
          const KVPair& kvp = response.events().data(i);
          legacy_event.kv_pairs.push_back(make_pair(kvp.key(), kvp.value()));
        }
        update_hashes.push_back(hashUpdate(*update));
        state.push_back(move(update));
      }
    }
    ConcordAssert(received_state_invalid || read_result == TrsConnection::Result::kFailure ||
                  read_result == TrsConnection::Result::kTimeout);
    if (read_result == TrsConnection::Result::kTimeout) {
      LOG_WARN(logger_,
               "While trying to fetch initial state for a subscription, "
               "ThinReplicaClient timed out an attempt to read an update "
               "from a state stream from a server (server index "
                   << data_server_index << ").");
      received_state_invalid = true;
    }

    TrsConnection::Result stream_close_result = config_->trs_conns[data_server_index]->closeStateStream();
    if (stream_close_result == TrsConnection::Result::kTimeout) {
      LOG_WARN(logger_,
               "While trying to fetch initial state for a subscription, "
               "ThinReplicaClient timed out an attempt to properly close "
               "a completed state stream from a server (server index: "
                   << data_server_index << ").");
      received_state_invalid = true;
    }
    if (stream_close_result == TrsConnection::Result::kFailure) {
      LOG_WARN(logger_,
               "While trying to fetch initial state for a subscription, "
               "ThinReplicaClient failed to properly close a completed "
               "state stream from a server (server index: "
                   << data_server_index << ").");
      received_state_invalid = true;
    }
    ConcordAssert(stream_close_result == TrsConnection::Result::kSuccess || received_state_invalid);

    LOG_DEBUG(logger_, "Got initial state from " << data_server_index);

    // We count the server we got the initial state data from as the first of
    // (max_faulty + 1) servers we need to find agreeing upon this state in
    // order to accept it.
    size_t agreeing_servers = 1;
    size_t hash_server_index = 0;
    string expected_hash;
    if (!received_state_invalid) {
      expected_hash = hashState(update_hashes);
    }
    while (!received_state_invalid && (hash_server_index < config_->trs_conns.size()) &&
           (agreeing_servers <= (size_t)config_->max_faulty)) {
      if (hash_server_index == data_server_index) {
        ++hash_server_index;
        continue;
      }
      LOG_DEBUG(logger_, "Read state hash from " << hash_server_index);
      Hash hash_response;
      ReadStateHashRequest hash_request;
      hash_request.mutable_events()->set_block_id(block_id);
      TrsConnection::Result read_hash_result =
          config_->trs_conns[hash_server_index]->readStateHash(hash_request, &hash_response);
      hash_server_index++;

      // Check whether the hash came back with an ok status, matches the Block
      // ID we requested, and matches the hash we computed locally of the data,
      // and only count it as agreeing if we complete all this verification.
      if (read_hash_result == TrsConnection::Result::kTimeout) {
        LOG_WARN(logger_,
                 "ThinReplicaClient timed out a call to ReadStateHash to server "
                     << hash_server_index - 1 << " (requested Block ID: " << block_id << ").");
        continue;
      }
      if (read_hash_result == TrsConnection::Result::kFailure) {
        LOG_WARN(logger_,
                 "Server " << hash_server_index - 1
                           << " gave error response to ReadStateHash (requested Block ID: " << block_id << ").");
        continue;
      }
      ConcordAssert(read_hash_result == TrsConnection::Result::kSuccess);
      if (hash_response.events().block_id() != block_id) {
        LOG_WARN(logger_,
                 "Server " << hash_server_index - 1
                           << " gave response to ReadStateHash disagreeing "
                              "with requested Block ID (requested Block ID: "
                           << block_id << ", response contained Block ID: " << hash_response.events().block_id()
                           << ").");
        continue;
      }
      if (hash_response.events().hash() != expected_hash) {
        LOG_WARN(logger_,
                 "Server " << hash_server_index - 1
                           << " gave response to ReadStateHash in disagreement "
                              "with the expected hash value (requested Block ID: "
                           << block_id << ").");
        continue;
      }

      ++agreeing_servers;
    }
    if (!received_state_invalid && (agreeing_servers > config_->max_faulty)) {
      has_verified_state = true;
    }

    ++data_server_index;
  }
  if (!has_verified_state) {
    throw runtime_error(
        "Could not start ThinReplicaClient subscription: Failed to find a set "
        "of at least " +
        to_string((uint32_t)config_->max_faulty + 1) +
        " responding Thin Replica Client Servers in agreement about what the "
        "initial state should be.");
  }

  LOG_DEBUG(logger_, "Got verified initial state for block " << block_id);

  config_->update_queue->clear();
  while (state.size() > 0) {
    config_->update_queue->push(move(state.front()));
    state.pop_front();
  }
  latest_verified_block_id_ = block_id;
  // Create and launch thread to stream updates from the servers and push them
  // into the queue.
  stop_subscription_thread_ = false;
  subscription_thread_.reset(new thread(&ThinReplicaClient::receiveUpdates, this));
}

void ThinReplicaClient::Subscribe(uint64_t block_id) {
  // Stop any existing subscription before trying to start a new one.
  stop_subscription_thread_ = true;
  if (subscription_thread_) {
    ConcordAssert(subscription_thread_->joinable());
    subscription_thread_->join();
    subscription_thread_.reset();
  }

  config_->update_queue->clear();
  latest_verified_block_id_ = block_id > 0 ? block_id - 1 : block_id;
  latest_verified_event_group_id_ = 0;
  is_event_group_request_ = false;

  // Create and launch thread to stream updates from the servers and push them
  // into the queue.
  stop_subscription_thread_ = false;
  subscription_thread_.reset(new thread(&ThinReplicaClient::receiveUpdates, this));
}

void ThinReplicaClient::Subscribe(const SubscribeRequest& req) {
  // Stop any existing subscription before trying to start a new one.
  stop_subscription_thread_ = true;
  if (subscription_thread_) {
    ConcordAssert(subscription_thread_->joinable());
    subscription_thread_->join();
    subscription_thread_.reset();
  }

  config_->update_queue->clear();
  // We assume that the latest known event group for the caller is the event group prior to the requested one.
  latest_verified_event_group_id_ = req.event_group_id > 0 ? req.event_group_id - 1 : req.event_group_id;
  is_event_group_request_ = true;

  // Create and launch thread to stream updates from the servers and push them
  // into the queue.
  stop_subscription_thread_ = false;
  subscription_thread_.reset(new thread(&ThinReplicaClient::receiveUpdates, this));
}

// This is a placeholder implementation as the Unsubscribe gRPC call is not yet
// implemented on the server side.
//
// TODO (Alex):
//     - Add lines to actually send an unsubscription one the Thin Replica
//       Server supports receiving it.
//     - Add logic for signing the unsubscription once the signature scheme is
//       defined.
//     - Add logic to pick a different server to send the acknowledgement to if
//       server 0 is known to be down or faulty.
void ThinReplicaClient::Unsubscribe() {
  LOG_DEBUG(logger_, "Unsubscribe");
  stop_subscription_thread_ = true;
  if (subscription_thread_) {
    ConcordAssert(subscription_thread_->joinable());
    subscription_thread_->join();
    subscription_thread_.reset();
  }

  size_t server_to_send_unsubscription_to = 0;
  ConcordAssert(config_->trs_conns.size() > server_to_send_unsubscription_to);
  ConcordAssertNE(config_->trs_conns[server_to_send_unsubscription_to], nullptr);
}

// This is a placeholder implementation as the AckUpdate gRPC call is not yet
// implemented on the server side.
//
// TODO (Alex):
//     - Add lines to actually send message once the Thin Replica Server
//       supports receiving it.
//     - Add logic for signing the acknowledgement once the signature scheme is
//       defined.
//     - Add logic to pick a different server to send the acknowledgement to if
//       server 0 is known to be down or faulty.
void ThinReplicaClient::AcknowledgeBlockID(uint64_t block_id) {
  BlockId AckMessage;
  AckMessage.set_block_id(block_id);

  size_t server_to_acknowledge_to = 0;
  ConcordAssert(config_->trs_conns.size() > server_to_acknowledge_to);
  ConcordAssertNE(config_->trs_conns[server_to_acknowledge_to], nullptr);
}

}  // namespace client::thin_replica_client
