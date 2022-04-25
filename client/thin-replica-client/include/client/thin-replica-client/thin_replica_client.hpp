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
//
// Primary Thin Replica Client Library header file; you should include this file
// to use the Thin Replica Client Library.
//
// Some general notes about the Thin Replica Client Library:
//  - std::string is used for passing byte strings to and from the library; this
//    is considered a cleaner solution than passing pointer/length pairs.
//    Note std::string is appropriate for byte strings because it is defined to
//    be std::basic_string<char> and chars are guaranteed to be one byte.
//  - The thin replica client mechanism associates each update with a specific
//    Block ID. Block IDs are 64-bit unsigned integer quantities. Updates a Thin
//    Replica Client receives from the Thin Replica Servers will have
//    monotonically increasing block numbers. An application using the
//    ThinReplicaClient Library should persist at least the block number for the
//    most recent update it received, as that block number can be used to resume
//    a subscription without having to first stream all the non-pruned updates
//    preceding that block number. An application using the ThinReplicaClient
//    library should use the ThinReplicaClient::AcknowledgeBlock to acknowledge
//    the block ID for each update it receives, as this is useful to the thin
//    replica servers for pruning decisions.
//  - It should be noted that the ThinReplicaClient itself logs via log4cplus;
//    specifically, it will log any noteworthy abnormalities it observes but
//    which it is its job to abstract out for the application using the library
//    rather than programatically expose to the application (ex: disagreement
//    among Thin Replica Servers).

#ifndef THIN_REPLICA_CLIENT_HPP_
#define THIN_REPLICA_CLIENT_HPP_

#include "thin_replica.pb.h"
#include "grpc_connection.hpp"
#include "assertUtils.hpp"
#include "Metrics.hpp"

#include <opentracing/span.h>
#include <condition_variable>
#include <thread>
#include "Logger.hpp"
#include "client/concordclient/event_update.hpp"
#include "client/concordclient/concord_client_exceptions.hpp"

namespace client::thin_replica_client {

// For the life time of an instance of this object we store the correlation ID
// in the loggers context. Log messages are composed from this context together
// with a log message. For the CID, it is important to be part of the loggers
// context so that it gets assigned to the correct field in the log line.
// Later, fluentd will parse log lines and assumes CIDs at a certain position.
// This inforamtion is then propagated to LogInsight. As the current log line
// format assumes at most 1 CID, there should not exist more than 1 LocCid
// object at a time.
class LogCid final {
 public:
  LogCid(const std::string& cid);
  ~LogCid();

  // As only one LogCid is allowed to exist at a time, copying or moving it is
  // not supported.
  LogCid(const LogCid& other) = delete;
  LogCid(const LogCid&& other) = delete;
  LogCid& operator=(const LogCid& other) = delete;
  LogCid& operator=(const LogCid&& other) = delete;

 private:
  const static std::string cid_key_;
  static std::atomic_bool cid_set_;
};

// Configuration for Thin Replica Client.
struct ThinReplicaClientConfig {
  // client_id is a byte string, at most 256 bytes in length, containing only
  // characters matching the regexp [a-zA-Z0-9._:-#/ ]. Client IDs may also be
  // referred as TRID (thin replica ID) or participant ID. A client ID typically
  // uniquely identfies a participant group if participant grouping is enabled,
  // say for hot standby failover. Otherwise it uniquely identifies a
  // participant.
  std::string client_id;
  // update_queue is a shared pointer to an UpdateQueue object to be used to
  // transfer updates from this ThinReplicaClient to the application. Note
  // ThinRepliaClient guarantees it will only use the Clear and Push functions
  // of this queue; the ThinReplicaClient will never call Pop, TryPop,
  // ReleaseConsumers, or ReEnableConsumers. Furthermore, a ThinReplicaClient
  // guarantees it will never execute the Clear or Push functions of the queue
  // after that ThinReplicaClient's destructor has returned.
  std::shared_ptr<concord::client::concordclient::EventUpdateQueue> update_queue;
  // max_faulty is the maximum number of simultaneously Byzantine-faulty servers
  // that must be tolerated (this is equivalent to the F value for the Concord
  // cluster the servers are from).
  std::size_t max_faulty;
  // trs_conns is a vector of connection objects. Each representing a direct
  // connection from this TRC to a specific Thin Replica Server.
  std::vector<std::shared_ptr<client::concordclient::GrpcConnection>>& trs_conns;
  // the time duration the TRC waits before printing warning logs when
  // responsive agreeing servers are less than config_->max_faulty + 1
  std::chrono::seconds no_agreement_warn_duration;

  ThinReplicaClientConfig(std::string client_id_,
                          std::shared_ptr<concord::client::concordclient::EventUpdateQueue> update_queue_,
                          std::size_t max_faulty_,
                          std::vector<std::shared_ptr<client::concordclient::GrpcConnection>>& trs_conns_,
                          std::chrono::seconds no_agreement_warn_duration_ = kNoAgreementWarnDuration)
      : client_id(std::move(client_id_)),
        update_queue(update_queue_),
        max_faulty(max_faulty_),
        trs_conns(trs_conns_),
        no_agreement_warn_duration(no_agreement_warn_duration_) {}

 private:
  static constexpr std::chrono::seconds kNoAgreementWarnDuration = 60s;
};

// TRC metrics
struct ThinReplicaClientMetrics {
  ThinReplicaClientMetrics()
      : metrics_component_{"ThinReplicaClient", std::make_shared<concordMetrics::Aggregator>()},
        read_timeouts_per_update{metrics_component_.RegisterGauge("read_timeouts_per_update", 0)},
        read_failures_per_update{metrics_component_.RegisterGauge("read_failures_per_update", 0)},
        read_ignored_per_update{metrics_component_.RegisterGauge("read_ignored_per_update", 0)},
        current_queue_size{metrics_component_.RegisterGauge("current_queue_size", 0)},
        last_verified_block_id{metrics_component_.RegisterGauge("last_verified_block_id", 0)},
        last_verified_event_group_id{metrics_component_.RegisterGauge("last_verified_event_group_id", 0)},
        update_dur_us{metrics_component_.RegisterGauge("update_dur_us", 0)},
        num_updates_processed{metrics_component_.RegisterCounter("num_updates_processed", 0)} {
    metrics_component_.Register();
  }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    metrics_component_.SetAggregator(aggregator);
  }

  void updateAggregator() { metrics_component_.UpdateAggregator(); }

 private:
  concordMetrics::Component metrics_component_;

 public:
  // read_timeouts_per_update - the number of times data/hash streams timeouts
  // per update
  concordMetrics::GaugeHandle read_timeouts_per_update;
  // read_failures_per_update - the number of times data/hash streams fails per
  // update
  concordMetrics::GaugeHandle read_failures_per_update;
  // read_ignored_per_update - the number of times data/hash stream
  // updates/hashes are ignored
  concordMetrics::GaugeHandle read_ignored_per_update;
  // current_queue_size - the current size of the update queue i.e., number of
  // updates in the update_queue
  concordMetrics::GaugeHandle current_queue_size;
  // last_verified_*_id - block or event group ID of the latest update verified by TRC
  concordMetrics::GaugeHandle last_verified_block_id;
  concordMetrics::GaugeHandle last_verified_event_group_id;
  // update_dur_us - duration of time (microseconds) between when an update is received by
  // the TRC, to when it is pushed to the update queue for consumption by the
  // application using TRC
  concordMetrics::GaugeHandle update_dur_us;
  // number of updates pushed by TRC to the update queue per second
  concordMetrics::CounterHandle num_updates_processed;
};

struct SubscribeRequest {
  uint64_t event_group_id;
};

// Thin Replica Client implementation; used to subscribe to and stream updates
// from thin replica servers. Note the ThinReplicaClient is intended to
// error-handle Byzantine failures among the thin replica servers; a
// ThinReplicaClient object will not accept an update unless it can find at
// least (max_faulty + 1) servers in agreement about the update. The
// ThinReplicaClient should also handle moving its subscription away from faulty
// servers in the event a server becomes faulty (assuming some set of
// (max_faulty + 1) agreeing servers still exists); this fail-over process
// should be automatic and transparent to code consuming the ThinReplicaClient.
class ThinReplicaClient final {
 public:
  ThinReplicaClientMetrics metrics_;

 private:
  logging::Logger logger_;
  std::unique_ptr<ThinReplicaClientConfig> config_;
  size_t data_conn_index_;

  bool is_event_group_request_;
  uint64_t latest_verified_block_id_;
  uint64_t latest_verified_event_group_id_;
  // a subscription is said to be successful when TRC receives the first verified update.
  bool is_subscription_successful_;

  std::unique_ptr<std::thread> subscription_thread_;
  std::atomic_bool stop_subscription_thread_;

  // Thread function to start subscription_thread_ with.
  void receiveUpdates();

  // wrapper around receiveUpdates to set exceptions_ptr in update_queue
  void receiveUpdatesWrapper();

  // Store call to the function that exposes and updates internal TRC metrics
  // to the user of the TRC library
  std::function<void(const ThinReplicaClientMetrics&)> onSetMetricsCallbackFunc;

  // Push update to update queue for consumption by the application using TRC.
  // Set TRC metrics before receiving next update
  void pushUpdateToUpdateQueue(std::unique_ptr<concord::client::concordclient::EventVariant> update,
                               const std::chrono::steady_clock::time_point& start,
                               bool is_event_group);

  // Reset metrics before next update
  void resetMetricsBeforeNextUpdate();

  struct HashRecord {
    enum Type { EventGroup, LegacyEvent };
    Type type;
    uint64_t id;
    std::string hash;
  };
  struct CompareHashRecord {
    bool operator()(const HashRecord& lhs, const HashRecord& rhs) const {
      if (lhs.type == rhs.type) {
        return lhs.id < rhs.id || (lhs.id == rhs.id && lhs.hash < rhs.hash);
      }
      if (lhs.type == HashRecord::Type::LegacyEvent) {
        // LegacyEvents are considered "less than" EventGroups due to the order in which they got introduced
        // Thereby, we assume that replicas will never stream LegacyEvents "after" EventGroups.
        return true;
      }
      return false;
    }
  };

  // Map recording every HashRecord we have seen for the update we are
  // seeking and which servers support that pair. Note we choose a map over an
  // unordered_map here as std does not seem to provide a hash implementation
  // for std::pair. The sets of servers supporting each pair are represented as
  // unordered sets of server indexes.
  using HashRecordMap = std::map<HashRecord, std::unordered_set<size_t>, CompareHashRecord>;

  using SpanPtr = std::unique_ptr<opentracing::Span>;
  std::pair<client::concordclient::GrpcConnection::Result, SpanPtr> readBlock(
      com::vmware::concord::thin_replica::Data& update_in,
      HashRecordMap& agreeing_subset_members,
      size_t& most_agreeing,
      HashRecord& most_agreed_block,
      std::unique_ptr<LogCid>& cid);

  // Opens hash streams to all the replicas and tries to read hash updates
  // from opened streams to check for maximal agreement.
  // If none of the replicas return a hash update i.e., the connections to
  // all the replicas either time out or fail while waiting for an update,
  // findBlockHashAgreement returns false, otherwise returns true.
  void findBlockHashAgreement(std::vector<bool>& servers_tried,
                              HashRecordMap& agreeing_subset_members,
                              size_t& most_agreeing,
                              HashRecord& most_agreed_block,
                              SpanPtr& parent_span,
                              size_t& servers_out_of_range,
                              size_t& servers_pruned);

  bool rotateDataStreamAndVerify(com::vmware::concord::thin_replica::Data& update_in,
                                 HashRecordMap& agreeing_subset_members,
                                 HashRecord& most_agreed_block,
                                 SpanPtr& parent_span,
                                 std::unique_ptr<LogCid>& cid);

  client::concordclient::GrpcConnection::Result resetDataStreamTo(size_t server_idx);
  client::concordclient::GrpcConnection::Result startHashStreamWith(size_t server_idx);
  void closeAllHashStreams();

  // Helper functions to receiveUpdates.
  void logDataStreamResetResult(const client::concordclient::GrpcConnection::Result& result, size_t server_index);
  void recordCollectedHash(size_t update_source,
                           bool is_event_group,
                           uint64_t id,
                           const std::string& update_hash,
                           HashRecordMap& server_indexes_by_reported_update,
                           size_t& maximal_agreeing_subset_size,
                           HashRecord& maximally_agreed_on_update);

  // Returns true if a hash update is received from a hash stream, returns false otherwise
  bool readUpdateHashFromStream(size_t server_index,
                                HashRecordMap& server_indexes_by_reported_update,
                                size_t& maximal_agreeing_subset_size,
                                HashRecord& maximally_agreed_on_update,
                                size_t& servers_out_of_range,
                                size_t& servers_pruned);

 public:
  // Constructor for ThinReplicaClient. Note that, as the ThinReplicaClient
  // protocol allows only one active subscription at a time for a given client,
  // the ThinReplicaClient library only allows the use of a single
  // ThinReplicaClient at a time; an exception will be thrown if  this
  // constructor is called when there already exists a ThinReplicaClient in the
  // calling process that has been constructed but not destructed.
  // The constructor takes unique_ptr to ThinReplicaClientConfig struct as a
  // parameter. See ThinReplicaClientConfig's definition for description of
  // configuration parameters accepted by TRC.
  ThinReplicaClient(std::unique_ptr<ThinReplicaClientConfig> config,
                    const std::shared_ptr<concordMetrics::Aggregator>& aggregator)
      : metrics_(),
        logger_(logging::getLogger("concord.client.thin_replica")),
        config_(std::move(config)),
        data_conn_index_(0),
        latest_verified_block_id_(0),
        latest_verified_event_group_id_(0),
        is_subscription_successful_(false),
        subscription_thread_(),
        stop_subscription_thread_(false) {
    metrics_.setAggregator(aggregator);
    if (config_->trs_conns.size() < (3 * (size_t)config_->max_faulty + 1)) {
      size_t num_servers = config_->trs_conns.size();
      config_->update_queue.reset();
      throw std::invalid_argument("Too few servers (" + std::to_string(num_servers) +
                                  ") given to ThinReplicaClient constructor to tolerate requested "
                                  "maximum faulty servers (" +
                                  std::to_string(config_->max_faulty) +
                                  "). The number of servers must be at least (3 * max_faulty + 1).");
    }

    // TODO (Alex): Enforce that, as far as this constructor can see (likely the
    //              virtual memory for the process it is running in), only one
    //              ThinReplicaClient is allowed to exist at a time.
  }

  // Destructor for ThinReplicaClient. Calling this destructor may block the
  // calling thread as necessary to stop and join worker thread(s) owned by the
  // ThinReplicaClient if the ThinReplicaClient has an active subscription.
  //
  // Any active subscription will be stopped in the process of destroying the
  // ThinReplicaClient, but destroying a ThinReplicaClient will not cause it to
  // request a final cancellation of that subscription with the Thin Replica
  // Server(s). An application can later resume that subscription by
  // constructing a new ThinReplicaClient and calling
  // ThinReplicaClient::Subscribe. If an application has no intention of later
  // resuming a subscription, it should call ThinReplicaClient::Unsubscribe
  // before destroying the ThinReplicaClient to inform the Thin Replica
  // Server(s) that this subscripiton is being cancelled.
  ~ThinReplicaClient();

  // Copying or moving of a ThinReplicaClient object is explicitly disallowed as
  // only 1 ThinReplicaClient at a time is allowed to exist by this library.
  ThinReplicaClient(const ThinReplicaClient& other) = delete;
  ThinReplicaClient(const ThinReplicaClient&& other) = delete;
  ThinReplicaClient& operator=(const ThinReplicaClient& other) = delete;
  ThinReplicaClient& operator=(const ThinReplicaClient&& other) = delete;

  // Subscribe to updates from the Thin Replica Servers. If a value for block_id
  // is given, the ThinReplicaClient will begin the subscription at and including
  // that Block ID, otherwise, subscription will begin by attempting to read all
  // current state.
  //
  // If no Block ID is given and the Thin Replica mechanism begins the
  // subscription procedure by fetching of initial state, the Subscribe call
  // will block until the initial state has been fetched; furthermore, Subscribe
  // may throw an exception in this case if it cannot reach enough agreeing
  // servers to collect the initial state. The updates contained in the initial
  // state will all be synchronously pushed to the UpdateQueue this
  // ThinReplicaClient was constructed with before the Subscribe function
  // returns.
  //
  // Once a subscription has fetched all initial state (if no Block ID was
  // given) or resumed an existing subscription from a given Block ID, each
  // update received via the stream will be asynchronously pushed to the
  // UpdateQueue (these asynchronous updates may begin before the Subscribe
  // function returns and may continue after it returns until the Unsubscribe
  // function is called and returned, Subscribe is called again to create
  // another subscription and returns, or the ThinReplicaClient object is
  // completely destoyed).
  //
  // It is expected that Thin Replica Client applications will make a reasonable
  // effort to call AcknowledgeBlockID for the most recent Block ID they have
  // received when they receive new update(s); please see
  // ThinReplicaClient::AcknowledgeBlockID's comments in this header file for
  // details.
  //
  // If this ThinReplicaClient already has an active subscription open when
  // Subscribe is called, that subscription may be ended when Subscribe is
  // called, and will always be ended before Subscribe returns if no error
  // occurs. If there are any updates leftover in update_queue when Subscribe is
  // called, the queue will be cleared.
  void Subscribe();
  void Subscribe(uint64_t block_id);
  void Subscribe(const SubscribeRequest&);

  // End any currently open subscription this ThinReplicaClient has; this will
  // stop any worker thread(s) this ThinReplicaClient has for maintaining this
  // subscription, close connection(s) to the Thin Replica Server(s) specific to
  // this subscription, and send message(s) informing the server(s) that this
  // subscription is being permanently cancelled (the cancellation may be
  // considered by the server(s) in pruning decisions). For this reason,
  // ThinReplicaClient::Unsubscribe should not be used to temporarilly stop
  // subscriptions that the application intends to resume soon. (Note destroying
  // the ThinReplicaClient object can be used to stop the subscriptions it has
  // without telling the server(s) that the subscription is being permanently
  // cancelled).
  //
  // Note this function will not automatically clear any updates still in the
  // update_queue. Unsubscribe may block the thread calling it as necessary to
  // stop and join worker thread(s) created by the ThinReplicaClient for the
  // subscription being terminated. Note Unsubscribe will effectively do nothing
  // if called for a ThinReplicaClient that has no active subscription.
  void Unsubscribe();

  // Acknowledge receipt of the update for a given Block ID to the Thin Replica
  // Servers. Thin Replica Client applications should make a reasonable effort
  // to call this function every time they receive, process, and persist changes
  // from new update(s).
  //
  // If any form of blockchain pruning is supported by the Concord cluster
  // containing the Thin Replica Servers to which this ThinReplicaClient
  // subscribes, AcknowledgeBlockID may inform those servers that this Thin
  // Replica Client has received and (as applicable) persisted the update
  // referenced by block_id; the Concord cluster may use this information in its
  // decisions about state to prune. For this reason, AcknowledgeBlockID must be
  // called strictly after the Thin Replica Client application has made
  // sufficient changes to its persisted state to recover from a crash or other
  // restart without needing to retrieve the acknowledged update from the Thin
  // Replica Server(s).
  //
  // Note that, as Block IDs are monotonically increasing with respect to the
  // order updates are sent and received, acknowledgement of one Block ID may be
  // taken to imply receipt and persistence of all preceding updates. For this
  // reason, an application that processes and persists updates out of order
  // should still acknowledge updates only in order; furthermore, in
  // applications where multiple sequential updates are received and processed
  // at once, it is sufficient to only acknowledge the latest update of a batch.
  void AcknowledgeBlockID(uint64_t block_id);

  // Register the callback to update external metrics
  void setMetricsCallback(const std::function<void(const ThinReplicaClientMetrics&)>& exposeAndSetMetrics);
};

}  // namespace client::thin_replica_client

#endif  // THIN_REPLICA_CLIENT_HPP_
