// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "status.hpp"
#include "sliver.hpp"
#include "communication/ICommunication.hpp"
#include "Metrics.hpp"
#include "storage/db_interface.h"
#include "db_interfaces.h"
#include "bftengine/Replica.hpp"
#include "kv_types.hpp"

namespace concord::kvbc {

using concordUtils::Status;
using concordUtils::Sliver;

// forward declarations
class ICommandsHandler;

struct ClientConfig {
  // F value - max number of faulty/malicious replicas. fVal >= 1
  uint16_t fVal;

  // C value. cVal >=0
  uint16_t cVal;

  // unique identifier of the client.
  // clientId should also represent this client in ICommunication.
  // In the current version, replicaId should be a number between N and
  // N+numOfClientProxies-1 (N is the number replicas in the system.
  // numOfClientProxies is part of the replicas' configuration)
  uint16_t clientId;
};

/////////////////////////////////////////////////////////////////////////////
// Client proxy
/////////////////////////////////////////////////////////////////////////////

// Represents a client of the blockchain database
class IClient {
 public:
  virtual ~IClient() = default;
  virtual Status start() = 0;
  virtual Status stop() = 0;

  virtual bool isRunning() = 0;

  virtual Status invokeCommandSynch(const char* request,
                                    uint32_t requestSize,
                                    uint8_t flags,
                                    std::chrono::milliseconds timeout,
                                    uint32_t replySize,
                                    char* outReply,
                                    uint32_t* outActualReplySize,
                                    const std::string& cid = "",
                                    const std::string& span_context = "") = 0;

  virtual void setMetricsAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) = 0;
};

// creates a new Client object
IClient* createClient(const ClientConfig& conf, bft::communication::ICommunication* comm);

// TODO: Implement:
//  // deletes a Client object
//  void release(IClient* r);

/////////////////////////////////////////////////////////////////////////////
// Replica
/////////////////////////////////////////////////////////////////////////////

// Represents a replica of the blockchain database
class IReplica {
 public:
  virtual Status start() = 0;
  virtual Status stop() = 0;
  virtual ~IReplica() = default;

  enum class RepStatus  // status of the replica
  { UnknownError = -1,
    Ready = 0,
    Starting,
    Running,
    Stopping,
    Idle };

  // returns the current status of the replica
  virtual RepStatus getReplicaStatus() const = 0;

  virtual bool isRunning() const = 0;

  /*
   * TODO(GG): Implement:
   *  virtual Status setStatusNotifier(StatusNotifier statusNotifier);
   */

  // Used to read from storage, only when a replica is Idle. Useful for
  // initialization and maintenance.
  virtual const concord::kvbc::ILocalKeyValueStorageReadOnly& getReadOnlyStorage() = 0;

  // Used to append blocks to storage, only when a replica is Idle. Useful
  // for initialization and maintenance.
  virtual Status addBlockToIdleReplica(const concord::kvbc::SetOfKeyValuePairs& updates) = 0;

  /// TODO(IG) the following methods are probably temp solution,
  /// need to split interfaces implementations to differrent modules
  /// instead of being all implemented bt ReplicaImpl
  virtual void set_command_handler(ICommandsHandler* handler) = 0;
};

/////////////////////////////////////////////////////////////////////////////
// Replica's commands handle
/////////////////////////////////////////////////////////////////////////////

class ICommandsHandler : public bftEngine::IRequestsHandler {
 public:
  int execute(uint16_t clientId,
              uint64_t sequenceNum,
              uint8_t flags,
              uint32_t requestSize,
              const char* request,
              uint32_t maxReplySize,
              char* outReply,
              uint32_t& outActualReplySize,
              uint32_t& outActualReplicaSpecificInfoSize,
              concordUtils::SpanWrapper& span) override = 0;
  virtual void setControlStateManager(std::shared_ptr<bftEngine::ControlStateManager> controlStateManager) = 0;
  ~ICommandsHandler() override = default;
};

}  // namespace concord::kvbc
