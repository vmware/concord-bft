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

#include "PrimitiveTypes.hpp"
#include "TimeUtils.hpp"
#include "bftengine/ReservedPagesClient.hpp"
#include "Metrics.hpp"
#include "IPendingRequest.hpp"
#include "bftengine/IKeyExchanger.hpp"
#include "PersistentStorage.hpp"
#include <map>
#include <set>
#include <unordered_map>
#include <memory>

namespace bftEngine {
class IStateTransfer;

namespace impl {
class ClientReplyMsg;
class ClientRequestMsg;

// Keeps track of Client IDs, public keys, and pending requests and replies. Supports saving and loading client public
// keys and pending reply messages to the reserved pages mechanism.
//
// Not thread-safe.
class ClientsManager : public ResPagesClient<ClientsManager>, public IPendingRequest, public IClientPublicKeyStore {
 public:
  // As preconditions to this constructor:
  //   - The ReplicaConfig singleton (i.e. ReplicaConfig::instance()) must be initialized with the relevant
  //     configuration.
  //   - The reserved pages mechanism must be initialized and usable.
  //   - The global logger CL_MNGR must be initialized.
  // Behavior is undefined if any of these preconditions are not met. Behavior is also undefined if proxyClients,
  // externalClients, and internalClients are all empty.
  // Additionally, all current and future behavior of the constructed ClientsManager object becomes undefined if any of
  // the following conditions occur:
  //   - The reserved pages mechanism stops being usable.
  //   - The concordMetrics::Component object referenced by metrics is destroyed.
  //   - The global logger CL_MNGR is destroyed.
  ClientsManager(const std::set<NodeIdType>& proxyClients,
                 const std::set<NodeIdType>& externalClients,
                 const std::set<NodeIdType>& internalClients,
                 concordMetrics::Component& metrics);
  ClientsManager(std::shared_ptr<PersistentStorage> ps,
                 const std::set<NodeIdType>& proxyClients,
                 const std::set<NodeIdType>& externalClients,
                 const std::set<NodeIdType>& internalClients,
                 concordMetrics::Component& metrics);

  uint32_t numberOfRequiredReservedPages() const { return clientIds_.size() * reservedPagesPerClient_; }

  // Loads any available client public keys and client replies from the reserved pages. Automatically deletes the oldest
  // reply record for a client if a reply message is found in the reserved pages for that client but the ClientsManager
  // already has a number of reply records for that client equalling or exceeding the maximum client batch size that was
  // configured at the time of this ClientManager's construction (or 1 if client batching was disabled). If the
  // ClientsManager already has existing reply records matching the client ID and sequence number of a reply found in
  // the reserved pages, the existing record will be overwritten. Automatically deletes any request records for a given
  // client with sequence numbers less than or equal to the sequence number of a reply to that client found in the
  // reserved pages. Behavior is undefined if the applicable reserved pages contain malformed data.
  void loadInfoFromReservedPages();

  // Replies

  // Returns true if clientId belongs to a valid client and this ClientsManager currently has a record for a reply to
  // that client with ID reqSeqNum. Returns false otherwise.
  // TODO(GG): make sure that ReqId is based on time (and ignore requests with time that does
  // not make sense (too high) - this will prevent some potential attacks)
  bool hasReply(NodeIdType clientId, ReqId reqSeqNum) const;

  bool isValidClient(NodeIdType clientId) const { return clientIds_.find(clientId) != clientIds_.end(); }

  // First, if this ClientsManager has a number of reply records for the given clientId equalling or exceeding the
  // maximum client batch size configured at the time of this ClientManager's construction (or 1 if client batching was
  // not enabled), deletes the oldest such record. Then, a ClientReplyMsg is allocated with the given sequence number
  // and payload, and a copy of the message is saved to the reserved pages, and this ClientManager adds a record for
  // this reply (potentially replacing any existing record for the given sequence number). Returns the allocated
  // ClientReplyMsg. Behavior is undefined for all of the following cases:
  // - clientId does not belong to a valid client.
  // - The number of reply records this ClientsManager has for the given client is above the maximum even after the
  //   oldest one is deleted.
  // - The size of the allocated reply message exceeds the maximum reply size that was configured at the time of this
  //   ClientsManager's construction.
  std::unique_ptr<ClientReplyMsg> allocateNewReplyMsgAndWriteToStorage(NodeIdType clientId,
                                                                       ReqId requestSeqNum,
                                                                       uint16_t currentPrimaryId,
                                                                       char* reply,
                                                                       uint32_t replyLength,
                                                                       uint32_t rsiLength);

  // Loads a client reply message from the reserved pages, and allocates and returns a ClientReplyMsg containing the
  // loaded message. Returns a null pointer if the configuration recorded at the time of this ClientManager's
  // construction enabled client batching with a maximum batch size greater than 1 and the message loaded from the
  // reserved pages has a sequence number not matching requestSeqNum. Behavior is undefined for all of the following
  // cases:
  // - clientId does not belong to a valid client.
  // - The reserved pages do not contain client reply message data of the expected format for clientId.
  // - The configuration recorded at the time of this ClientsManager's construction did not enable client batching or
  //   enabled it with a maximum batch size of 1, but the sequence number of the reply loaded from the reserved pages
  //   does not match requestSeqNum.
  std::unique_ptr<ClientReplyMsg> allocateReplyFromSavedOne(NodeIdType clientId,
                                                            ReqId requestSeqNum,
                                                            uint16_t currentPrimaryId);

  // Requests

  // Returns true if there is a valid client with ID clientId and this ClientsManager currently has a recorded request
  // with ID reqSeqNum from that client; otherwise returns false.
  bool isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) const;

  // Returns true IFF there is no pending requests for clientId, and reqSeqNum can become the new pending request, that
  // is, if all of the following are true:
  // - clientId belongs to a valid client.
  // - The number of requests this ClientsManager currently has recorded for that client is not exactly equal to the
  //   maximum client batch size configured at the time of this ClientsManager's construction (or 1 if client batching
  //   was not enabled).
  // - This ClientsManager currently has any request or reply associated with that client recorded with ID matching
  //   reqSeqNum.
  // otherwise returns false.
  bool canBecomePending(NodeIdType clientId, ReqId reqSeqNum) const;

  // Returns true if there is a valid client with ID clientId, this ClientsManager currently has a recorded request with
  // ID reqSeqNum from that client, and that request has not been marked as committed; otherwise returns false.
  bool isPending(NodeIdType clientId, ReqId reqSeqNum) const override;

  // Adds a record for the request with reqSeqNum from the client with the given clientId (if a record for that request
  // does not already exist). Behavior is undefined if clientId does not belong to a valid client.
  void addPendingRequest(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid);

  // Mark a request with ID reqSequenceNum that this ClientsManager currently has recorded as committed (does nothing if
  // there is no existing record for reqSequenceNum). Behavior is undefined if clientId does not belong to a valid
  // client.
  void markRequestAsCommitted(NodeIdType clientId, ReqId reqSequenceNum);

  // Removes the current request record from the given client with the greatest sequence number if both of the following
  // are true:
  // - That greatest sequence number is greater than reqSequenceNum.
  // - The number of requests this ClientsManager currently has recorded for the given client is exactly equal to the
  //   maximum client batch size configured at the time of this ClientManager's construction (or 1 if client batching
  //   was not enabled).
  // Does nothing otherwise. Behavior is undefined if clientId does not belong to a valid client.
  void removeRequestsOutOfBatchBounds(NodeIdType clientId, ReqId reqSequenceNum);

  // If clientId belongs to a valid client and this ClientsManager currently has a request recorded with reqSeqNum,
  // removes the record for that request. Does nothing otherwise.
  void removePendingForExecutionRequest(NodeIdType clientId, ReqId reqSeqNum);

  // Removes all request records this ClientsManager currently has.
  void clearAllPendingRequests();

  // Finds the request recorded by this ClientsManager at the earliest time (ignoring requests marked as committed),
  // writes its CID to the reference cid, and returns what that earliest time was. Writes an empty string to cid and
  // returns bftEngine::impl::MaxTime if this ClientsManager does not currently have records for any non-committed
  // requests.
  Time infoOfEarliestPendingRequest(std::string& cid) const;

  // Log a message for each request not marked as committed that this ClientsManager currently has a record for created
  // at a time more than threshold milliseconds before currTime. As a precondition to this function, the global logger
  // VC_LOG must be initialized. Behavior is undefined if it is not.
  void logAllPendingRequestsExceedingThreshold(const int64_t threshold, const Time& currTime) const;

  // Deletes the reply to clientId this ClientsManager currently has a record for made at the earliest time. If this
  // ClientsManager has any reply records to the given clientId, but none of those records have a record time, the one
  // for the earliest request sequence number will be deleted. Does nothing if this ClientsManager has no reply records
  // to the given clientId. Behavior is undefined if clientId does not belong to a valid client.
  void deleteOldestReply(NodeIdType clientId);

  bool isInternal(NodeIdType clientId) const { return internalClients_.find(clientId) != internalClients_.end(); }

  // Sets/updates a client public key and persist it to the reserved pages. Behavior is undefined in the following
  // cases:
  //   - The given NodeIdType parameter is not the ID of a valid client.
  //   - The given public key does not fit in a single reserved page under ClientsManager's implementation.
  void setClientPublicKey(NodeIdType, const std::string& key, concord::util::crypto::KeyFormat) override;

  // General
  static uint32_t reservedPagesPerClient(const uint32_t& sizeOfReservedPage, const uint32_t& maxReplySize);

 protected:
  uint32_t getReplyFirstPageId(NodeIdType clientId) const { return getKeyPageId(clientId) + 1; }

  uint32_t getKeyPageId(NodeIdType clientId) const {
    return (clientId - *clientIds_.cbegin()) * reservedPagesPerClient_;
  }

  const ReplicaId myId_;

  std::string scratchPage_;

  uint32_t reservedPagesPerClient_;

  struct RequestInfo {
    RequestInfo() : time(MinTime) {}
    RequestInfo(Time t, const std::string& c) : time(t), cid(c) {}

    Time time;
    std::string cid;
    bool committed = false;
  };

  struct ClientInfo {
    std::map<ReqId, RequestInfo> requestsInfo;
    std::map<ReqId, Time> repliesInfo;  // replyId to replyTime
    std::pair<std::string, concord::util::crypto::KeyFormat> pubKey;
  };

  std::set<NodeIdType> proxyClients_;
  std::set<NodeIdType> externalClients_;
  std::set<NodeIdType> internalClients_;
  std::set<NodeIdType> clientIds_;
  std::unordered_map<NodeIdType, ClientInfo> clientsInfo_;
  const uint32_t maxReplySize_;
  const uint16_t maxNumOfReqsPerClient_;
  concordMetrics::Component& metrics_;
  concordMetrics::CounterHandle metric_reply_inconsistency_detected_;
  concordMetrics::CounterHandle metric_removed_due_to_out_of_boundaries_;
  std::shared_ptr<PersistentStorage> ps_;
};

}  // namespace impl
}  // namespace bftEngine
