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
#include <map>
#include <set>
#include <vector>
#include <memory>

namespace bftEngine {
class IStateTransfer;

namespace impl {
class ClientReplyMsg;
class ClientRequestMsg;

class ClientsManager : public ResPagesClient<ClientsManager>, public IPendingRequest {
 public:
  ClientsManager(concordMetrics::Component& metrics, std::set<NodeIdType>& clientsSet);
  ~ClientsManager();

  uint32_t numberOfRequiredReservedPages() const;

  void loadInfoFromReservedPages();

  // Replies

  // TODO(GG): make sure that ReqId is based on time (and ignore requests with time that does
  // not make sense (too high) - this will prevent some potential attacks)
  bool hasReply(NodeIdType clientId, ReqId reqSeqNum);

  bool isValidClient(NodeIdType clientId) const;

  std::unique_ptr<ClientReplyMsg> allocateNewReplyMsgAndWriteToStorage(
      NodeIdType clientId, ReqId requestSeqNum, uint16_t currentPrimaryId, char* reply, uint32_t replyLength);

  std::unique_ptr<ClientReplyMsg> allocateReplyFromSavedOne(NodeIdType clientId,
                                                            ReqId requestSeqNum,
                                                            uint16_t currentPrimaryId);

  // Requests

  bool isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) const;

  // Return true IFF there is no pending requests for clientId, and reqSeqNum can become the new pending request
  bool canBecomePending(NodeIdType clientId, ReqId reqSeqNum) const;

  bool isPending(NodeIdType clientId, ReqId reqSeqNum) const override;
  void addPendingRequest(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid);

  void markRequestAsCommitted(NodeIdType clientId, ReqId reqSequenceNum);

  void removeRequestsOutOfBatchBounds(NodeIdType clientId, ReqId reqSequenceNum);
  void removePendingForExecutionRequest(NodeIdType clientId, ReqId reqSeqNum);

  void clearAllPendingRequests();

  Time infoOfEarliestPendingRequest(std::string& cid) const;

  void deleteOldestReply(NodeIdType clientId);

  // Internal Clients
  void initInternalClientInfo(const int& numReplicas);
  inline bool isInternal(NodeIdType clientId) const { return clientId > highestIdOfNonInternalClient_; };

  // Returns the ID of the last client before internal clients.
  NodeIdType getHighestIdOfNonInternalClient();

  // General
  static uint32_t reservedPagesPerClient(const uint32_t& sizeOfReservedPage, const uint32_t& maxReplySize);
  int getIndexOfClient(const NodeIdType& id) const;

 protected:
  const ReplicaId myId_;
  const uint32_t sizeOfReservedPage_;

  char* scratchPage_ = nullptr;

  uint32_t reservedPagesPerClient_;
  uint32_t requiredNumberOfPages_;

  uint16_t numOfClients_{0};
  NodeIdType highestIdOfNonInternalClient_{0};

  std::map<NodeIdType, uint16_t> clientIdToIndex_;

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
  };

  std::vector<ClientInfo> indexToClientInfo_;
  const uint32_t maxReplySize_;
  const uint16_t maxNumOfReqsPerClient_;
  concordMetrics::Component& metrics_;
  concordMetrics::CounterHandle metric_reply_inconsistency_detected_;
  concordMetrics::CounterHandle metric_removed_due_to_out_of_boundaries_;
};

}  // namespace impl
}  // namespace bftEngine
