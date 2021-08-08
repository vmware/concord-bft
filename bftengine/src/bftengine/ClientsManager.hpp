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
#include <map>
#include <set>
#include <unordered_map>
#include <memory>

namespace bftEngine {
class IStateTransfer;

namespace impl {
class ClientReplyMsg;
class ClientRequestMsg;

class ClientsManager : public ResPagesClient<ClientsManager>, public IPendingRequest, public IClientPublicKeyStore {
 public:
  ClientsManager(const std::set<NodeIdType>& proxyClients,
                 const std::set<NodeIdType>& externalClients,
                 const std::set<NodeIdType>& internalClients,
                 concordMetrics::Component& metrics);

  uint32_t numberOfRequiredReservedPages() const { return clientIds_.size() * reservedPagesPerClient_; }

  void loadInfoFromReservedPages();

  // Replies

  // TODO(GG): make sure that ReqId is based on time (and ignore requests with time that does
  // not make sense (too high) - this will prevent some potential attacks)
  bool hasReply(NodeIdType clientId, ReqId reqSeqNum) const;

  bool isValidClient(NodeIdType clientId) const { return clientIds_.find(clientId) != clientIds_.end(); }

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

  bool isInternal(NodeIdType clientId) const { return internalClients_.find(clientId) != internalClients_.end(); }

  // set/update and persist a client public key
  void setClientPublicKey(NodeIdType, const std::string& key, KeyFormat) override;
  // gett a client public key
  std::string getClientPublicKey(uint16_t clientId) override;

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
    std::pair<std::string, KeyFormat> pubKey;
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
};

}  // namespace impl
}  // namespace bftEngine
