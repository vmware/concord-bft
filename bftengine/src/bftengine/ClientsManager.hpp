// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include "PrimitiveTypes.hpp"
#include "TimeUtils.hpp"

#include <map>
#include <set>
#include <vector>

namespace bftEngine {
class IStateTransfer;

namespace impl {
class ClientReplyMsg;
class ClientRequestMsg;

class ClientsManager {
 public:
  ClientsManager(ReplicaId myId,
                 std::set<NodeIdType>& clientsSet,
                 uint32_t sizeOfReservedPage);
  ~ClientsManager();

  void init(IStateTransfer* stateTransfer);

  uint32_t numberOfRequiredReservedPages() const;

  void clearReservedPages();

  void loadInfoFromReservedPages();

  // Replies

  ReqId seqNumberOfLastReplyToClient(
      NodeIdType
          clientId);  // TODO(GG): make sure that ReqId is based on time (and
                      // ignore requests with time that does not make sense (too
                      // high) - this will prevent some potential attacks)

  bool isValidClient(NodeIdType clientId) const;

  void getInfoAboutLastReplyToClient(NodeIdType clientId,
                                     ReqId& outseqNumber,
                                     Time& outSentTime);

  ClientReplyMsg* allocateNewReplyMsgAndWriteToStorage(
      NodeIdType clientId,
      ReqId requestSeqNum,
      uint16_t currentPrimaryId,
      char* reply,
      uint32_t replyLength);

  ClientReplyMsg* allocateMsgWithLatestReply(NodeIdType clientId,
                                             uint16_t currentPrimaryId);

  // Requests

  bool noPendingAndRequestCanBecomePending(NodeIdType clientId, ReqId reqSeqNum)
      const;  // return true IFF there is no pending requests for clientId, and
              // reqSeqNum can become the new pending request

  // bool isPendingOrLate(NodeIdType clientId, ReqId reqSeqNum) const ;

  void addPendingRequest(NodeIdType clientId, ReqId reqSeqNum);

  // void removePendingRequest(NodeIdType clientId, ReqId reqSeqNum);

  // void removeEarlierPendingRequests(NodeIdType clientId, ReqId reqSeqNum);

  // void removeEarlierOrEqualPendingRequests(NodeIdType clientId, ReqId
  // reqSeqNum);

  void removePendingRequestOfClient(NodeIdType clientId);

  void clearAllPendingRequests();

  Time timeOfEarliestPendingRequest() const;

 protected:
  const ReplicaId myId_;
  const uint32_t sizeOfReservedPage_;

  IStateTransfer* stateTransfer_ = nullptr;

  char* scratchPage_ = nullptr;

  uint32_t reservedPagesPerClient_;
  uint32_t requiredNumberOfPages_;

  std::map<NodeIdType, uint16_t> clientIdToIndex_;

  struct ClientInfo {
    // requests
    ReqId currentPendingRequest;
    Time timeOfCurrentPendingRequest;

    // replies
    ReqId lastSeqNumberOfReply;
    Time latestReplyTime;
  };

  std::vector<ClientInfo> indexToClientInfo_;
};
}  // namespace impl
}  // namespace bftEngine
