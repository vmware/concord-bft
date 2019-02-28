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

#include <string.h>
#include "ClientsManager.hpp"
#include "ClientReplyMsg.hpp"
#include "ClientRequestMsg.hpp"
#include "IStateTransfer.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"

namespace bftEngine {
namespace impl {

ClientsManager::ClientsManager(ReplicaId myId,
                               std::set<NodeIdType>& clientsSet,
                               uint32_t sizeOfReservedPage)
    : myId_(myId),
      sizeOfReservedPage_(sizeOfReservedPage),
      indexToClientInfo_(clientsSet.size()) {
  Assert(clientsSet.size() >= 1);

  scratchPage_ = (char*)std::malloc(sizeOfReservedPage);
  memset(scratchPage_, 0, sizeOfReservedPage);

  uint16_t idx = 0;
  for (NodeIdType c : clientsSet) {
    clientIdToIndex_.insert(std::pair<NodeIdType, uint16_t>(c, idx));

    indexToClientInfo_[idx].currentPendingRequest = 0;
    indexToClientInfo_[idx].timeOfCurrentPendingRequest = MinTime;

    indexToClientInfo_[idx].lastSeqNumberOfReply = 0;
    indexToClientInfo_[idx].latestReplyTime = MinTime;

    idx++;
  }

  reservedPagesPerClient_ = maxReplyMessageSize / sizeOfReservedPage;
  if (maxReplyMessageSize % sizeOfReservedPage != 0) reservedPagesPerClient_++;

  uint16_t numOfClients = (uint16_t)clientsSet.size();

  requiredNumberOfPages_ = (numOfClients * reservedPagesPerClient_);
}

ClientsManager::~ClientsManager() { std::free(scratchPage_); }

void ClientsManager::init(IStateTransfer* stateTransfer) {
  Assert(stateTransfer != nullptr);
  Assert(stateTransfer_ == nullptr);

  stateTransfer_ = stateTransfer;
}

uint32_t ClientsManager::numberOfRequiredReservedPages() const {
  return requiredNumberOfPages_;
}

void ClientsManager::clearReservedPages() {
  for (uint32_t i = 0; i < requiredNumberOfPages_; i++)
    stateTransfer_->zeroReservedPage(i);
}

void ClientsManager::loadInfoFromReservedPages() {
  for (std::pair<NodeIdType, uint16_t> e : clientIdToIndex_) {
    const uint32_t firstPageId = e.second * reservedPagesPerClient_;

    stateTransfer_->loadReservedPage(
        firstPageId, sizeOfReservedPage_, scratchPage_);

    ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_;
    Assert(replyHeader->msgType == 0 || replyHeader->msgType == MsgCode::Reply);
    Assert(replyHeader->currentPrimaryId == 0);
    Assert(replyHeader->replyLength >= 0);
    Assert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <=
           maxReplyMessageSize);

    ClientInfo& ci = indexToClientInfo_.at(e.second);
    ci.lastSeqNumberOfReply = replyHeader->reqSeqNum;
    ci.latestReplyTime = MinTime;

    // update pending request
    if (ci.currentPendingRequest != 0 &&
        (ci.currentPendingRequest <= replyHeader->reqSeqNum)) {
      ci.currentPendingRequest = 0;
      ci.timeOfCurrentPendingRequest = MinTime;
    }
  }
}

ReqId ClientsManager::seqNumberOfLastReplyToClient(NodeIdType clientId) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  ReqId retVal = indexToClientInfo_.at(idx).lastSeqNumberOfReply;
  return retVal;
}

bool ClientsManager::isValidClient(NodeIdType clientId) const {
  return (clientIdToIndex_.count(clientId) > 0);
}

void ClientsManager::getInfoAboutLastReplyToClient(NodeIdType clientId,
                                                   ReqId& outSeqNumber,
                                                   Time& outLatestTime) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const ClientInfo& c = indexToClientInfo_.at(idx);

  outSeqNumber = c.lastSeqNumberOfReply;
  outLatestTime = c.latestReplyTime;
}

ClientReplyMsg* ClientsManager::allocateNewReplyMsgAndWriteToStorage(
    NodeIdType clientId,
    ReqId requestSeqNum,
    uint16_t currentPrimaryId,
    char* reply,
    uint32_t replyLength) {
  // Assert(replyLength <= .... ) - TODO(GG)

  const uint16_t clientIdx = clientIdToIndex_.at(clientId);

  ClientInfo& c = indexToClientInfo_.at(clientIdx);

  Assert(c.lastSeqNumberOfReply < requestSeqNum);

  c.lastSeqNumberOfReply = requestSeqNum;
  c.latestReplyTime = getMonotonicTime();

  // LOG_INFO_F(GL, "allocateNewReplyMsgAndWriteToStorage - requestSeqNum=%d",
  // (int)requestSeqNum);

  ClientReplyMsg* const r =
      new ClientReplyMsg(myId_, requestSeqNum, reply, replyLength);

  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;

  // LOG_INFO_F(GL, "allocateNewReplyMsgAndWriteToStorage - firstPageId=%d",
  // (int)firstPageId);

  uint32_t numOfPages = r->size() / sizeOfReservedPage_;
  uint32_t sizeLastPage = sizeOfReservedPage_;

  if (r->size() % sizeOfReservedPage_ != 0) {
    numOfPages++;
    sizeLastPage = r->size() % sizeOfReservedPage_;
  }

  // LOG_INFO_F(GL, "allocateNewReplyMsgAndWriteToStorage - numOfPages=%d",
  // (int)numOfPages); LOG_INFO_F(GL, "allocateNewReplyMsgAndWriteToStorage -
  // sizeLastPage=%d", (int)sizeLastPage);

  // write reply message to reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    const char* ptrPage = r->body() + i * sizeOfReservedPage_;
    const uint32_t sizePage =
        ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
    stateTransfer_->saveReservedPage(firstPageId + i, sizePage, ptrPage);
  }

  // write currentPrimaryId to message (we don't store the currentPrimaryId in
  // the reserved pages)
  r->setPrimaryId(currentPrimaryId);

  LOG_INFO_F(
      GL,
      "allocateNewReplyMsgAndWriteToStorage returns reply with hash=%" PRIu64
      "",
      r->debugHash());

  return r;
}

ClientReplyMsg* ClientsManager::allocateMsgWithLatestReply(
    NodeIdType clientId, uint16_t currentPrimaryId) {
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);

  ClientInfo& info = indexToClientInfo_.at(clientIdx);

  Assert(info.lastSeqNumberOfReply != 0);

  // LOG_INFO_F(GL, "allocateMsgWithLatestReply - info.lastSeqNumberOfReply=%d",
  // (int)info.lastSeqNumberOfReply);

  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;

  // LOG_INFO_F(GL, "allocateMsgWithLatestReply - firstPageId=%d",
  // (int)firstPageId);

  stateTransfer_->loadReservedPage(
      firstPageId, sizeOfReservedPage_, scratchPage_);

  ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_;
  Assert(replyHeader->msgType == MsgCode::Reply);
  Assert(replyHeader->currentPrimaryId == 0);
  Assert(replyHeader->replyLength > 0);
  Assert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <=
         maxReplyMessageSize);

  uint32_t replyMsgSize =
      sizeof(ClientReplyMsgHeader) + replyHeader->replyLength;

  uint32_t numOfPages = replyMsgSize / sizeOfReservedPage_;
  uint32_t sizeLastPage = sizeOfReservedPage_;
  if (replyMsgSize % sizeOfReservedPage_ != 0) {
    numOfPages++;
    sizeLastPage = replyMsgSize % sizeOfReservedPage_;
  }

  // LOG_INFO_F(GL, "allocateMsgWithLatestReply - numOfPages=%d",
  // (int)numOfPages); LOG_INFO_F(GL, "allocateMsgWithLatestReply -
  // sizeLastPage=%d", (int)sizeLastPage);

  ClientReplyMsg* const r = new ClientReplyMsg(myId_, replyHeader->replyLength);

  // load reply message from reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    char* const ptrPage = r->body() + i * sizeOfReservedPage_;
    const uint32_t sizePage =
        ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
    stateTransfer_->loadReservedPage(firstPageId + i, sizePage, ptrPage);
  }

  r->setPrimaryId(currentPrimaryId);

  LOG_INFO_F(GL,
             "allocateMsgWithLatestReply returns reply with hash=%" PRIu64 "",
             r->debugHash());

  return r;
}

bool ClientsManager::noPendingAndRequestCanBecomePending(
    NodeIdType clientId, ReqId reqSeqNum) const {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const ClientInfo& c = indexToClientInfo_.at(idx);

  if (c.currentPendingRequest != 0) return false;  // if has pending request

  if (reqSeqNum <= c.lastSeqNumberOfReply)
    return false;  // if already executed a later/equivalent request

  return true;
}

/*
bool ClientsManager::isPendingOrLate(NodeIdType clientId, ReqId reqSeqNum) const
{
        uint16_t idx = clientIdToIndex_.at(clientId);
        const ClientInfo& c = indexToClientInfo_.at(idx);
        bool retVal = (reqSeqNum <= c.lastSeqNumberOfReply || reqSeqNum <=
c.currentPendingRequest); return retVal;
}
*/

void ClientsManager::addPendingRequest(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  ClientInfo& c = indexToClientInfo_.at(idx);
  Assert(reqSeqNum > c.lastSeqNumberOfReply &&
         reqSeqNum > c.currentPendingRequest);

  c.currentPendingRequest = reqSeqNum;
  c.timeOfCurrentPendingRequest = getMonotonicTime();
}

/*
void ClientsManager::removePendingRequest(NodeIdType clientId, ReqId reqSeqNum)
{
        uint16_t idx = clientIdToIndex_.at(clientId);
        ClientInfo& c = indexToClientInfo_.at(idx);

        if (c.currentPendingRequest == reqSeqNum)
        {
                c.currentPendingRequest = 0;
                c.timeOfCurrentPendingRequest = MinTime;
        }
}


void ClientsManager::removeEarlierPendingRequests(NodeIdType clientId, ReqId
reqSeqNum)
{
        uint16_t idx = clientIdToIndex_.at(clientId);
        ClientInfo& c = indexToClientInfo_.at(idx);

        if (c.currentPendingRequest < reqSeqNum)
        {
                c.currentPendingRequest = 0;
                c.timeOfCurrentPendingRequest = MinTime;
        }
}


void ClientsManager::removeEarlierOrEqualPendingRequests(NodeIdType clientId,
ReqId reqSeqNum)
{
        uint16_t idx = clientIdToIndex_.at(clientId);
        ClientInfo& c = indexToClientInfo_.at(idx);

        if (c.currentPendingRequest <= reqSeqNum)
        {
                c.currentPendingRequest = 0;
                c.timeOfCurrentPendingRequest = MinTime;
        }
}

*/

void ClientsManager::removePendingRequestOfClient(NodeIdType clientId) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  ClientInfo& c = indexToClientInfo_.at(idx);

  if (c.currentPendingRequest != 0) {
    c.currentPendingRequest = 0;
    c.timeOfCurrentPendingRequest = MinTime;
  }
}

void ClientsManager::clearAllPendingRequests() {
  for (ClientInfo& c : indexToClientInfo_) {
    c.currentPendingRequest = 0;
    c.timeOfCurrentPendingRequest = MinTime;
  }

  Assert(indexToClientInfo_[0].currentPendingRequest == 0);  // TODO(GG): debug
}

Time ClientsManager::timeOfEarliestPendingRequest()
    const  // TODO(GG): naive implementation - consider to optimize
{
  Time t = MaxTime;

  for (const ClientInfo& c : indexToClientInfo_) {
    if (c.timeOfCurrentPendingRequest != MinTime &&
        t > c.timeOfCurrentPendingRequest)
      t = c.timeOfCurrentPendingRequest;
  }

  return t;
}
}  // namespace impl
}  // namespace bftEngine
