// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <string.h>
#include "ClientsManager.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "IStateTransfer.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine {
namespace impl {
// initialize:
//* map of client id to indices.
//* map indices to client info.
//* Calculate reserved pages per client.
ClientsManager::ClientsManager(ReplicaId myId,
                               std::set<NodeIdType>& clientsSet,
                               uint32_t sizeOfReservedPage,
                               const uint32_t& maxReplysize)
    : myId_(myId),
      sizeOfReservedPage_(sizeOfReservedPage),
      indexToClientInfo_(clientsSet.size()),
      maxReplysize_(maxReplysize) {
  ConcordAssert(clientsSet.size() >= 1);

  scratchPage_ = (char*)std::malloc(sizeOfReservedPage);
  memset(scratchPage_, 0, sizeOfReservedPage);

  uint16_t idx = 0;
  for (NodeIdType c : clientsSet) {
    clientIdToIndex_.insert(std::pair<NodeIdType, uint16_t>(c, idx));

    indexToClientInfo_[idx].currentPendingRequest = 0;
    indexToClientInfo_[idx].timeOfCurrentPendingRequest = MinTime;

    indexToClientInfo_[idx].lastSeqNumberOfReply = 0;
    indexToClientInfo_[idx].latestReplyTime = MinTime;
    highestIdOfNonInternalClient_ = c;
    idx++;
  }

  reservedPagesPerClient_ = reservedPagesPerClient(sizeOfReservedPage, maxReplysize_);

  numOfClients_ = (uint16_t)clientsSet.size();

  requiredNumberOfPages_ = (numOfClients_ * reservedPagesPerClient_);
}

uint32_t ClientsManager::reservedPagesPerClient(const uint32_t& sizeOfReservedPage, const uint32_t& maxReplysize) {
  uint32_t reservedPagesPerClient = maxReplysize / sizeOfReservedPage;
  if (maxReplysize % sizeOfReservedPage != 0) {
    reservedPagesPerClient++;
  }
  return reservedPagesPerClient;
}

// Internal bft clients will be located after all other clients.
void ClientsManager::initInternalClientInfo(const int& numReplicas) {
  indexToClientInfo_.resize(indexToClientInfo_.size() + numReplicas);
  requiredNumberOfPages_ += requiredNumberOfPages_ * numReplicas;
  auto currClId = highestIdOfNonInternalClient_;
  auto currIdx = clientIdToIndex_[highestIdOfNonInternalClient_];
  for (int i = 0; i < numReplicas; i++) {
    clientIdToIndex_.insert(std::pair<NodeIdType, uint16_t>(++currClId, ++currIdx));
    indexToClientInfo_[currIdx].currentPendingRequest = 0;
    indexToClientInfo_[currIdx].timeOfCurrentPendingRequest = MinTime;

    indexToClientInfo_[currIdx].lastSeqNumberOfReply = 0;
    indexToClientInfo_[currIdx].latestReplyTime = MinTime;
    LOG_DEBUG(GL,
              "Adding internal client, id [" << currClId << "] as index [" << currIdx << "] vector size "
                                             << indexToClientInfo_.size());
  }
}

NodeIdType ClientsManager::getHighestIdOfNonInternalClient() { return highestIdOfNonInternalClient_; }

int ClientsManager::getIndexOfClient(const NodeIdType& id) const {
  if (clientIdToIndex_.find(id) == clientIdToIndex_.end()) return -1;
  return clientIdToIndex_.at(id);
}

ClientsManager::~ClientsManager() { std::free(scratchPage_); }

void ClientsManager::init(IStateTransfer* stateTransfer) {
  ConcordAssert(stateTransfer != nullptr);
  ConcordAssert(stateTransfer_ == nullptr);

  stateTransfer_ = stateTransfer;
}

uint32_t ClientsManager::numberOfRequiredReservedPages() const { return requiredNumberOfPages_; }

void ClientsManager::clearReservedPages() {
  for (uint32_t i = 0; i < requiredNumberOfPages_; i++) stateTransfer_->zeroReservedPage(resPageOffset() + i);
}

// per client:
// * calculate offset of reserved page start.
// * load corresponding page from state-transfer to scratchPage.
// * Fill its clientInfo.
// * remove pending request if loaded reply is newer.
void ClientsManager::loadInfoFromReservedPages() {
  for (std::pair<NodeIdType, uint16_t> e : clientIdToIndex_) {
    const uint32_t firstPageId = e.second * reservedPagesPerClient_;

    if (!stateTransfer_->loadReservedPage(resPageOffset() + firstPageId, sizeOfReservedPage_, scratchPage_)) continue;

    ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_;
    ConcordAssert(replyHeader->msgType == 0 || replyHeader->msgType == MsgCode::ClientReply);
    ConcordAssert(replyHeader->currentPrimaryId == 0);
    ConcordAssert(replyHeader->replyLength >= 0);
    ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplysize_);

    ClientInfo& ci = indexToClientInfo_.at(e.second);
    ci.lastSeqNumberOfReply = replyHeader->reqSeqNum;
    ci.latestReplyTime = MinTime;

    // update pending request
    if (ci.currentPendingRequest != 0 && (ci.currentPendingRequest <= replyHeader->reqSeqNum)) {
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

bool ClientsManager::isValidClient(NodeIdType clientId) const { return (clientIdToIndex_.count(clientId) > 0); }

void ClientsManager::getInfoAboutLastReplyToClient(NodeIdType clientId, ReqId& outSeqNumber, Time& outLatestTime) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const ClientInfo& c = indexToClientInfo_.at(idx);

  outSeqNumber = c.lastSeqNumberOfReply;
  outLatestTime = c.latestReplyTime;
}

// Reference the ClientInfo of the corresponding client:
//* set last reply seq num to the seq num of the request we reply to.
//* set reply time to `now`.
//* allocate new ClientReplyMsg
//* calculate: num of pages, size of last page.
//* save the reply to the reserved pages.
ClientReplyMsg* ClientsManager::allocateNewReplyMsgAndWriteToStorage(
    NodeIdType clientId, ReqId requestSeqNum, uint16_t currentPrimaryId, char* reply, uint32_t replyLength) {
  // ConcordAssert(replyLength <= .... ) - TODO(GG)

  const uint16_t clientIdx = clientIdToIndex_.at(clientId);

  ClientInfo& c = indexToClientInfo_.at(clientIdx);

  ConcordAssert(c.lastSeqNumberOfReply <= requestSeqNum);

  c.lastSeqNumberOfReply = requestSeqNum;
  c.latestReplyTime = getMonotonicTime();

  LOG_DEBUG(GL, "requestSeqNum=" << requestSeqNum);

  ClientReplyMsg* const r = new ClientReplyMsg(myId_, requestSeqNum, reply, replyLength);

  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;

  LOG_DEBUG(GL, "firstPageId=" << firstPageId);

  uint32_t numOfPages = r->size() / sizeOfReservedPage_;
  uint32_t sizeLastPage = sizeOfReservedPage_;

  if (r->size() % sizeOfReservedPage_ != 0) {
    numOfPages++;
    sizeLastPage = r->size() % sizeOfReservedPage_;
  }

  LOG_DEBUG(GL, "numOfPages=" << numOfPages << " sizeLastPage=" << sizeLastPage);

  // write reply message to reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    const char* ptrPage = r->body() + i * sizeOfReservedPage_;
    const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
    stateTransfer_->saveReservedPage(resPageOffset() + firstPageId + i, sizePage, ptrPage);
  }

  // write currentPrimaryId to message (we don't store the currentPrimaryId in the reserved pages)
  r->setPrimaryId(currentPrimaryId);

  LOG_DEBUG(GL, "returns reply with hash=" << r->debugHash());

  return r;
}

//* load client reserve page to scratchPage
//* cast to ClientReplyMsgHeader and validate.
//* calculate: reply msg size, num of pages, size of last page.
//* allocate new ClientReplyMsg.
//* copy relpy from reserved pages to ClientReplyMsg.
//* set primary id.
ClientReplyMsg* ClientsManager::allocateMsgWithLatestReply(NodeIdType clientId, uint16_t currentPrimaryId) {
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);

  ClientInfo& info = indexToClientInfo_.at(clientIdx);

  ConcordAssert(info.lastSeqNumberOfReply != 0);

  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;

  LOG_DEBUG(GL, "info.lastSeqNumberOfReply=" << info.lastSeqNumberOfReply << " firstPageId=" << firstPageId);

  stateTransfer_->loadReservedPage(resPageOffset() + firstPageId, sizeOfReservedPage_, scratchPage_);

  ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_;
  ConcordAssert(replyHeader->msgType == MsgCode::ClientReply);
  ConcordAssert(replyHeader->currentPrimaryId == 0);
  ConcordAssert(replyHeader->replyLength > 0);
  ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplysize_);

  uint32_t replyMsgSize = sizeof(ClientReplyMsgHeader) + replyHeader->replyLength;

  uint32_t numOfPages = replyMsgSize / sizeOfReservedPage_;
  uint32_t sizeLastPage = sizeOfReservedPage_;
  if (replyMsgSize % sizeOfReservedPage_ != 0) {
    numOfPages++;
    sizeLastPage = replyMsgSize % sizeOfReservedPage_;
  }

  LOG_DEBUG(GL, "numOfPages=" << numOfPages << " sizeLastPage=" << sizeLastPage);

  ClientReplyMsg* const r = new ClientReplyMsg(myId_, replyHeader->replyLength);

  // load reply message from reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    char* const ptrPage = r->body() + i * sizeOfReservedPage_;
    const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
    stateTransfer_->loadReservedPage(resPageOffset() + firstPageId + i, sizePage, ptrPage);
  }

  r->setPrimaryId(currentPrimaryId);

  LOG_DEBUG(GL, "returns reply with hash=" << r->debugHash());

  return r;
}

// Check that:
//* no pending req is set for that client.
//* request seq number is bigger than the last reply seq number.
bool ClientsManager::noPendingAndRequestCanBecomePending(NodeIdType clientId, ReqId reqSeqNum) const {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const ClientInfo& c = indexToClientInfo_.at(idx);

  if (c.currentPendingRequest != 0) return false;  // if has pending request

  if (reqSeqNum <= c.lastSeqNumberOfReply) return false;  // if already executed a later/equivalent request

  return true;
}

/*
bool ClientsManager::isPendingOrLate(NodeIdType clientId, ReqId reqSeqNum) const
{
        uint16_t idx = clientIdToIndex_.at(clientId);
        const ClientInfo& c = indexToClientInfo_.at(idx);
        bool retVal = (reqSeqNum <= c.lastSeqNumberOfReply || reqSeqNum <= c.currentPendingRequest);
        return retVal;
}
*/

void ClientsManager::addPendingRequest(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  ClientInfo& c = indexToClientInfo_.at(idx);
  ConcordAssert(reqSeqNum > c.lastSeqNumberOfReply && reqSeqNum > c.currentPendingRequest);

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


void ClientsManager::removeEarlierPendingRequests(NodeIdType clientId, ReqId reqSeqNum)
{
        uint16_t idx = clientIdToIndex_.at(clientId);
        ClientInfo& c = indexToClientInfo_.at(idx);

        if (c.currentPendingRequest < reqSeqNum)
        {
                c.currentPendingRequest = 0;
                c.timeOfCurrentPendingRequest = MinTime;
        }
}


void ClientsManager::removeEarlierOrEqualPendingRequests(NodeIdType clientId, ReqId reqSeqNum)
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

  ConcordAssert(indexToClientInfo_[0].currentPendingRequest == 0);  // TODO(GG): debug
}

// iterate over all clients and choose the earliest pending request.
Time ClientsManager::timeOfEarliestPendingRequest() const  // TODO(GG): naive implementation - consider to optimize
{
  Time t = MaxTime;
  ClientInfo earliestClientWithPendingRequest = indexToClientInfo_.at(0);

  for (const ClientInfo& c : indexToClientInfo_) {
    if (c.timeOfCurrentPendingRequest != MinTime && t > c.timeOfCurrentPendingRequest) {
      t = c.timeOfCurrentPendingRequest;
      earliestClientWithPendingRequest = c;
    }
  }

  LOG_INFO(GL, "Earliest pending client request: " << KVLOG(earliestClientWithPendingRequest.currentPendingRequest));

  return t;
}
}  // namespace impl
}  // namespace bftEngine
