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

#include "ClientsManager.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "IStateTransfer.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine::impl {
// Initialize:
// * map of client id to indices.
// * Calculate reserved pages per client.
ClientsManager::ClientsManager(std::set<NodeIdType>& clientsSet)
    : myId_(ReplicaConfig::instance().replicaId),
      sizeOfReservedPage_(ReplicaConfig::instance().getsizeOfReservedPage()),
      indexToClientInfo_(clientsSet.size()),
      maxReplySize_(ReplicaConfig::instance().getmaxReplyMessageSize()),
      maxNumOfReqsPerClient_(ReplicaConfig::instance().clientMiniBatchingEnabled
                                 ? ReplicaConfig::instance().clientMiniBatchingMaxMsgsNbr
                                 : 1) {
  ConcordAssert(clientsSet.size() >= 1);
  scratchPage_ = (char*)std::malloc(sizeOfReservedPage_);
  memset(scratchPage_, 0, sizeOfReservedPage_);

  uint16_t idx = 0;
  for (NodeIdType c : clientsSet) {
    clientIdToIndex_.insert(std::pair<NodeIdType, uint16_t>(c, idx));
    highestIdOfNonInternalClient_ = c;
    indexToClientInfo_.push_back(ClientInfo());
    idx++;
  }
  reservedPagesPerClient_ = reservedPagesPerClient(sizeOfReservedPage_, maxReplySize_);
  numOfClients_ = (uint16_t)clientsSet.size();
  requiredNumberOfPages_ = (numOfClients_ * reservedPagesPerClient_);
  LOG_DEBUG(GL, KVLOG(sizeOfReservedPage_, reservedPagesPerClient_, maxReplySize_, maxNumOfReqsPerClient_));
}

uint32_t ClientsManager::reservedPagesPerClient(const uint32_t& sizeOfReservedPage, const uint32_t& maxReplySize) {
  uint32_t reservedPagesPerClient = maxReplySize / sizeOfReservedPage;
  if (maxReplySize % sizeOfReservedPage != 0) {
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
    indexToClientInfo_.push_back(ClientInfo());
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

// Per client:
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
    ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplySize_);

    auto& repliesInfo = indexToClientInfo_.at(e.second).repliesInfo;
    repliesInfo.emplace(replyHeader->reqSeqNum, MinTime);

    // Remove pending request
    auto& requestsInfo = indexToClientInfo_.at(e.second).requestsInfo;
    const auto& reqIt = requestsInfo.find(replyHeader->reqSeqNum);
    if (reqIt != requestsInfo.end()) requestsInfo.erase(reqIt);
  }
}

bool ClientsManager::hasReply(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const auto& repliesInfo = indexToClientInfo_.at(idx).repliesInfo;
  const auto& elem = repliesInfo.find(reqSeqNum);
  return (elem != repliesInfo.end());
}

void ClientsManager::deleteOldestReply(NodeIdType clientId) {
  Time earliestTime = MaxTime;
  ReqId earliestReplyId = 0;
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);
  auto& repliesInfo = indexToClientInfo_.at(clientIdx).repliesInfo;
  for (const auto& reply : repliesInfo) {
    if (reply.second != MinTime && earliestTime > reply.second) {
      earliestReplyId = reply.first;
      earliestTime = reply.second;
    }
  }
  if (earliestReplyId) {
    repliesInfo.erase(earliestReplyId);
    LOG_DEBUG(GL, "Deleted oldest reply message" << KVLOG(earliestReplyId));
  }
}

bool ClientsManager::isValidClient(NodeIdType clientId) const { return (clientIdToIndex_.count(clientId) > 0); }

// Reference the ClientInfo of the corresponding client:
// * set last reply seq num to the seq num of the request we reply to.
// * set reply time to `now`.
// * allocate new ClientReplyMsg
// * calculate: num of pages, size of last page.
// * save the reply to the reserved pages.
ClientReplyMsg* ClientsManager::allocateNewReplyMsgAndWriteToStorage(
    NodeIdType clientId, ReqId requestSeqNum, uint16_t currentPrimaryId, char* reply, uint32_t replyLength) {
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);
  ClientInfo& c = indexToClientInfo_.at(clientIdx);
  if (c.repliesInfo.size() >= maxNumOfReqsPerClient_) deleteOldestReply(clientId);

  c.repliesInfo.emplace(requestSeqNum, getMonotonicTime());
  LOG_DEBUG(GL, KVLOG(clientId, requestSeqNum));
  ClientReplyMsg* const r = new ClientReplyMsg(myId_, requestSeqNum, reply, replyLength);
  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;
  LOG_DEBUG(GL, "firstPageId=" << firstPageId);
  uint32_t numOfPages = r->size() / sizeOfReservedPage_;
  uint32_t sizeLastPage = sizeOfReservedPage_;

  if (r->size() % sizeOfReservedPage_ != 0) {
    numOfPages++;
    sizeLastPage = r->size() % sizeOfReservedPage_;
  }

  LOG_DEBUG(GL, KVLOG(clientId, requestSeqNum, numOfPages, sizeLastPage));
  // write reply message to reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    const char* ptrPage = r->body() + i * sizeOfReservedPage_;
    const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
    stateTransfer_->saveReservedPage(resPageOffset() + firstPageId + i, sizePage, ptrPage);
  }

  // write currentPrimaryId to message (we don't store the currentPrimaryId in the reserved pages)
  r->setPrimaryId(currentPrimaryId);
  LOG_DEBUG(GL, "Returns reply with hash=" << r->debugHash() << KVLOG(clientId, requestSeqNum));
  return r;
}

// * load client reserve page to scratchPage
// * cast to ClientReplyMsgHeader and validate.
// * calculate: reply msg size, num of pages, size of last page.
// * allocate new ClientReplyMsg.
// * copy reply from reserved pages to ClientReplyMsg.
// * set primary id.
ClientReplyMsg* ClientsManager::allocateReplyFromSavedOne(NodeIdType clientId,
                                                          ReqId requestSeqNum,
                                                          uint16_t currentPrimaryId) {
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);
  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;
  LOG_DEBUG(GL, KVLOG(requestSeqNum, firstPageId));
  stateTransfer_->loadReservedPage(resPageOffset() + firstPageId, sizeOfReservedPage_, scratchPage_);

  ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_;
  ConcordAssert(replyHeader->msgType == MsgCode::ClientReply);
  ConcordAssert(replyHeader->currentPrimaryId == 0);
  ConcordAssert(replyHeader->replyLength > 0);
  ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplySize_);

  uint32_t replyMsgSize = sizeof(ClientReplyMsgHeader) + replyHeader->replyLength;
  uint32_t numOfPages = replyMsgSize / sizeOfReservedPage_;
  uint32_t sizeLastPage = sizeOfReservedPage_;
  if (replyMsgSize % sizeOfReservedPage_ != 0) {
    numOfPages++;
    sizeLastPage = replyMsgSize % sizeOfReservedPage_;
  }
  LOG_DEBUG(GL, KVLOG(numOfPages, sizeLastPage));
  ClientReplyMsg* const r = new ClientReplyMsg(myId_, replyHeader->replyLength);
  // load reply message from reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    char* const ptrPage = r->body() + i * sizeOfReservedPage_;
    const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
    stateTransfer_->loadReservedPage(resPageOffset() + firstPageId + i, sizePage, ptrPage);
  }

  if (r->reqSeqNum() != requestSeqNum) {
    LOG_INFO(GL,
             "The reserved page contains a reply for a different request, so the current request gets ignored"
                 << KVLOG(r->reqSeqNum(), requestSeqNum));
    delete r;
    return nullptr;
  }

  r->setPrimaryId(currentPrimaryId);
  LOG_DEBUG(GL, "Returns reply with hash=" << r->debugHash());
  return r;
}

// Check that:
// * max number of pending requests not reached for that client.
// * request seq number is bigger than the last reply seq number.
bool ClientsManager::canBecomePending(NodeIdType clientId, ReqId reqSeqNum) const {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  if (requestsInfo.size() == maxNumOfReqsPerClient_) {
    LOG_INFO(GL, "Maximum number of requests per client reached" << KVLOG(maxNumOfReqsPerClient_, clientId, reqSeqNum));
    return false;
  }
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    LOG_DEBUG(GL, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
    return false;
  }
  const auto& repliesInfo = indexToClientInfo_.at(idx).repliesInfo;
  const auto& replyIt = repliesInfo.find(reqSeqNum);
  if (replyIt != repliesInfo.end()) {
    LOG_DEBUG(GL, "The request has been already executed" << KVLOG(clientId, reqSeqNum));
    return false;
  }
  LOG_DEBUG(GL, "The request can become pending" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
  return true;
}

void ClientsManager::addPendingRequest(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  if (requestsInfo.find(reqSeqNum) != requestsInfo.end()) {
    LOG_WARN(GL, "The request already exists - skip adding" << KVLOG(clientId, reqSeqNum));
    return;
  }
  requestsInfo.emplace(reqSeqNum, RequestInfo{getMonotonicTime(), cid});
  LOG_DEBUG(GL, "Added request" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
}

void ClientsManager::markRequestAsCommitted(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    reqIt->second.committed = true;
    LOG_DEBUG(GL, "Marked committed" << KVLOG(clientId, reqSeqNum));
  }
  LOG_ERROR(GL, "Request not found" << KVLOG(clientId, reqSeqNum));
}

void ClientsManager::removePendingForExecutionRequest(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    requestsInfo.erase(reqIt);
    LOG_DEBUG(GL, "Removed request" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
  }
}

void ClientsManager::clearAllPendingRequests() {
  for (ClientInfo& clientInfo : indexToClientInfo_) clientInfo.requestsInfo.clear();
}

// Iterate over all clients and choose the earliest pending request.
Time ClientsManager::infoOfEarliestPendingRequest(std::string& cid) const {
  Time earliestTime = MaxTime;
  RequestInfo earliestPendingReqInfo{MaxTime, std::string()};
  for (const ClientInfo& clientInfo : indexToClientInfo_) {
    for (const auto& req : clientInfo.requestsInfo) {
      // Don't take into account already committed requests
      if ((req.second.time != MinTime) && (earliestTime > req.second.time) && (!req.second.committed)) {
        earliestPendingReqInfo = req.second;
        earliestTime = earliestPendingReqInfo.time;
      }
    }
  }
  cid = earliestPendingReqInfo.cid;
  if (earliestPendingReqInfo.time != MaxTime) LOG_INFO(GL, "Earliest pending request: " << KVLOG(cid));
  return earliestPendingReqInfo.time;
}

}  // namespace bftEngine::impl
