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
ClientsManager::ClientsManager(concordMetrics::Component& metrics, std::set<NodeIdType>& clientsSet)
    : myId_(ReplicaConfig::instance().replicaId),
      sizeOfReservedPage_(ReplicaConfig::instance().getsizeOfReservedPage()),
      indexToClientInfo_(clientsSet.size()),
      maxReplySize_(ReplicaConfig::instance().getmaxReplyMessageSize()),
      maxNumOfReqsPerClient_(
          ReplicaConfig::instance().clientBatchingEnabled ? ReplicaConfig::instance().clientBatchingMaxMsgsNbr : 1),
      metrics_(metrics),
      metric_reply_inconsistency_detected_{metrics_.RegisterCounter("totalReplyInconsistenciesDetected")},
      metric_removed_due_to_out_of_boundaries_{metrics_.RegisterCounter("totalRemovedDueToOutOfBoundaries")} {
  ConcordAssert(clientsSet.size() >= 1);
  scratchPage_ = std::string(sizeOfReservedPage_, 0);

  uint16_t idx = 0;
  for (NodeIdType c : clientsSet) {
    clientIdToIndex_.insert(std::pair<NodeIdType, uint16_t>(c, idx));
    highestIdOfNonInternalClient_ = c;
    indexToClientInfo_.push_back(ClientInfo());
    idx++;
  }
  reservedPagesPerClient_ = reservedPagesPerClient(sizeOfReservedPage_, maxReplySize_, maxNumOfReqsPerClient_);
  numOfClients_ = (uint16_t)clientsSet.size();
  requiredNumberOfPages_ = (numOfClients_ * reservedPagesPerClient_);
  singleReplyMaxNumOfPages_ = reservedPagesPerClient_ / maxNumOfReqsPerClient_;
  LOG_DEBUG(CL_MNGR, KVLOG(sizeOfReservedPage_, reservedPagesPerClient_, maxReplySize_, maxNumOfReqsPerClient_));
}

uint32_t ClientsManager::reservedPagesPerClient(const uint32_t& sizeOfReservedPage,
                                                const uint32_t& maxReplySize,
                                                const uint16_t maxNumOfReqsPerClient) {
  uint32_t reservedPagesPerClient = maxReplySize / sizeOfReservedPage;
  if (maxReplySize % sizeOfReservedPage != 0) {
    reservedPagesPerClient++;
  }
  return reservedPagesPerClient * maxNumOfReqsPerClient;
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
    LOG_DEBUG(CL_MNGR,
              "Adding internal client, id [" << currClId << "] as index [" << currIdx << "] vector size "
                                             << indexToClientInfo_.size());
  }
}

NodeIdType ClientsManager::getHighestIdOfNonInternalClient() { return highestIdOfNonInternalClient_; }

int ClientsManager::getIndexOfClient(const NodeIdType& id) const {
  if (clientIdToIndex_.find(id) == clientIdToIndex_.end()) return -1;
  return clientIdToIndex_.at(id);
}

void ClientsManager::init(IStateTransfer* stateTransfer) {
  ConcordAssert(stateTransfer != nullptr);
  ConcordAssert(stateTransfer_ == nullptr);
  stateTransfer_ = stateTransfer;
}

uint32_t ClientsManager::numberOfRequiredReservedPages() const { return requiredNumberOfPages_; }

// Per client:
// * calculate offset of reserved page start.
// * Iterate for max number of requests per client
// * load corresponding page from state-transfer to scratchPage.
// * Fill its clientInfo.
// * At the end of the loop take the latest loaded reply and remove pending request if latest reply is newer.
void ClientsManager::loadInfoFromReservedPages() {
  for (auto const& [clientId, clientIdx] : clientIdToIndex_) {
    const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;

    // load all replies into the map of replies one by one
    for (int i = 0; i < maxNumOfReqsPerClient_; i++) {
      if (!stateTransfer_->loadReservedPage(resPageOffset() + firstPageId + (i * singleReplyMaxNumOfPages_),
                                            sizeOfReservedPage_,
                                            scratchPage_.data()))
        break;

      auto replyHeader = (ClientReplyMsgHeader*)(scratchPage_.data());
      auto replyPtr = std::make_shared<ClientReplyMsg>(myId_, replyHeader->replyLength);
      ConcordAssert(replyHeader->msgType == 0 || replyHeader->msgType == MsgCode::ClientReply);
      ConcordAssert(replyHeader->currentPrimaryId == 0);
      ConcordAssert(replyHeader->replyLength >= 0);
      ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplySize_);

      auto& repliesInfo = indexToClientInfo_.at(clientIdx).repliesInfo;
      if (repliesInfo.size() >= maxNumOfReqsPerClient_) deleteOldestReply(clientId);
      const auto& res = repliesInfo.insert_or_assign(replyHeader->reqSeqNum, replyPtr);
      const bool added = res.second;
      LOG_INFO(CL_MNGR, "Added/updated reply message" << KVLOG(clientId, replyHeader->reqSeqNum, added));
    }
    // get the newest reply from the map of replies.
    if (!indexToClientInfo_.at(clientIdx).repliesInfo.empty()) {
      auto lastReply = indexToClientInfo_.at(clientIdx).repliesInfo.rbegin();
      // Remove old pending requests
      auto& requestsInfo = indexToClientInfo_.at(clientIdx).requestsInfo;
      for (auto it = requestsInfo.begin(); it != requestsInfo.end();) {
        if (it->first <= lastReply->first) {
          LOG_INFO(CL_MNGR, "Remove old pending request" << KVLOG(clientId, lastReply->first));
          it = requestsInfo.erase(it);
        } else
          break;
      }
    }
  }
}

bool ClientsManager::hasReply(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const auto& repliesInfo = indexToClientInfo_.at(idx).repliesInfo;
  const auto& elem = repliesInfo.find(reqSeqNum);
  const bool found = (elem != repliesInfo.end());
  if (found) LOG_DEBUG(CL_MNGR, "Reply found for" << KVLOG(clientId, reqSeqNum));
  return found;
}

void ClientsManager::deleteOldestReply(NodeIdType clientId) {
  Time earliestTime = MaxTime;
  ReqId earliestReplyId = 0;
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);
  auto& repliesInfo = indexToClientInfo_.at(clientIdx).repliesInfo;
  // Since seqnum is always growing we can be sure that the first element on the map is the oldest reply so we want to
  // remove this reply.
  if (!repliesInfo.empty()) {
    earliestReplyId = repliesInfo.cbegin()->first;
    repliesInfo.erase(earliestReplyId);
    LOG_DEBUG(CL_MNGR,
              "Deleted reply message" << KVLOG(
                  clientId, earliestReplyId, earliestTime.time_since_epoch().count(), repliesInfo.size()));
  }
}

bool ClientsManager::isValidClient(NodeIdType clientId) const { return (clientIdToIndex_.count(clientId) > 0); }

// Reference the ClientInfo of the corresponding client:
// * Remove the oldest reply from the replies map
// * allocate client reply message and hold it inside the replies map
// * calculate: num of pages, size of last page.
// * save the reply to the reserved pages.
std::shared_ptr<ClientReplyMsg> ClientsManager::allocateNewReplyMsgAndWriteToStorage(
    NodeIdType clientId, ReqId requestSeqNum, uint16_t currentPrimaryId, char* reply, uint32_t replyLength) {
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);
  ClientInfo& c = indexToClientInfo_.at(clientIdx);
  if (c.repliesInfo.size() >= maxNumOfReqsPerClient_) deleteOldestReply(clientId);
  if (c.repliesInfo.size() > maxNumOfReqsPerClient_) {
    LOG_FATAL(CL_MNGR,
              "More than maxNumOfReqsPerClient_ items in repliesInfo"
                  << KVLOG(c.repliesInfo.size(), maxNumOfReqsPerClient_, clientId, requestSeqNum, replyLength));
  }

  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum));
  auto r = std::make_shared<ClientReplyMsg>(myId_, requestSeqNum, reply, replyLength);
  c.repliesInfo.insert_or_assign(requestSeqNum, r);
  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;
  LOG_DEBUG(CL_MNGR, "firstPageId=" << firstPageId);
  uint16_t replyNum = 0;
  for (auto& rep : c.repliesInfo) {
    uint32_t numOfPages = rep.second->size() / sizeOfReservedPage_;
    uint32_t sizeLastPage = sizeOfReservedPage_;
    if (numOfPages > reservedPagesPerClient_) {
      LOG_FATAL(CL_MNGR,
                "Client reply is larger than reservedPagesPerClient_ allows"
                    << KVLOG(clientId,
                             rep.second->reqSeqNum(),
                             reservedPagesPerClient_ * sizeOfReservedPage_,
                             rep.second->replyLength()));
      ConcordAssert(false);
    }

    if (rep.second->size() % sizeOfReservedPage_ != 0) {
      numOfPages++;
      sizeLastPage = rep.second->size() % sizeOfReservedPage_;
    }

    LOG_DEBUG(CL_MNGR, KVLOG(clientId, rep.second->reqSeqNum(), numOfPages, sizeLastPage));
    // write reply message to reserved pages
    for (uint32_t i = 0; i < numOfPages; i++) {
      const char* ptrPage = rep.second->body() + i * sizeOfReservedPage_;
      const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
      stateTransfer_->saveReservedPage(
          resPageOffset() + firstPageId + i + (replyNum * singleReplyMaxNumOfPages_), sizePage, ptrPage);
    }
    replyNum++;
  }

  // write currentPrimaryId to message (we don't store the currentPrimaryId in the reserved pages)
  r->setPrimaryId(currentPrimaryId);
  LOG_DEBUG(CL_MNGR, "Returns reply with hash=" << r->debugHash() << KVLOG(clientId, requestSeqNum));
  return r;
}

// * Iterate over all the replies of the client
// * load client reserve page to scratchPage
// * cast to ClientReplyMsgHeader and validate if this is the requested sequence number.
// * calculate: reply msg size, num of pages, size of last page.
// * allocate new ClientReplyMsg.
// * copy reply from reserved pages to ClientReplyMsg.
// * set primary id.
std::shared_ptr<ClientReplyMsg> ClientsManager::allocateReplyFromSavedOne(NodeIdType clientId,
                                                                          ReqId requestSeqNum,
                                                                          uint16_t currentPrimaryId) {
  const uint16_t clientIdx = clientIdToIndex_.at(clientId);
  const uint32_t firstPageId = clientIdx * reservedPagesPerClient_;
  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum, firstPageId));
  for (uint16_t j = 0; j < maxNumOfReqsPerClient_; j++) {
    stateTransfer_->loadReservedPage(
        resPageOffset() + firstPageId + (j * singleReplyMaxNumOfPages_), sizeOfReservedPage_, scratchPage_.data());

    ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_.data();
    if (replyHeader->reqSeqNum != requestSeqNum) continue;
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
    LOG_DEBUG(CL_MNGR, KVLOG(clientId, numOfPages, sizeLastPage));
    auto r = std::make_shared<ClientReplyMsg>(myId_, replyHeader->replyLength);
    // load reply message from reserved pages
    for (uint32_t i = 0; i < numOfPages; i++) {
      char* const ptrPage = r->body() + i * sizeOfReservedPage_;
      const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage_ : sizeLastPage);
      stateTransfer_->loadReservedPage(
          resPageOffset() + firstPageId + (j * singleReplyMaxNumOfPages_) + i, sizePage, ptrPage);
    }

    r->setPrimaryId(currentPrimaryId);
    LOG_DEBUG(CL_MNGR, "Returns reply with hash=" << r->debugHash());
    return r;
  }
  LOG_ERROR(CL_MNGR,
            "Client reply with sequence number=" << requestSeqNum
                                                 << " has not been found on the reserved pages of client=" << clientId);
  return nullptr;
}

bool ClientsManager::isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) const {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  const auto& reqIt = indexToClientInfo_.at(idx).requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    LOG_DEBUG(CL_MNGR, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
    return true;
  }
  return false;
}

bool ClientsManager::isPending(NodeIdType clientId, ReqId reqSeqNum) const {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end() && !reqIt->second.committed) {
    return true;
  }
  return false;
}
// Check that:
// * max number of pending requests not reached for that client.
// * request seq number is bigger than the last reply seq number.
bool ClientsManager::canBecomePending(NodeIdType clientId, ReqId reqSeqNum) const {
  uint16_t idx = clientIdToIndex_.at(clientId);
  const auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  if (requestsInfo.size() == maxNumOfReqsPerClient_) {
    LOG_DEBUG(CL_MNGR,
              "Maximum number of requests per client reached" << KVLOG(maxNumOfReqsPerClient_, clientId, reqSeqNum));
    return false;
  }
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    LOG_DEBUG(CL_MNGR, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
    return false;
  }
  const auto& repliesInfo = indexToClientInfo_.at(idx).repliesInfo;
  const auto& replyIt = repliesInfo.find(reqSeqNum);
  if (replyIt != repliesInfo.end()) {
    LOG_DEBUG(CL_MNGR, "The request has been already executed" << KVLOG(clientId, reqSeqNum));
    return false;
  }
  LOG_DEBUG(CL_MNGR, "The request can become pending" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
  return true;
}

void ClientsManager::addPendingRequest(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  if (requestsInfo.find(reqSeqNum) != requestsInfo.end()) {
    LOG_WARN(CL_MNGR, "The request already exists - skip adding" << KVLOG(clientId, reqSeqNum));
    return;
  }
  requestsInfo.emplace(reqSeqNum, RequestInfo{getMonotonicTime(), cid});
  LOG_DEBUG(CL_MNGR, "Added request" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
}

void ClientsManager::markRequestAsCommitted(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    reqIt->second.committed = true;
    LOG_DEBUG(CL_MNGR, "Marked committed" << KVLOG(clientId, reqSeqNum));
    return;
  }
  LOG_DEBUG(CL_MNGR, "Request not found" << KVLOG(clientId, reqSeqNum));
}

/*
 * We have to keep the following invariant:
 * The client manager cannot hold request that are out of the bounds of a committed sequence number +
 * maxNumOfRequestsInBatch We know that the client sequence number are always ascending. In order to keep this invariant
 * we do the following: Every time we commit or execute a sequence number, we order all of our existing tracked sequence
 * numbers. Then, we count how many bigger sequence number than the given reqSequenceNumber we have. We know for sure
 * that we shouldn't have more than maxNumOfRequestsInBatch. Thus, we can safely remove them from the client manager.
 */
void ClientsManager::removeRequestsOutOfBatchBounds(NodeIdType clientId, ReqId reqSequenceNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  if (requestsInfo.find(reqSequenceNum) != requestsInfo.end()) return;
  ReqId maxReqId{0};
  for (const auto& entry : requestsInfo) {
    if (entry.first > maxReqId) maxReqId = entry.first;
  }

  // If we don't have room for the sequence number and we see that the highest sequence number is greater
  // than the given one, it means that the highest sequence number is out of the boundries and can be safely removed
  if (requestsInfo.size() == maxNumOfRequestsInBatch && maxReqId > reqSequenceNum) {
    requestsInfo.erase(maxReqId);
    metric_removed_due_to_out_of_boundaries_.Get().Inc();
  }
}
void ClientsManager::removePendingForExecutionRequest(NodeIdType clientId, ReqId reqSeqNum) {
  uint16_t idx = clientIdToIndex_.at(clientId);
  auto& requestsInfo = indexToClientInfo_.at(idx).requestsInfo;
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    requestsInfo.erase(reqIt);
    LOG_DEBUG(CL_MNGR, "Removed request" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
  }
}

void ClientsManager::clearAllPendingRequests() {
  for (ClientInfo& clientInfo : indexToClientInfo_) clientInfo.requestsInfo.clear();
  LOG_DEBUG(CL_MNGR, "Cleared pending requests for all clients");
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
  if (earliestPendingReqInfo.time != MaxTime) LOG_DEBUG(CL_MNGR, "Earliest pending request: " << KVLOG(cid));
  return earliestPendingReqInfo.time;
}

}  // namespace bftEngine::impl
