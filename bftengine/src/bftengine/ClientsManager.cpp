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
#include "bftengine/KeyExchangeManager.hpp"
#include "Serializable.h"
#include "PersistentStorageImp.hpp"
#include "ReplicasInfo.hpp"

#include <chrono>
using namespace std::chrono;
using namespace concord::serialize;
namespace bftEngine::impl {

/*************************** Class ClientsManager::RequestsInfo ***************************/

void ClientsManager::RequestsInfo::emplaceSafe(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid) {
  if (requestsMap_.find(reqSeqNum) != requestsMap_.end()) {
    LOG_WARN(CL_MNGR, "The request already exists - skip adding" << KVLOG(clientId, reqSeqNum));
    return;
  }
  const lock_guard<mutex> lock(requestsMapMutex_);
  requestsMap_.emplace(reqSeqNum, RequestInfo{getMonotonicTime(), cid});
  LOG_DEBUG(CL_MNGR, "Added request" << KVLOG(clientId, reqSeqNum, requestsMap_.size()));
}

bool ClientsManager::RequestsInfo::findSafe(ReqId reqSeqNum) {
  const lock_guard<mutex> lock(requestsMapMutex_);
  return (requestsMap_.find(reqSeqNum) != requestsMap_.end());
}

bool ClientsManager::RequestsInfo::removeRequestsOutOfBatchBoundsSafe(NodeIdType clientId, ReqId reqSequenceNum) {
  ReqId maxReqId{0};
  if (requestsMap_.find(reqSequenceNum) != requestsMap_.end()) return false;

  const lock_guard<mutex> lock(requestsMapMutex_);
  for (const auto& reqInfo : requestsMap_)
    if (reqInfo.first > maxReqId) maxReqId = reqInfo.first;

  if (requestsMap_.size() == maxNumOfRequestsInBatch && maxReqId > reqSequenceNum) {
    // If we don't have room for the sequence number, and we see that the highest sequence number is greater
    // than the given one, it means that the highest sequence number is out of the boundaries and can be safely removed
    requestsMap_.erase(maxReqId);
    return true;
  }
  return false;
}

void ClientsManager::RequestsInfo::removeOldPendingReqsSafe(NodeIdType clientId, ReqId reqSeqNum) {
  const lock_guard<mutex> lock(requestsMapMutex_);
  for (auto it = requestsMap_.begin(); it != requestsMap_.end();) {
    const auto oldSeqNum = it->first;
    if (oldSeqNum <= reqSeqNum) {
      LOG_INFO(CL_MNGR, "Remove old pending request" << KVLOG(clientId, oldSeqNum, reqSeqNum));
      it = requestsMap_.erase(it);
    } else
      it++;
  }
}

void ClientsManager::RequestsInfo::removePendingForExecutionRequestSafe(NodeIdType clientId, ReqId reqSeqNum) {
  const lock_guard<mutex> lock(requestsMapMutex_);
  const auto& reqIt = requestsMap_.find(reqSeqNum);
  if (reqIt != requestsMap_.end()) {
    requestsMap_.erase(reqIt);
    LOG_DEBUG(CL_MNGR, "Removed request" << KVLOG(clientId, reqSeqNum, requestsMap_.size()));
  }
}

void ClientsManager::RequestsInfo::clearSafe() {
  const lock_guard<mutex> lock(requestsMapMutex_);
  requestsMap_.clear();
}

bool ClientsManager::RequestsInfo::find(ReqId reqSeqNum) const {
  return (requestsMap_.find(reqSeqNum) != requestsMap_.end());
}

bool ClientsManager::RequestsInfo::isPending(ReqId reqSeqNum) const {
  const auto& reqIt = requestsMap_.find(reqSeqNum);
  if (reqIt != requestsMap_.end() && !reqIt->second.committed) return true;
  return false;
}

void ClientsManager::RequestsInfo::markRequestAsCommitted(NodeIdType clientId, ReqId reqSeqNum) {
  const auto& reqIt = requestsMap_.find(reqSeqNum);
  if (reqIt != requestsMap_.end()) {
    reqIt->second.committed = true;
    LOG_DEBUG(CL_MNGR, "Marked committed" << KVLOG(clientId, reqSeqNum));
    return;
  }
  LOG_DEBUG(CL_MNGR, "Request not found" << KVLOG(clientId, reqSeqNum));
}

void ClientsManager::RequestsInfo::infoOfEarliestPendingRequest(Time& earliestTime,
                                                                RequestInfo& earliestPendingReqInfo) const {
  for (const auto& req : requestsMap_) {
    // Don't take into account already committed requests
    if ((req.second.time != MinTime) && (earliestTime > req.second.time) && (!req.second.committed)) {
      earliestPendingReqInfo = req.second;
      earliestTime = earliestPendingReqInfo.time;
    }
  }
}

void ClientsManager::RequestsInfo::logAllPendingRequestsExceedingThreshold(const int64_t threshold,
                                                                           const Time& currTime,
                                                                           int& numExceeding) const {
  for (const auto& req : requestsMap_) {
    // Don't take into account already committed requests
    if ((req.second.time != MinTime) && (!req.second.committed)) {
      const auto delayed = duration_cast<milliseconds>(currTime - req.second.time).count();
      if (delayed > threshold) {
        LOG_INFO(CL_MNGR, "Request exceeding threshold:" << KVLOG(req.second.cid, delayed));
        numExceeding++;
      }
    }
  }
}

/*************************** Class ClientsManager::RepliesInfo ***************************/

void ClientsManager::RepliesInfo::deleteReplyIfNeededSafe(NodeIdType clientId,
                                                          ReqId reqSeqNum,
                                                          uint16_t maxNumOfReqsPerClient,
                                                          uint16_t reqIndex) {
  ReqId savedReplyId = 0;
  if (repliesBiMap_.size() > maxNumOfReqsPerClient) {
    LOG_FATAL(CL_MNGR,
              "More than maxNumOfReqsPerClient_ items in repliesInfo"
                  << KVLOG(repliesBiMap_.size(), maxNumOfReqsPerClient, clientId));
  }

  if (repliesBiMap_.right.find(reqIndex) != repliesBiMap_.right.end()) {
    savedReplyId = repliesBiMap_.right.at(reqIndex);
    if (savedReplyId > reqSeqNum) {
      LOG_WARN(CL_MNGR, "A newer reply was already saved" << KVLOG(clientId, reqSeqNum, savedReplyId, reqIndex));
    }
  }

  if (savedReplyId) {
    const lock_guard<mutex> lock(repliesMapMutex_);
    repliesBiMap_.left.erase(savedReplyId);
    LOG_DEBUG(CL_MNGR, "Deleted reply message" << KVLOG(clientId, savedReplyId, reqIndex, repliesBiMap_.size()));
  }
}

void ClientsManager::RepliesInfo::insertOrAssignSafe(ReqId reqSeqNum, uint16_t reqIndexInBatch) {
  const lock_guard<mutex> lock(repliesMapMutex_);
  repliesBiMap_.insert(boost::bimaps::bimap<ReqId, uint16_t>::value_type(reqSeqNum, reqIndexInBatch));
}

bool ClientsManager::RepliesInfo::findSafe(ReqId reqSeqNum) {
  const lock_guard<mutex> lock(repliesMapMutex_);
  return (repliesBiMap_.left.find(reqSeqNum) != repliesBiMap_.left.end());
}

bool ClientsManager::RepliesInfo::find(ReqId reqSeqNum) const {
  return (repliesBiMap_.left.find(reqSeqNum) != repliesBiMap_.left.end());
}

uint16_t ClientsManager::RepliesInfo::getIndex(ReqId reqSeqNum) const {
  const auto& replyIt = repliesBiMap_.left.find(reqSeqNum);
  return (replyIt != repliesBiMap_.left.end()) ? replyIt->second : 0;
}

/*************************** Class ClientsManager ***************************/

// Initialize:
// * map of client id to indices.
// * Calculate reserved pages per client.
ClientsManager::ClientsManager(std::shared_ptr<PersistentStorage> ps,
                               const std::set<NodeIdType>& proxyClients,
                               const std::set<NodeIdType>& externalClients,
                               const std::set<NodeIdType>& clientServices,
                               const std::set<NodeIdType>& internalClients,
                               concordMetrics::Component& metrics)
    : ClientsManager{proxyClients, externalClients, clientServices, internalClients, metrics} {
  rsiManager_.reset(
      new RsiDataManager(proxyClients.size() + externalClients.size() + internalClients.size() + clientServices.size(),
                         maxNumOfReqsPerClient_,
                         ps));
}
ClientsManager::ClientsManager(const std::set<NodeIdType>& proxyClients,
                               const std::set<NodeIdType>& externalClients,
                               const std::set<NodeIdType>& clientServices,
                               const std::set<NodeIdType>& internalClients,
                               concordMetrics::Component& metrics)
    : myId_(ReplicaConfig::instance().replicaId),
      scratchPage_(sizeOfReservedPage(), 0),
      proxyClients_{proxyClients},
      externalClients_{externalClients},
      clientServices_{clientServices},
      internalClients_{internalClients},
      maxReplySize_(ReplicaConfig::instance().getmaxReplyMessageSize()),
      maxNumOfReqsPerClient_(ReplicaConfig::instance().clientBatchingEnabled &&
                                     ReplicaConfig::instance().preExecutionFeatureEnabled
                                 ? ReplicaConfig::instance().clientBatchingMaxMsgsNbr
                                 : 1),
      metrics_(metrics),
      metric_reply_inconsistency_detected_{metrics_.RegisterCounter("totalReplyInconsistenciesDetected")},
      metric_removed_due_to_out_of_boundaries_{metrics_.RegisterCounter("totalRemovedDueToOutOfBoundaries")} {
  ConcordAssert(maxNumOfReqsPerClient_ > 0);
  reservedPagesPerRequest_ = reservedPagesPerRequest(sizeOfReservedPage(), maxReplySize_);
  reservedPagesPerClient_ = reservedPagesPerClient(sizeOfReservedPage(), maxReplySize_, maxNumOfReqsPerClient_);
  for (NodeIdType i = 0; i < ReplicaConfig::instance().numReplicas + ReplicaConfig::instance().numRoReplicas; i++) {
    clientIds_.insert(i);
  }
  clientIds_.insert(proxyClients_.begin(), proxyClients_.end());
  clientIds_.insert(externalClients_.begin(), externalClients_.end());
  clientIds_.insert(clientServices_.begin(), clientServices_.end());
  clientIds_.insert(internalClients_.begin(), internalClients_.end());
  ConcordAssert(clientIds_.size() >= 1);
  uint32_t rpage = 0;
  for (const auto cid : clientIds_) {
    clientIdsToReservedPages_.emplace(cid, rpage);
    rpage++;
  }
  // For the benefit of code accessing clientsInfo_, pre-fill clientsInfo_ with a blank entry for each client to reduce
  // ambiguity between invalid client IDs and valid client IDs for which nothing stored in clientsInfo_ has been loaded
  // so far.
  for (const auto& client_id : clientIds_) {
    clientsInfo_.emplace(client_id, ClientInfo());
    clientsInfo_[client_id].requestsInfo = make_shared<RequestsInfo>();
    clientsInfo_[client_id].repliesInfo = make_shared<RepliesInfo>();
  }

  LOG_INFO(
      CL_MNGR,
      "proxy clients: " << concord::util::toString(proxyClients_, " ")
                        << "external clients: " << concord::util::toString(externalClients_, " ")
                        << "internal clients: " << concord::util::toString(internalClients_, " ")
                        << KVLOG(sizeOfReservedPage(), reservedPagesPerClient_, maxReplySize_, maxNumOfReqsPerClient_));
}

uint32_t ClientsManager::reservedPagesPerRequest(uint32_t sizeOfReservedPage, uint32_t maxReplySize) {
  uint32_t reservedPagesPerReq = maxReplySize / sizeOfReservedPage;
  if (maxReplySize % sizeOfReservedPage != 0) {
    reservedPagesPerReq++;
  }
  return reservedPagesPerReq;
}

uint32_t ClientsManager::reservedPagesPerClient(uint32_t sizeOfReservedPage,
                                                uint32_t maxReplySize,
                                                uint16_t maxNumReqPerClient) {
  uint32_t reservedPagesPerReq = reservedPagesPerRequest(sizeOfReservedPage, maxReplySize);
  uint32_t reservedPagesPerClient = reservedPagesPerReq * maxNumReqPerClient;
  reservedPagesPerClient++;  // for storing client public key
  return reservedPagesPerClient;
}

// Per client:
// * load public key
// * load page of the reply header
// * fill clientInfo
// * remove pending request if loaded reply is newer
void ClientsManager::loadInfoFromReservedPages() {
  for (auto const& [clientId, clientInfo] : clientsInfo_) {
    if (internalClients_.find(clientId) != internalClients_.end()) continue;
    if (loadReservedPage(getKeyPageId(clientId), sizeOfReservedPage(), scratchPage_.data())) {
      std::istringstream iss(scratchPage_);
      concord::serialize::Serializable::deserialize(iss, clientInfo.pubKey);
      ConcordAssertGT(clientInfo.pubKey.first.length(), 0);
      KeyExchangeManager::instance().loadClientPublicKey(
          clientInfo.pubKey.first, clientInfo.pubKey.second, clientId, false);
    }

    for (uint16_t indexInBatch = 0; indexInBatch < maxNumOfReqsPerClient_; indexInBatch++) {
      const auto pageOffset = getReplyFirstPageId(clientId) + indexInBatch * reservedPagesPerRequest_;
      if (!loadReservedPage(pageOffset, sizeOfReservedPage(), scratchPage_.data())) continue;
      ClientReplyMsgHeader* replyHeader = reinterpret_cast<ClientReplyMsgHeader*>(scratchPage_.data());
      ConcordAssert(replyHeader->msgType == 0 || replyHeader->msgType == MsgCode::ClientReply);
      ConcordAssert(replyHeader->currentPrimaryId == 0);
      ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplySize_);

      clientInfo.repliesInfo->deleteReplyIfNeededSafe(
          clientId, replyHeader->reqSeqNum, maxNumOfReqsPerClient_, indexInBatch);
      clientInfo.repliesInfo->insertOrAssignSafe(replyHeader->reqSeqNum, indexInBatch);

      LOG_INFO(CL_MNGR,
               "Added/updated reply message" << KVLOG(clientId, replyHeader->reqSeqNum, indexInBatch, pageOffset));
      clientInfo.requestsInfo->removeOldPendingReqsSafe(clientId, replyHeader->reqSeqNum);
    }
  }
}

bool ClientsManager::hasReply(NodeIdType clientId, ReqId reqSeqNum) {
  try {
    const bool found = clientsInfo_.at(clientId).repliesInfo->findSafe(reqSeqNum);
    if (found) {
      LOG_DEBUG(CL_MNGR, "Reply found for" << KVLOG(clientId, reqSeqNum));
    }
    return found;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSeqNum));
    return false;
  }
}

void ClientsManager::deleteReplyIfNeeded(NodeIdType clientId, uint16_t indexInBatch, ReqId newReqSeqNum) {
  try {
    clientsInfo_[clientId].repliesInfo->deleteReplyIfNeededSafe(
        clientId, newReqSeqNum, maxNumOfReqsPerClient_, indexInBatch);
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, newReqSeqNum));
  }
}

// Reference the ClientInfo of the corresponding client:
// * set last reply seq num to the seq num of the request we reply to.
// * set reply time to `now`.
// * allocate new ClientReplyMsg
// * calculate: num of pages, size of last page.
// * save the reply to the reserved pages.
std::unique_ptr<ClientReplyMsg> ClientsManager::allocateNewReplyMsgAndWriteToStorage(NodeIdType clientId,
                                                                                     ReqId requestSeqNum,
                                                                                     uint16_t currentPrimaryId,
                                                                                     char* reply,
                                                                                     uint32_t replyLength,
                                                                                     uint16_t reqIndexInBatch,
                                                                                     uint32_t rsiLength,
                                                                                     uint32_t executionResult) {
  ConcordAssert(reqIndexInBatch < maxNumOfReqsPerClient_);
  if (!isValidClient(clientId)) {
    LOG_DEBUG(CL_MNGR, "Invalid client id" << KVLOG(clientId, requestSeqNum));
    return nullptr;
  }
  uint16_t requestOffset = reqIndexInBatch * reservedPagesPerRequest_;
  clientsInfo_[clientId].repliesInfo->deleteReplyIfNeededSafe(
      clientId, requestSeqNum, maxNumOfReqsPerClient_, reqIndexInBatch);
  clientsInfo_[clientId].repliesInfo->insertOrAssignSafe(requestSeqNum, reqIndexInBatch);

  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum, reqIndexInBatch, requestOffset));
  auto r = std::make_unique<ClientReplyMsg>(myId_, requestSeqNum, reply, replyLength - rsiLength, executionResult);

  // At this point, the rsi data is not part of the reply
  uint32_t commonMsgSize = r->size();
  uint32_t numOfPages = commonMsgSize / sizeOfReservedPage();
  uint32_t sizeLastPage = sizeOfReservedPage();
  if (numOfPages > reservedPagesPerClient_) {
    LOG_FATAL(CL_MNGR,
              "Client reply is larger than reservedPagesPerClient_ allows" << KVLOG(
                  clientId, requestSeqNum, reservedPagesPerClient_ * sizeOfReservedPage(), replyLength - rsiLength));
    ConcordAssert(false);
  }

  if (commonMsgSize % sizeOfReservedPage() != 0) {
    numOfPages++;
    sizeLastPage = commonMsgSize % sizeOfReservedPage();
  }

  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum, numOfPages, sizeLastPage));
  // write reply message to reserved pages
  const uint32_t firstPageId = getReplyFirstPageId(clientId) + requestOffset;
  for (uint32_t i = 0; i < numOfPages; i++) {
    const char* ptrPage = r->body() + i * sizeOfReservedPage();
    const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage() : sizeLastPage);
    saveReservedPage(firstPageId + i, sizePage, ptrPage);
  }
  // now save the RSI in the rsiManager, if this ClientsManager has one.
  if (rsiManager_) {
    rsiManager_->setRsiForClient(clientId, requestSeqNum, std::string(reply + commonMsgSize, rsiLength));
  }
  // we cannot set the RSI metadata before saving the reply to the reserved paged, hence save it now.
  r->setReplicaSpecificInfoLength(rsiLength);

  // write currentPrimaryId to message (we don't store the currentPrimaryId in the reserved pages)
  r->setPrimaryId(currentPrimaryId);
  LOG_DEBUG(CL_MNGR, "Returns reply with hash=" << r->debugHash() << KVLOG(clientId, requestSeqNum));
  return r;
}

// * load client reserve page to scratchPage
// * cast to ClientReplyMsgHeader and validate.
// * calculate: reply msg size, num of pages, size of last page.
// * allocate new ClientReplyMsg.
// * copy reply from reserved pages to ClientReplyMsg.
// * set primary id.
std::unique_ptr<ClientReplyMsg> ClientsManager::allocateReplyFromSavedOne(NodeIdType clientId,
                                                                          ReqId requestSeqNum,
                                                                          uint16_t currentPrimaryId) {
  if (!isValidClient(clientId)) {
    LOG_DEBUG(CL_MNGR, "Invalid client id" << KVLOG(clientId, requestSeqNum));
    return nullptr;
  }
  auto& repliesInfo = clientsInfo_[clientId].repliesInfo;
  if (!repliesInfo->findSafe(requestSeqNum)) {
    LOG_WARN(CL_MNGR, "Reply is not found for specified request" << KVLOG(clientId, requestSeqNum));
  }
  uint16_t reqIndexInBatch = repliesInfo->getIndex(requestSeqNum);
  uint16_t requestOffset = reqIndexInBatch * reservedPagesPerRequest_;
  const uint32_t firstPageId = getReplyFirstPageId(clientId) + requestOffset;
  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum, firstPageId, requestOffset));
  loadReservedPage(firstPageId, sizeOfReservedPage(), scratchPage_.data());

  ClientReplyMsgHeader* replyHeader = reinterpret_cast<ClientReplyMsgHeader*>(scratchPage_.data());
  ConcordAssert(replyHeader->msgType == MsgCode::ClientReply);
  ConcordAssert(replyHeader->currentPrimaryId == 0);
  ConcordAssert(replyHeader->replyLength > 0);
  ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplySize_);

  uint32_t replyMsgSize = sizeof(ClientReplyMsgHeader) + replyHeader->replyLength;
  uint32_t numOfPages = replyMsgSize / sizeOfReservedPage();
  uint32_t sizeLastPage = sizeOfReservedPage();
  if (replyMsgSize % sizeOfReservedPage() != 0) {
    numOfPages++;
    sizeLastPage = replyMsgSize % sizeOfReservedPage();
  }
  LOG_DEBUG(CL_MNGR, KVLOG(clientId, numOfPages, sizeLastPage));
  auto r = std::make_unique<ClientReplyMsg>(myId_, replyHeader->replyLength, replyHeader->result);

  // load reply message from reserved pages
  for (uint32_t i = 0; i < numOfPages; i++) {
    char* const ptrPage = r->body() + i * sizeOfReservedPage();
    const uint32_t sizePage = ((i < numOfPages - 1) ? sizeOfReservedPage() : sizeLastPage);
    loadReservedPage(firstPageId + i, sizePage, ptrPage);
  }

  // Load the RSI data from persistent storage, if an RSI manager is in use.
  if (rsiManager_) {
    auto rsiItem = rsiManager_->getRsiForClient(clientId, requestSeqNum);
    auto rsiSize = rsiItem.data().size();
    if (rsiSize > 0) {
      auto commDataLength = r->replyLength();
      r->setReplyLength(r->replyLength() + rsiSize);
      memcpy(r->replyBuf() + commDataLength, rsiItem.data().data(), rsiSize);
      r->setReplicaSpecificInfoLength(rsiSize);
    }
  }
  const auto& replySeqNum = r->reqSeqNum();
  if (replySeqNum != requestSeqNum) {
    if (maxNumOfReqsPerClient_ == 1) {
      metric_reply_inconsistency_detected_++;
      LOG_FATAL(CL_MNGR,
                "The client reserved page does not contain a reply for specified request"
                    << KVLOG(clientId, replySeqNum, requestSeqNum));
      ConcordAssert(false);
    }
    // YS TBD: Fix this for client batching with a proper ordering of incoming requests
    LOG_INFO(CL_MNGR,
             "The client reserved page does not contain a reply for specified request; skipping"
                 << KVLOG(clientId, replySeqNum, requestSeqNum));
    return nullptr;
  }

  r->setPrimaryId(currentPrimaryId);
  LOG_DEBUG(CL_MNGR, "Returns reply with hash=" << r->debugHash());
  return r;
}

void ClientsManager::setClientPublicKey(NodeIdType clientId,
                                        const std::string& key,
                                        concord::util::crypto::KeyFormat fmt) {
  try {
    LOG_INFO(CL_MNGR, "key: " << key << " fmt: " << (uint16_t)fmt << " client: " << clientId);
    ClientInfo& info = clientsInfo_[clientId];
    info.pubKey = std::make_pair(key, fmt);
    std::string page(sizeOfReservedPage(), 0);
    std::ostringstream oss(page);
    concord::serialize::Serializable::serialize(oss, info.pubKey);
    saveReservedPage(getKeyPageId(clientId), oss.tellp(), oss.str().data());
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId));
  }
}

bool ClientsManager::isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) {
  try {
    const bool found = clientsInfo_.at(clientId).requestsInfo->findSafe(reqSeqNum);
    if (found) {
      LOG_DEBUG(CL_MNGR, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
    }
    return found;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSeqNum));
    return false;
  }
}

bool ClientsManager::isPending(NodeIdType clientId, ReqId reqSeqNum) const {
  try {
    return clientsInfo_.at(clientId).requestsInfo->isPending(reqSeqNum);
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSeqNum));
    return false;
  }
}

// Check that:
// * max number of pending requests not reached for that client.
// * request seq number is bigger than the last reply seq number.
bool ClientsManager::canBecomePending(NodeIdType clientId, ReqId reqSeqNum) const {
  try {
    const auto& clientInfo = clientsInfo_.at(clientId);
    ReqId requestsNum = clientInfo.requestsInfo->size();
    if (requestsNum == maxNumOfReqsPerClient_) {
      LOG_DEBUG(CL_MNGR,
                "Maximum number of requests per client reached" << KVLOG(maxNumOfReqsPerClient_, clientId, reqSeqNum));
      return false;
    }
    if (clientInfo.requestsInfo->find(reqSeqNum)) {
      LOG_DEBUG(CL_MNGR, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
      return false;
    }
    if (clientInfo.repliesInfo->find(reqSeqNum)) {
      LOG_DEBUG(CL_MNGR, "The request has been already executed" << KVLOG(clientId, reqSeqNum));
      return false;
    }
    LOG_DEBUG(CL_MNGR, "The request can become pending" << KVLOG(clientId, reqSeqNum, requestsNum));
    return true;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSeqNum));
    return false;
  }
}

void ClientsManager::addPendingRequest(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid) {
  try {
    clientsInfo_[clientId].requestsInfo->emplaceSafe(clientId, reqSeqNum, cid);
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSeqNum));
  }
}

void ClientsManager::markRequestAsCommitted(NodeIdType clientId, ReqId reqSeqNum) {
  try {
    clientsInfo_[clientId].requestsInfo->markRequestAsCommitted(clientId, reqSeqNum);
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSeqNum));
  }
}

/*
 * We have to keep the following invariant:
 * The client manager cannot hold request that are out of the bounds of a committed sequence number +
 * maxNumOfRequestsInBatch We know that the client sequence number is always ascending. In order to keep this invariant
 * we do the following: every time we commit or execute a sequence number, we order all of our existing tracked sequence
 * numbers. Then, we count how many bigger sequence number than the given reqSequenceNumber we have. We know for sure
 * that we shouldn't have more than maxNumOfRequestsInBatch. Thus, we can safely remove them from the client manager.
 */
void ClientsManager::removeRequestsOutOfBatchBounds(NodeIdType clientId, ReqId reqSequenceNum) {
  try {
    if (clientsInfo_[clientId].requestsInfo->removeRequestsOutOfBatchBoundsSafe(clientId, reqSequenceNum))
      metric_removed_due_to_out_of_boundaries_++;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "No info found for client" << KVLOG(clientId, reqSequenceNum));
  }
}

void ClientsManager::removePendingForExecutionRequest(NodeIdType clientId, ReqId reqSeqNum) {
  if (!isValidClient(clientId)) return;
  clientsInfo_[clientId].requestsInfo->removePendingForExecutionRequestSafe(clientId, reqSeqNum);
}

void ClientsManager::clearAllPendingRequests() {
  for (auto& clientInfo : clientsInfo_) clientInfo.second.requestsInfo->clearSafe();
  LOG_DEBUG(CL_MNGR, "Cleared pending requests for all clients");
}

// Iterate over all clients and choose the earliest pending request.
Time ClientsManager::infoOfEarliestPendingRequest(std::string& cid) const {
  Time earliestTime = MaxTime;
  RequestInfo earliestPendingReqInfo{MaxTime, std::string()};
  for (const auto& clientInfo : clientsInfo_)
    clientInfo.second.requestsInfo->infoOfEarliestPendingRequest(earliestTime, earliestPendingReqInfo);
  cid = earliestPendingReqInfo.cid;
  if (earliestPendingReqInfo.time != MaxTime) {
    LOG_DEBUG(CL_MNGR, "Earliest pending request: " << KVLOG(cid));
  }
  return earliestPendingReqInfo.time;
}

// Iterate over all clients and log the ones that have not been committed for more than threshold milliseconds.
void ClientsManager::logAllPendingRequestsExceedingThreshold(const int64_t threshold, const Time& currTime) const {
  int numExceeding = 0;
  for (const auto& clientInfo : clientsInfo_)
    clientInfo.second.requestsInfo->logAllPendingRequestsExceedingThreshold(threshold, currTime, numExceeding);
  if (numExceeding) {
    LOG_INFO(CL_MNGR, "Total Client request with more than " << threshold << "ms delay: " << numExceeding);
  }
}

bool ClientsManager::isInternal(NodeIdType clientId) const {
  return internalClients_.find(clientId) != internalClients_.end();
}

}  // namespace bftEngine::impl
