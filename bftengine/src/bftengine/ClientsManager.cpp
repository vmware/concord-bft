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

#include <chrono>
using namespace std::chrono;
using namespace concord::serialize;
namespace bftEngine::impl {
// Initialize:
// * map of client id to indices.
// * Calculate reserved pages per client.
ClientsManager::ClientsManager(std::shared_ptr<PersistentStorage> ps,
                               const std::set<NodeIdType>& proxyClients,
                               const std::set<NodeIdType>& externalClients,
                               const std::set<NodeIdType>& internalClients,
                               concordMetrics::Component& metrics)
    : ClientsManager{proxyClients, externalClients, internalClients, metrics} {
  rsiManager_.reset(new RsiDataManager(
      proxyClients.size() + externalClients.size() + internalClients.size(), maxNumOfReqsPerClient_, ps));
}
ClientsManager::ClientsManager(const std::set<NodeIdType>& proxyClients,
                               const std::set<NodeIdType>& externalClients,
                               const std::set<NodeIdType>& internalClients,
                               concordMetrics::Component& metrics)
    : myId_(ReplicaConfig::instance().replicaId),
      scratchPage_(sizeOfReservedPage(), 0),
      proxyClients_{proxyClients},
      externalClients_{externalClients},
      internalClients_{internalClients},
      maxReplySize_(ReplicaConfig::instance().getmaxReplyMessageSize()),
      maxNumOfReqsPerClient_(
          ReplicaConfig::instance().clientBatchingEnabled ? ReplicaConfig::instance().clientBatchingMaxMsgsNbr : 1),
      metrics_(metrics),
      metric_reply_inconsistency_detected_{metrics_.RegisterCounter("totalReplyInconsistenciesDetected")},
      metric_removed_due_to_out_of_boundaries_{metrics_.RegisterCounter("totalRemovedDueToOutOfBoundaries")} {
  reservedPagesPerClient_ = reservedPagesPerClient(sizeOfReservedPage(), maxReplySize_);
  for (NodeIdType i = 0; i < ReplicaConfig::instance().numReplicas + ReplicaConfig::instance().numRoReplicas; i++) {
    clientIds_.insert(i);
  }
  clientIds_.insert(proxyClients_.begin(), proxyClients_.end());
  clientIds_.insert(externalClients_.begin(), externalClients_.end());
  clientIds_.insert(internalClients_.begin(), internalClients_.end());
  ConcordAssert(clientIds_.size() >= 1);

  // For the benefit of code accessing clientsInfo_, pre-fill cliensInfo_ with a blank entry for each client to reduce
  // ambiguity between invalid client IDs and valid client IDs for which nothing stored in clientsInfo_ has been loaded
  // so far.
  for (const auto& client_id : clientIds_) {
    clientsInfo_.emplace(client_id, ClientInfo());
  }

  std::ostringstream oss;
  oss << "proxy clients: ";
  std::copy(proxyClients_.begin(), proxyClients_.end(), std::ostream_iterator<NodeIdType>(oss, " "));
  oss << "external clients: ";
  std::copy(externalClients_.begin(), externalClients_.end(), std::ostream_iterator<NodeIdType>(oss, " "));
  oss << "internal clients: ";
  std::copy(internalClients_.begin(), internalClients_.end(), std::ostream_iterator<NodeIdType>(oss, " "));
  LOG_INFO(CL_MNGR,
           oss.str() << KVLOG(sizeOfReservedPage(), reservedPagesPerClient_, maxReplySize_, maxNumOfReqsPerClient_));
}

uint32_t ClientsManager::reservedPagesPerClient(const uint32_t& sizeOfReservedPage, const uint32_t& maxReplySize) {
  uint32_t reservedPagesPerClient = maxReplySize / sizeOfReservedPage;
  if (maxReplySize % sizeOfReservedPage != 0) {
    reservedPagesPerClient++;
  }
  reservedPagesPerClient++;  // for storing client public key
  return reservedPagesPerClient;
}

// Per client:
// * load public key
// * load page of the reply header
// * fill clientInfo
// * remove pending request if loaded reply is newer
void ClientsManager::loadInfoFromReservedPages() {
  for (auto const& clientId : clientIds_) {
    if (loadReservedPage(getKeyPageId(clientId), sizeOfReservedPage(), scratchPage_.data())) {
      auto& info = clientsInfo_[clientId];
      std::istringstream iss(scratchPage_);
      concord::serialize::Serializable::deserialize(iss, info.pubKey);
      ConcordAssertGT(info.pubKey.first.length(), 0);
      KeyExchangeManager::instance().loadClientPublicKey(info.pubKey.first, info.pubKey.second, clientId, false);
    }

    if (!loadReservedPage(getReplyFirstPageId(clientId), sizeOfReservedPage(), scratchPage_.data())) continue;
    ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_.data();
    ConcordAssert(replyHeader->msgType == 0 || replyHeader->msgType == MsgCode::ClientReply);
    ConcordAssert(replyHeader->currentPrimaryId == 0);
    ConcordAssert(replyHeader->replyLength >= 0);
    ConcordAssert(replyHeader->replyLength + sizeof(ClientReplyMsgHeader) <= maxReplySize_);

    // YS TBD: Multiple replies for client batching should be sorted by incoming time
    auto& info = clientsInfo_[clientId];
    if (info.repliesInfo.size() >= maxNumOfReqsPerClient_) deleteOldestReply(clientId);
    const auto& res = info.repliesInfo.insert_or_assign(replyHeader->reqSeqNum, MinTime);
    LOG_INFO(CL_MNGR, "Added/updated reply message" << KVLOG(clientId, replyHeader->reqSeqNum, res.second));

    // Remove old pending requests
    for (auto it = info.requestsInfo.begin(); it != info.requestsInfo.end();) {
      if (it->first <= replyHeader->reqSeqNum) {
        LOG_INFO(CL_MNGR, "Remove old pending request" << KVLOG(clientId, replyHeader->reqSeqNum));
        it = info.requestsInfo.erase(it);
      } else
        it++;
    }
  }
}

bool ClientsManager::hasReply(NodeIdType clientId, ReqId reqSeqNum) const {
  try {
    const auto& repliesInfo = clientsInfo_.at(clientId).repliesInfo;
    const auto& elem = repliesInfo.find(reqSeqNum);
    const bool found = (elem != repliesInfo.end());
    if (found) LOG_DEBUG(CL_MNGR, "Reply found for" << KVLOG(clientId, reqSeqNum));
    return found;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "no info for client: " << clientId);
    return false;
  }
}

void ClientsManager::deleteOldestReply(NodeIdType clientId) {
  // YS TBD: Once multiple replies for client batching are sorted by incoming time, they could be deleted properly
  Time earliestTime = MaxTime;
  ReqId earliestReplyId = 0;

  auto& repliesInfo = clientsInfo_[clientId].repliesInfo;
  for (const auto& reply : repliesInfo) {
    if (earliestTime > reply.second) {
      earliestReplyId = reply.first;
      earliestTime = reply.second;
    }
  }
  if (earliestReplyId)
    repliesInfo.erase(earliestReplyId);
  else if (!repliesInfo.empty()) {
    // Delete reply arrived through ST
    auto const& reply = repliesInfo.cbegin();
    earliestReplyId = reply->first;
    earliestTime = reply->second;
    repliesInfo.erase(reply);
  }
  LOG_DEBUG(CL_MNGR,
            "Deleted reply message" << KVLOG(
                clientId, earliestReplyId, earliestTime.time_since_epoch().count(), repliesInfo.size()));
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
                                                                                     uint32_t rsiLength,
                                                                                     uint32_t executionResult) {
  ClientInfo& c = clientsInfo_[clientId];
  if (c.repliesInfo.size() >= maxNumOfReqsPerClient_) deleteOldestReply(clientId);
  if (c.repliesInfo.size() > maxNumOfReqsPerClient_) {
    LOG_FATAL(CL_MNGR,
              "More than maxNumOfReqsPerClient_ items in repliesInfo"
                  << KVLOG(c.repliesInfo.size(), maxNumOfReqsPerClient_, clientId, requestSeqNum, replyLength));
  }

  c.repliesInfo.insert_or_assign(requestSeqNum, getMonotonicTime());
  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum));
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
  const uint32_t firstPageId = getReplyFirstPageId(clientId);
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
  const uint32_t firstPageId = getReplyFirstPageId(clientId);
  LOG_DEBUG(CL_MNGR, KVLOG(clientId, requestSeqNum, firstPageId));
  loadReservedPage(firstPageId, sizeOfReservedPage(), scratchPage_.data());

  ClientReplyMsgHeader* replyHeader = (ClientReplyMsgHeader*)scratchPage_.data();
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
  auto r = std::make_unique<ClientReplyMsg>(myId_, replyHeader->replyLength);

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
  LOG_INFO(CL_MNGR, "key: " << key << " fmt: " << (uint16_t)fmt << " client: " << clientId);
  ClientInfo& info = clientsInfo_[clientId];
  info.pubKey = std::make_pair(key, fmt);
  std::string page(sizeOfReservedPage(), 0);
  std::ostringstream oss(page);
  concord::serialize::Serializable::serialize(oss, info.pubKey);
  saveReservedPage(getKeyPageId(clientId), oss.tellp(), oss.str().data());
}

bool ClientsManager::isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) const {
  try {
    const auto& info = clientsInfo_.at(clientId);
    const auto& reqIt = info.requestsInfo.find(reqSeqNum);
    if (reqIt != info.requestsInfo.end()) {
      LOG_DEBUG(CL_MNGR, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
      return true;
    }
    return false;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "no info for client: " << clientId);
    return false;
  }
}

bool ClientsManager::isPending(NodeIdType clientId, ReqId reqSeqNum) const {
  try {
    const auto& requestsInfo = clientsInfo_.at(clientId).requestsInfo;
    const auto& reqIt = requestsInfo.find(reqSeqNum);
    if (reqIt != requestsInfo.end() && !reqIt->second.committed) return true;
    return false;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "no info for client: " << clientId);
    return false;
  }
}
// Check that:
// * max number of pending requests not reached for that client.
// * request seq number is bigger than the last reply seq number.
bool ClientsManager::canBecomePending(NodeIdType clientId, ReqId reqSeqNum) const {
  try {
    const auto& info = clientsInfo_.at(clientId);
    if (info.requestsInfo.size() == maxNumOfReqsPerClient_) {
      LOG_DEBUG(CL_MNGR,
                "Maximum number of requests per client reached" << KVLOG(maxNumOfReqsPerClient_, clientId, reqSeqNum));
      return false;
    }
    const auto& reqIt = info.requestsInfo.find(reqSeqNum);
    if (reqIt != info.requestsInfo.end()) {
      LOG_DEBUG(CL_MNGR, "The request is executing right now" << KVLOG(clientId, reqSeqNum));
      return false;
    }
    const auto& replyIt = info.repliesInfo.find(reqSeqNum);
    if (replyIt != info.repliesInfo.end()) {
      LOG_DEBUG(CL_MNGR, "The request has been already executed" << KVLOG(clientId, reqSeqNum));
      return false;
    }
    LOG_DEBUG(CL_MNGR, "The request can become pending" << KVLOG(clientId, reqSeqNum, info.requestsInfo.size()));
    return true;
  } catch (const std::out_of_range& e) {
    LOG_DEBUG(CL_MNGR, "no info for client: " << clientId);
    return false;
  }
}

void ClientsManager::addPendingRequest(NodeIdType clientId, ReqId reqSeqNum, const std::string& cid) {
  auto& requestsInfo = clientsInfo_[clientId].requestsInfo;
  if (requestsInfo.find(reqSeqNum) != requestsInfo.end()) {
    LOG_WARN(CL_MNGR, "The request already exists - skip adding" << KVLOG(clientId, reqSeqNum));
    return;
  }
  requestsInfo.emplace(reqSeqNum, RequestInfo{getMonotonicTime(), cid});
  LOG_DEBUG(CL_MNGR, "Added request" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
}

void ClientsManager::markRequestAsCommitted(NodeIdType clientId, ReqId reqSeqNum) {
  auto& requestsInfo = clientsInfo_[clientId].requestsInfo;
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
  auto& requestsInfo = clientsInfo_[clientId].requestsInfo;
  if (requestsInfo.find(reqSequenceNum) != requestsInfo.end()) return;
  ReqId maxReqId{0};
  for (const auto& entry : requestsInfo) {
    if (entry.first > maxReqId) maxReqId = entry.first;
  }
  // If we don't have room for the sequence number and we see that the highest sequence number is greater
  // than the given one, it means that the highest sequence number is out of the boundries and can be safely removed
  if (requestsInfo.size() == maxNumOfRequestsInBatch && maxReqId > reqSequenceNum) {
    requestsInfo.erase(maxReqId);
    metric_removed_due_to_out_of_boundaries_++;
  }
}
void ClientsManager::removePendingForExecutionRequest(NodeIdType clientId, ReqId reqSeqNum) {
  if (!isValidClient(clientId)) return;
  auto& requestsInfo = clientsInfo_[clientId].requestsInfo;
  const auto& reqIt = requestsInfo.find(reqSeqNum);
  if (reqIt != requestsInfo.end()) {
    requestsInfo.erase(reqIt);
    LOG_DEBUG(CL_MNGR, "Removed request" << KVLOG(clientId, reqSeqNum, requestsInfo.size()));
  }
}

void ClientsManager::clearAllPendingRequests() {
  for (auto& info : clientsInfo_) info.second.requestsInfo.clear();
  LOG_DEBUG(CL_MNGR, "Cleared pending requests for all clients");
}

// Iterate over all clients and choose the earliest pending request.
Time ClientsManager::infoOfEarliestPendingRequest(std::string& cid) const {
  Time earliestTime = MaxTime;
  RequestInfo earliestPendingReqInfo{MaxTime, std::string()};
  for (const auto& info : clientsInfo_) {
    for (const auto& req : info.second.requestsInfo) {
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

// Iterate over all clients and log the ones that have not been committed for more than threshold milliseconds.
void ClientsManager::logAllPendingRequestsExceedingThreshold(const int64_t threshold, const Time& currTime) const {
  int numExceeding = 0;
  for (const auto& info : clientsInfo_) {
    for (const auto& req : info.second.requestsInfo) {
      // Don't take into account already committed requests
      if ((req.second.time != MinTime) && (!req.second.committed)) {
        const auto delayed = duration_cast<milliseconds>(currTime - req.second.time).count();
        const auto& CID = req.second.cid;
        if (delayed > threshold) {
          LOG_INFO(VC_LOG, "Request exceeding threshold:" << KVLOG(CID, delayed));
          numExceeding++;
        }
      }
    }
  }
  if (numExceeding) {
    LOG_INFO(VC_LOG, "Total Client request with more than " << threshold << "ms delay: " << numExceeding);
  }
}

}  // namespace bftEngine::impl
