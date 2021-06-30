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

#include <algorithm>
#include <set>
#include <vector>

#include "Logger.hpp"
#include "PrimitiveTypes.hpp"
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/ViewChangeMsg.hpp"
#include "messages/NewViewMsg.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "CryptoManager.hpp"

namespace bftEngine {
namespace impl {

bool ViewsManager::PrevViewInfo::equals(const PrevViewInfo& other) const {
  if (other.hasAllRequests != hasAllRequests) return false;
  if ((other.prePrepare && !prePrepare) || (!other.prePrepare && prePrepare)) return false;
  if ((other.prepareFull && !prepareFull) || (!other.prepareFull && prepareFull)) return false;
  bool res1 = prePrepare ? (other.prePrepare->equals(*prePrepare)) : true;
  bool res2 = prepareFull ? (other.prepareFull->equals(*prepareFull)) : true;
  return res1 && res2;
}

uint32_t ViewsManager::PrevViewInfo::maxSize() {
  return maxMessageSizeInLocalBuffer<PrePrepareMsg>() + maxMessageSizeInLocalBuffer<PrepareFullMsg>() +
         sizeof(hasAllRequests);
}

ViewsManager::ViewsManager(const ReplicasInfo* const r)
    : replicasInfo(r),
      N(r->numberOfReplicas()),
      F(r->fVal()),
      C(r->cVal()),
      myId(r->myId()),
      complainedReplicas(r->fVal()),
      complainedReplicasForHigherView(r->fVal()) {
  ConcordAssert(N == (3 * F + 2 * C + 1));

  viewChangeSafetyLogic = new ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  stat = Stat::IN_VIEW;

  myCurrentView = 0;
  myLatestActiveView = 0;
  myLatestPendingView = 0;
  viewChangeMessages = new ViewChangeMsg*[N];
  newViewMessages = new NewViewMsg*[N];
  viewChangeMsgsOfPendingView = new ViewChangeMsg*[N];

  for (uint16_t i = 0; i < N; i++) {
    viewChangeMessages[i] = nullptr;
    newViewMessages[i] = nullptr;
    viewChangeMsgsOfPendingView[i] = nullptr;
  }

  newViewMsgOfOfPendingView = nullptr;

  minRestrictionOfPendingView = 0;
  maxRestrictionOfPendingView = 0;

  for (uint16_t i = 0; i < kWorkWindowSize; i++) {
    restrictionsOfPendingView[i].isNull = true;
    restrictionsOfPendingView[i].digest.makeZero();
    prePrepareMsgsOfRestrictions[i] = nullptr;
  }

  lowerBoundStableForPendingView = 0;

  debugHighestKnownStable = 0;
  debugHighestViewNumberPassedByClient = 0;
}

ViewsManager::~ViewsManager() {
  delete viewChangeSafetyLogic;

  for (uint16_t i = 0; i < N; i++) {
    delete viewChangeMessages[i];
    delete newViewMessages[i];
    delete viewChangeMsgsOfPendingView[i];
  }
  delete[] viewChangeMessages;
  delete[] newViewMessages;
  delete[] viewChangeMsgsOfPendingView;

  delete newViewMsgOfOfPendingView;

  if (minRestrictionOfPendingView > 0) {
    // delete messages from prePrepareMsgsOfRestrictions which are not in
    // collectionOfPrePrepareMsgs (we don't want to delete twice)
    for (SeqNum i = minRestrictionOfPendingView; i <= maxRestrictionOfPendingView; i++) {
      int64_t idx = (i - minRestrictionOfPendingView);
      if (prePrepareMsgsOfRestrictions[idx] == nullptr) continue;
      if (collectionOfPrePrepareMsgs.count(i) == 0 ||
          collectionOfPrePrepareMsgs.at(i) != prePrepareMsgsOfRestrictions[idx])
        delete prePrepareMsgsOfRestrictions[idx];
    }
  }

  for (auto it : collectionOfPrePrepareMsgs) delete it.second;
}

ViewsManager* ViewsManager::createOutsideView(const ReplicasInfo* const r,
                                              ViewNum lastActiveView,
                                              SeqNum lastStable,
                                              SeqNum lastExecuted,
                                              SeqNum stableLowerBound,
                                              ViewChangeMsg* myLastViewChange,
                                              std::vector<PrevViewInfo>& elementsOfPrevView) {
  // check arguments
  ConcordAssert(lastActiveView >= 0);
  ConcordAssert(lastStable >= 0);
  ConcordAssert(lastExecuted >= lastStable);
  ConcordAssert(stableLowerBound >= 0 && lastStable >= stableLowerBound);
  if (lastActiveView > 0) {
    ConcordAssert(myLastViewChange != nullptr);
    ConcordAssert(myLastViewChange->idOfGeneratedReplica() == r->myId());
    ConcordAssert(myLastViewChange->newView() == lastActiveView);
    ConcordAssert(myLastViewChange->lastStable() <= lastStable);
  } else {
    ConcordAssert(myLastViewChange == nullptr);
  }

  ConcordAssert(elementsOfPrevView.size() <= kWorkWindowSize);
  for (size_t i = 0; i < elementsOfPrevView.size(); i++) {
    const PrevViewInfo& pvi = elementsOfPrevView[i];
    ConcordAssert(pvi.prePrepare != nullptr && pvi.prePrepare->viewNumber() == lastActiveView);
    ConcordAssert(pvi.hasAllRequests == true);
    ConcordAssert(pvi.prepareFull == nullptr || pvi.prepareFull->viewNumber() == lastActiveView);
    ConcordAssert(pvi.prepareFull == nullptr || pvi.prepareFull->seqNumber() == pvi.prePrepare->seqNumber());
  }

  ViewsManager* v = new ViewsManager(r);
  ConcordAssert(v->stat == Stat::IN_VIEW);
  ConcordAssert(v->myLatestActiveView == 0);

  v->myLatestActiveView = lastActiveView;
  v->myLatestPendingView = lastActiveView;
  v->viewChangeMessages[v->myId] = myLastViewChange;
  v->lowerBoundStableForPendingView = stableLowerBound;

  v->exitFromCurrentView(lastStable, lastExecuted, elementsOfPrevView);

  // check the new ViewsManager
  ConcordAssert(v->stat == Stat::NO_VIEW);
  ConcordAssert(v->myLatestActiveView == lastActiveView);
  ConcordAssert(v->myLatestPendingView == lastActiveView);
  for (size_t i = 0; i < v->N; i++) {
    ConcordAssert(v->viewChangeMessages[i] == nullptr || i == v->myId);
    ConcordAssert(v->newViewMessages[i] == nullptr);
    ConcordAssert(v->viewChangeMsgsOfPendingView[i] == nullptr);
  }
  ConcordAssert(v->viewChangeMessages[v->myId]->idOfGeneratedReplica() == r->myId());
  ConcordAssert(v->viewChangeMessages[v->myId]->newView() == lastActiveView + 1);
  ConcordAssert(v->viewChangeMessages[v->myId]->lastStable() == lastStable);
  ConcordAssert(v->newViewMsgOfOfPendingView == nullptr);

  return v;
}

ViewsManager* ViewsManager::createInsideViewZero(const ReplicasInfo* const r) {
  ViewsManager* v = new ViewsManager(r);
  return v;
}

ViewsManager* ViewsManager::createInsideView(const ReplicasInfo* const r,
                                             ViewNum view,
                                             SeqNum stableLowerBound,
                                             NewViewMsg* newViewMsg,
                                             ViewChangeMsg* myLastViewChange,
                                             std::vector<ViewChangeMsg*> viewChangeMsgs) {
  // check arguments
  ConcordAssert(view >= 1);
  ConcordAssert(stableLowerBound >= 0);
  ConcordAssert(newViewMsg != nullptr);
  ConcordAssert(newViewMsg->newView() == view);
  ConcordAssert(viewChangeMsgs.size() == (size_t)(2 * r->fVal() + 2 * r->cVal() + 1));
  std::set<ReplicaId> replicasWithVCMsg;
  for (size_t i = 0; i < viewChangeMsgs.size(); i++) {
    ConcordAssert(viewChangeMsgs[i] != nullptr);
    ConcordAssert(viewChangeMsgs[i]->newView() == view);
    Digest msgDigest;
    viewChangeMsgs[i]->getMsgDigest(msgDigest);
    ReplicaId idOfGenReplica = viewChangeMsgs[i]->idOfGeneratedReplica();
    ConcordAssert(newViewMsg->includesViewChangeFromReplica(idOfGenReplica, msgDigest) == true);
    replicasWithVCMsg.insert(idOfGenReplica);
  }
  ConcordAssert(replicasWithVCMsg.size() == viewChangeMsgs.size());
  ConcordAssert(replicasWithVCMsg.count(r->myId()) == 1 || myLastViewChange != nullptr);
  if (myLastViewChange != nullptr) {
    ConcordAssert(myLastViewChange->newView() == view);
    Digest msgDigest;
    myLastViewChange->getMsgDigest(msgDigest);
    ConcordAssert(myLastViewChange->idOfGeneratedReplica() == r->myId());
    ConcordAssert(newViewMsg->includesViewChangeFromReplica(r->myId(), msgDigest) == false);
  }

  ViewsManager* v = new ViewsManager(r);

  ConcordAssert(v->stat == Stat::IN_VIEW);
  v->myLatestActiveView = view;
  v->myLatestPendingView = view;

  ConcordAssert(v->collectionOfPrePrepareMsgs.empty());

  for (size_t i = 0; i < viewChangeMsgs.size(); i++) {
    ViewChangeMsg* vcm = viewChangeMsgs[i];

    ConcordAssert(v->viewChangeMsgsOfPendingView[vcm->idOfGeneratedReplica()] == nullptr);
    v->viewChangeMsgsOfPendingView[vcm->idOfGeneratedReplica()] = vcm;
  }

  v->newViewMsgOfOfPendingView = newViewMsg;

  if (v->viewChangeMsgsOfPendingView[v->myId] == nullptr) {
    ConcordAssert(myLastViewChange != nullptr);
    v->viewChangeMessages[v->myId] = myLastViewChange;
  } else if (myLastViewChange != nullptr)
    delete myLastViewChange;

  v->lowerBoundStableForPendingView = stableLowerBound;

  return v;
}

void ViewsManager::setHigherView(ViewNum higherViewNum) {
  ConcordAssertLT(myCurrentView, higherViewNum);
  myCurrentView = higherViewNum;
}

ViewChangeMsg* ViewsManager::getMyLatestViewChangeMsg() const {
  ViewChangeMsg* vc = viewChangeMsgsOfPendingView[myId];
  if (vc == nullptr) vc = viewChangeMessages[myId];

  ConcordAssert(vc != nullptr || myLatestPendingView == 0);

  return vc;
}

bool ViewsManager::add(NewViewMsg* m) {
  const uint16_t sId = m->senderId();
  const ViewNum v = m->newView();

  // these asserts should be verified before accepting this message
  ConcordAssert(sId != myId);
  ConcordAssert(sId == replicasInfo->primaryOfView(v));

  if (v <= myLatestPendingView) {
    delete m;

    return false;
  }

  delete newViewMessages[sId];
  newViewMessages[sId] = m;

  return true;
}

bool ViewsManager::add(ViewChangeMsg* m) {
  const ViewNum v = m->newView();
  const uint16_t id = m->idOfGeneratedReplica();

  ConcordAssert(id != myId);

  if (v <= myLatestPendingView) {
    delete m;

    return false;
  }

  uint16_t relPrimary = replicasInfo->primaryOfView(v);
  NewViewMsg* relatedNewView = newViewMessages[relPrimary];
  if (relatedNewView != nullptr) {
    bool ignoreMsg = false;

    if (relatedNewView->newView() > v) {
      ignoreMsg = true;
    } else if (relatedNewView->newView() == v) {
      ViewChangeMsg* prevVC = viewChangeMessages[id];
      if (prevVC != nullptr && prevVC->newView() == v) {
        Digest prevDigest;
        prevVC->getMsgDigest(prevDigest);

        // don't override a message that already has a match NewViewMsg (needed
        // to make sure that the primary will always be able to send the VM msgs
        // required for the view change; and these messages will not be
        // overridden by malicious replicas)
        if (relatedNewView->includesViewChangeFromReplica(id, prevDigest)) ignoreMsg = true;
      }
    }

    if (ignoreMsg) {
      delete m;

      return false;
    }
  }

  delete viewChangeMessages[id];  // delete previous
  viewChangeMessages[id] = m;

  return true;
}

// used to sort view numbers
static int compareViews(const void* a, const void* b) {
  ViewNum x = *reinterpret_cast<const ViewNum*>(a);
  ViewNum y = *reinterpret_cast<const ViewNum*>(b);

  if (x > y)
    return (-1);
  else if (x < y)
    return 1;
  else
    return 0;
}

// TODO(GG): optimize this method.
void ViewsManager::computeCorrectRelevantViewNumbers(ViewNum* outMaxKnownCorrectView,
                                                     ViewNum* outMaxKnownAgreedView) const {
  const uint16_t CORRECT = (F + 1);
  const uint16_t SMAJOR = (2 * F + 2 * C + 1);

  *outMaxKnownCorrectView = myLatestPendingView;
  *outMaxKnownAgreedView = myLatestPendingView;

  size_t numOfVC = 0;
  std::vector<ViewNum> viewNumbers(N);
  for (uint16_t i = 0; i < N; i++) {
    ViewChangeMsg* vc = viewChangeMessages[i];
    if (i == myId) vc = getMyLatestViewChangeMsg();

    if ((vc != nullptr) && (vc->newView() > myLatestPendingView)) {
      viewNumbers[numOfVC] = vc->newView();
      numOfVC++;
    }
  }

  if (numOfVC < CORRECT) return;

  qsort(viewNumbers.data(), numOfVC, sizeof(ViewNum), compareViews);

  ConcordAssert(viewNumbers[0] >= viewNumbers[numOfVC - 1]);

  *outMaxKnownCorrectView = viewNumbers[CORRECT - 1];

  if (numOfVC < SMAJOR) return;

  *outMaxKnownAgreedView = viewNumbers[SMAJOR - 1];
}

bool ViewsManager::hasNewViewMessage(ViewNum v) {
  ConcordAssert(v >= myLatestPendingView);

  if (v == myLatestPendingView) return true;

  const uint16_t relPrimary = replicasInfo->primaryOfView(v);

  NewViewMsg* nv = newViewMessages[relPrimary];

  return (nv != nullptr && nv->newView() == v);
}

bool ViewsManager::hasViewChangeMessageForFutureView(uint16_t repId) {
  ConcordAssert(repId < N);

  if (stat != Stat::NO_VIEW) return true;

  ViewChangeMsg* vc = viewChangeMessages[repId];

  // If we have moved to higher view (reflected in myCurrentView) we need the
  // View Change Messages from peer Replicas for it.
  return ((vc != nullptr) && (vc->newView() >= myCurrentView));
}

NewViewMsg* ViewsManager::getMyNewViewMsgForCurrentView() {
  ConcordAssert(stat == Stat::IN_VIEW);
  ConcordAssert(myId == replicasInfo->primaryOfView(myLatestActiveView));

  NewViewMsg* r = newViewMsgOfOfPendingView;

  ConcordAssert(r != nullptr);
  ConcordAssert(r->senderId() == myId);
  ConcordAssert(r->newView() == myLatestActiveView);

  return r;
}

vector<ViewChangeMsg*> ViewsManager::getViewChangeMsgsForCurrentView() {
  ConcordAssert(stat == Stat::IN_VIEW);

  const uint16_t SMAJOR = (2 * F + 2 * C + 1);

  vector<ViewChangeMsg*> retVal(SMAJOR);

  uint16_t j = 0;
  for (uint16_t i = 0; (i < N) && (j < SMAJOR); i++) {
    if (viewChangeMsgsOfPendingView[i] == nullptr) continue;

    ConcordAssert(viewChangeMsgsOfPendingView[i]->newView() == myLatestActiveView);

    retVal[j] = viewChangeMsgsOfPendingView[i];
    j++;
  }
  ConcordAssert(j == SMAJOR);

  return retVal;
}

vector<ViewChangeMsg*> ViewsManager::getViewChangeMsgsForView(ViewNum v) {
  vector<ViewChangeMsg*> relevantVCMsgs;
  if (viewIsActive(v)) {
    relevantVCMsgs = getViewChangeMsgsForCurrentView();
  } else {
    for (uint16_t i = 0; i < N; i++) {
      ViewChangeMsg* msgToAdd = viewChangeMsgsOfPendingView[i];
      if (viewChangeMessages[i] != nullptr &&
          (msgToAdd == nullptr || viewChangeMessages[i]->newView() > msgToAdd->newView())) {
        msgToAdd = viewChangeMessages[i];
      }
      if (msgToAdd && msgToAdd->newView() >= v) {
        relevantVCMsgs.push_back(msgToAdd);
      }
    }
  }
  return relevantVCMsgs;
}

NewViewMsg* ViewsManager::getNewViewMsgForCurrentView() {
  ConcordAssert(stat == Stat::IN_VIEW);

  NewViewMsg* r = newViewMsgOfOfPendingView;

  ConcordAssert(r != nullptr);
  ConcordAssert(r->newView() == myLatestActiveView);

  return r;
}

SeqNum ViewsManager::stableLowerBoundWhenEnteredToView() const {
  ConcordAssert(stat == Stat::IN_VIEW);
  return lowerBoundStableForPendingView;
}

ViewChangeMsg* ViewsManager::exitFromCurrentView(SeqNum currentLastStable,
                                                 SeqNum currentLastExecuted,
                                                 const std::vector<PrevViewInfo>& prevViewInfo) {
  ConcordAssert(stat == Stat::IN_VIEW);
  ConcordAssert(myLatestActiveView == myLatestPendingView);
  ConcordAssert(prevViewInfo.size() <= kWorkWindowSize);
  ConcordAssert(collectionOfPrePrepareMsgs.empty());

  ConcordAssert((currentLastStable >= debugHighestKnownStable));
  debugHighestKnownStable = currentLastStable;

  stat = Stat::NO_VIEW;

  // get my previous ViewChangeMsg message
  ViewChangeMsg* myPreviousVC = viewChangeMsgsOfPendingView[myId];
  if (myPreviousVC == nullptr) myPreviousVC = viewChangeMessages[myId];

  ConcordAssert(myLatestActiveView == 0 || myPreviousVC != nullptr);
  ConcordAssert(myLatestActiveView == 0 || myPreviousVC->newView() == myLatestActiveView);

  ViewChangeMsg* myNewVC = new ViewChangeMsg(myId, myLatestActiveView + 1, currentLastStable);

  ViewChangeMsg::ElementsIterator iterPrevVC(myPreviousVC);

  SeqNum debugExpected = currentLastStable;
  for (auto& it : prevViewInfo) {
    PrePrepareMsg* pp = it.prePrepare;

    ConcordAssert(pp != nullptr);
    ConcordAssert(pp->viewNumber() == myLatestActiveView);

    const SeqNum s = pp->seqNumber();
    ConcordAssert(s > lowerBoundStableForPendingView);

    ConcordAssert(s > debugExpected);  // ensures that the elements are sorted
    debugExpected = s;

    const PrepareFullMsg* pf = it.prepareFull;
    const bool allRequests = it.hasAllRequests;
    // assert ((pf != nullptr) ==> allRequests)
    ConcordAssert(pf == nullptr || allRequests);

    const Digest& digest = pp->digestOfRequests();

    ViewChangeMsg::PreparedCertificate* preparedCertInPrev = nullptr;

    if (allRequests && (pf == nullptr) && !iterPrevVC.end()) {
      ViewChangeMsg::Element* elemInPrev = nullptr;
      iterPrevVC.goToAtLeast(s);
      if (iterPrevVC.getCurrent(elemInPrev) && (elemInPrev->hasPreparedCertificate) && (elemInPrev->seqNum == s) &&
          (elemInPrev->prePrepareDigest == digest)) {
        ConcordAssert(elemInPrev->originView <= myLatestActiveView);

        // convert to PreparedCertificate
        char* x = reinterpret_cast<char*>(elemInPrev);
        x = x + sizeof(ViewChangeMsg::Element);
        preparedCertInPrev = (ViewChangeMsg::PreparedCertificate*)x;

        ConcordAssert(preparedCertInPrev->certificateView <= elemInPrev->originView);
      }
    }

    if (pf != nullptr) {
      // if we have prepare certificate from the recent view
      ConcordAssert(allRequests);
      ConcordAssert(s == pf->seqNumber());
      ConcordAssert(pf->viewNumber() == myLatestActiveView);

      myNewVC->addElement(
          s, digest, myLatestActiveView, true, myLatestActiveView, pf->signatureLen(), pf->signatureBody());
    } else if ((preparedCertInPrev != nullptr) && allRequests) {
      // if we have a prepared certificate from previous VC + we didn't find a
      // conflicted pre-prepare-digest

      // If !allRequests, then it means that the PrePrepareDigest of this
      // certificate was not safe at beginning of the last k>=1 views ==> it was
      // not executed before the beginning of the recent view ==> we don't have
      // to use the prepared certificate

      char* sig = reinterpret_cast<char*>(preparedCertInPrev) + sizeof(ViewChangeMsg::PreparedCertificate);

      myNewVC->addElement(s,
                          digest,
                          myLatestActiveView,
                          true,
                          preparedCertInPrev->certificateView,
                          preparedCertInPrev->certificateSigLength,
                          sig);
    } else if (allRequests && !pp->isNull()) {
      // we don't add null-operation to a VC message, because our default for
      // non-safe operations is null-operation (notice that, in the above
      // lines, we still may add prepared certificate for null-op)
      myNewVC->addElement(s, digest, myLatestActiveView, false, 0, 0, nullptr);
    } else {
      ConcordAssert((s > currentLastExecuted) || (pp->isNull()));
    }

    delete pf;  // we can't use this prepared certificate in the new view

    if ((allRequests) && (!pp->isNull()))
      collectionOfPrePrepareMsgs[s] = pp;  // we may need pp for the next views
    else
      delete pp;
  }

  ConcordAssert((debugExpected - currentLastStable) <= kWorkWindowSize);

  resetDataOfLatestPendingAndKeepMyViewChange();

  // delete my previous VC
  delete viewChangeMessages[myId];

  // store my new VC
  viewChangeMessages[myId] = myNewVC;

  return myNewVC;
}

bool ViewsManager::tryToEnterView(ViewNum v,
                                  SeqNum currentLastStable,
                                  SeqNum currentLastExecuted,
                                  std::vector<PrePrepareMsg*>* outPrePrepareMsgsOfView) {
  ConcordAssert(stat != Stat::IN_VIEW);
  ConcordAssert(v > myLatestActiveView);
  ConcordAssert(v >= myLatestPendingView);
  ConcordAssert(outPrePrepareMsgsOfView->empty());

  // debug lines
  ConcordAssert((v >= debugHighestViewNumberPassedByClient) && (currentLastStable >= debugHighestKnownStable));
  debugHighestViewNumberPassedByClient = v;
  debugHighestKnownStable = currentLastStable;

  if (currentLastExecuted < currentLastStable) {
    // we don't have state, let's wait for state synchronization...
    LOG_INFO(VC_LOG, "Waiting for state synchronization before entering view=" << v << " ...");
    return false;
  }

  if (currentLastStable < lowerBoundStableForPendingView) {
    // we don't have the latest stable point, let's wait for more information
    LOG_INFO(VC_LOG, "Waiting for latest stable point before entering view=" << v << " ...");
    return false;
  }

  // if we need a new pending view
  if (v > myLatestPendingView) {
    stat = Stat::NO_VIEW;

    bool newPendingView = false;

    if (replicasInfo->primaryOfView(v) == myId)
      newPendingView = tryMoveToPendingViewAsPrimary(v);
    else
      newPendingView = tryMoveToPendingViewAsNonPrimary(v);

    if (!newPendingView) return false;

    SeqNum t1 = viewChangeSafetyLogic->calcLBStableForView(viewChangeMsgsOfPendingView);
    SeqNum t2 = std::max(currentLastStable, lowerBoundStableForPendingView);
    lowerBoundStableForPendingView = std::max(t1, t2);

    stat = Stat::PENDING;

    if (currentLastStable < lowerBoundStableForPendingView) {
      // we don't have the latest stable point, let's wait for more information
      LOG_INFO(VC_LOG,
               "New pending view. The previous pending view was "
                   << myLatestPendingView << ". Waiting for latest stable point before entering view=" << v << " ...");
      return false;
    }
  }

  ConcordAssert(v == myLatestPendingView);

  if (stat == Stat::PENDING) {
    computeRestrictionsOfNewView(v);

    stat = Stat::PENDING_WITH_RESTRICTIONS;

    // BEGIN DEBUG CODE
    LOG_INFO(VC_LOG,
             "Restrictions for pending view=" << this->myLatestPendingView << ", minSeq=" << minRestrictionOfPendingView
                                              << ", maxSeq=" << maxRestrictionOfPendingView << ".");

    if (minRestrictionOfPendingView == 0) {
      LOG_INFO(VC_LOG, "No Restrictions of pending view\n");
    } else {
      size_t pp_count = 0;
      size_t ropv_count = 0;
      for (SeqNum i = minRestrictionOfPendingView; i <= maxRestrictionOfPendingView; i++) {
        uint64_t idx = i - minRestrictionOfPendingView;
        bool bHasPP = (prePrepareMsgsOfRestrictions[idx] != nullptr);
        if (bHasPP) ++pp_count;
        if (!restrictionsOfPendingView[idx].isNull) ++ropv_count;
        LOG_DEBUG(
            VC_LOG,
            "Seqnum=" << i << ", isNull=" << static_cast<int>(restrictionsOfPendingView[idx].isNull)
                      << ", digestPrefix=" << *reinterpret_cast<int*>(restrictionsOfPendingView[idx].digest.content())
                      << (bHasPP ? " ." : ", PP=null ."));
        if (bHasPP) {
          LOG_DEBUG(
              VC_LOG,
              "PP seq=" << prePrepareMsgsOfRestrictions[idx]->seqNumber() << ", digestPrefix="
                        << *reinterpret_cast<int*>(prePrepareMsgsOfRestrictions[idx]->digestOfRequests().content())
                        << " .");
        }
      }
      LOG_INFO(VC_LOG,
               "Sequence Numbers with restrictions counter: "
                   << ropv_count << " Sequence Numbers with Preprepares counter: " << pp_count);
    }

    // END DEBUG CODE
  }

  // return if we don't have restrictions
  if (stat != Stat::PENDING_WITH_RESTRICTIONS) {
    LOG_INFO(VC_LOG, "Waiting for restrictions before entering view=" << v << " ...");
    return false;
  }
  // return if some messages are missing
  if (hasMissingMsgs(currentLastStable)) {
    LOG_INFO(VC_LOG, "Waiting for missing messages before entering view=" << v << " ...");
    return false;
  }
  ///////////////////////////////////////////////////////////////////////////
  // enter to view v
  ///////////////////////////////////////////////////////////////////////////

  LOG_INFO(VC_LOG, "View is activated " << v << "previous view was " << myLatestActiveView);
  myLatestActiveView = v;
  stat = Stat::IN_VIEW;

  ///////////////////////////////////////////////////////////////////////////
  // fill outPrePrepareMsgsOfView
  // also clear prePrepareMsgsOfRestrictions and collectionOfPrePrepareMsgs
  ///////////////////////////////////////////////////////////////////////////
  if (minRestrictionOfPendingView != 0) {
    const SeqNum firstRelevant =
        std::min(maxRestrictionOfPendingView + 1, std::max(minRestrictionOfPendingView, currentLastStable + 1));

    for (SeqNum i = minRestrictionOfPendingView; i <= (firstRelevant - 1); i++) {
      const int64_t idx = i - minRestrictionOfPendingView;

      if (restrictionsOfPendingView[idx].isNull) continue;

      PrePrepareMsg* pp = prePrepareMsgsOfRestrictions[idx];
      if (pp == nullptr) continue;
      ConcordAssert(pp->seqNumber() == i);

      prePrepareMsgsOfRestrictions[idx] = nullptr;

      auto pos = collectionOfPrePrepareMsgs.find(i);
      if ((pos == collectionOfPrePrepareMsgs.end()) || (pos->second != pp))
        // if pp is not in collectionOfPrePrepareMsgs
        delete pp;
    }

    auto nbNoopPPs = 0u;
    auto nbActualRequestPPs = 0u;

    for (SeqNum i = firstRelevant; i <= maxRestrictionOfPendingView; i++) {
      const int64_t idx = i - minRestrictionOfPendingView;
      if (restrictionsOfPendingView[idx].isNull) {
        ConcordAssert(prePrepareMsgsOfRestrictions[idx] == nullptr);
        nbNoopPPs++;
        // TODO(GG): do we want to start from the slow path in these cases?
        PrePrepareMsg* pp = new PrePrepareMsg(myId, myLatestActiveView, i, CommitPath::SLOW, 0);
        outPrePrepareMsgsOfView->push_back(pp);
      } else {
        PrePrepareMsg* pp = prePrepareMsgsOfRestrictions[idx];
        ConcordAssert(pp != nullptr && pp->seqNumber() == i);
        nbActualRequestPPs++;
        // TODO(GG): do we want to start from the slow path in these cases?
        pp->updateView(myLatestActiveView);
        outPrePrepareMsgsOfView->push_back(pp);
        prePrepareMsgsOfRestrictions[idx] = nullptr;

        // if needed, remove from collectionOfPrePrepareMsgs (because we don't
        // want to delete messages returned in outPrePrepareMsgsOfView)
        auto pos = collectionOfPrePrepareMsgs.find(i);
        if ((pos != collectionOfPrePrepareMsgs.end()) && (pos->second == pp))
          // if pp is also in collectionOfPrePrepareMsgs
          collectionOfPrePrepareMsgs.erase(pos);
      }
    }

    LOG_INFO(VC_LOG, "The new view's active window contains: " << KVLOG(nbNoopPPs, nbActualRequestPPs));
  }

  for (auto it : collectionOfPrePrepareMsgs) delete it.second;
  collectionOfPrePrepareMsgs.clear();

  return true;
}

bool ViewsManager::tryMoveToPendingViewAsPrimary(ViewNum v) {
  ConcordAssert(v > myLatestPendingView);

  const uint16_t relevantPrimary = replicasInfo->primaryOfView(v);
  ConcordAssert(relevantPrimary == myId);

  const uint16_t SMAJOR = (2 * F + 2 * C + 1);

  resetDataOfLatestPendingAndKeepMyViewChange();

  // debug: check my last VC message
  ViewChangeMsg* myLastVC = viewChangeMessages[myId];
  ConcordAssert(myLastVC != nullptr && myLastVC->newView() == v);

  // remove my old NV message
  NewViewMsg* prevNewView = newViewMessages[myId];
  ConcordAssert(prevNewView == nullptr || prevNewView->newView() < v);
  if (prevNewView != nullptr) {
    delete prevNewView;
    newViewMessages[myId] = nullptr;
  }

  std::set<uint16_t> relatedVCMsgs;  // ordered set

  relatedVCMsgs.insert(myId);  // add myself

  for (uint16_t i = 0; i < N; i++) {
    if (i == myId) continue;  // my ViewChangeMsg has already been added

    ViewChangeMsg* vc = viewChangeMessages[i];
    if (vc == nullptr || vc->newView() != v) continue;
    relatedVCMsgs.insert(i);

    if (relatedVCMsgs.size() == SMAJOR) break;
  }

  if (relatedVCMsgs.size() < SMAJOR) {
    LOG_INFO(VC_LOG, "Waiting for sufficient ViewChange messages to entering view=" << v << " as primary ...");
    return false;
  }

  ConcordAssert(relatedVCMsgs.size() == SMAJOR);

  // create NewViewMsg
  NewViewMsg* nv = new NewViewMsg(myId, v);

  for (uint16_t i : relatedVCMsgs) {
    ViewChangeMsg* vc = viewChangeMessages[i];

    ConcordAssert(vc->newView() == v);

    Digest d;
    vc->getMsgDigest(d);

    ConcordAssert(!d.isZero());

    nv->addElement(i, d);

    ConcordAssert(viewChangeMsgsOfPendingView[i] == nullptr);

    viewChangeMsgsOfPendingView[i] = vc;
    viewChangeMessages[i] = nullptr;
  }

  ConcordAssert(newViewMsgOfOfPendingView == nullptr);

  newViewMsgOfOfPendingView = nv;

  myLatestPendingView = v;

  return true;
}

bool ViewsManager::tryMoveToPendingViewAsNonPrimary(ViewNum v) {
  ConcordAssert(v > myLatestPendingView);

  const uint16_t relevantPrimary = replicasInfo->primaryOfView(v);
  ConcordAssert(relevantPrimary != myId);

  const uint16_t MAJOR = (2 * F + 2 * C + 1);

  resetDataOfLatestPendingAndKeepMyViewChange();

  NewViewMsg* nv = newViewMessages[relevantPrimary];

  if (nv == nullptr || nv->newView() != v) return false;

  std::set<uint16_t> relatedVCMsgs;
  for (uint16_t i = 0; i < N; i++) {
    ViewChangeMsg* vc = viewChangeMessages[i];
    if (vc == nullptr || vc->newView() != v) continue;

    Digest d;
    vc->getMsgDigest(d);

    if (nv->includesViewChangeFromReplica(vc->idOfGeneratedReplica(), d)) {
      relatedVCMsgs.insert(i);
    }
  }

  if (relatedVCMsgs.size() < MAJOR) {
    LOG_INFO(VC_LOG, "Waiting for sufficient ViewChange messages to entering view=" << v << " ...");
    return false;
  }
  ConcordAssert(relatedVCMsgs.size() == MAJOR);

  ConcordAssert(newViewMsgOfOfPendingView == nullptr);

  newViewMsgOfOfPendingView = nv;

  newViewMessages[relevantPrimary] = nullptr;

  for (uint16_t i : relatedVCMsgs) {
    ConcordAssert(viewChangeMsgsOfPendingView[i] == nullptr);
    viewChangeMsgsOfPendingView[i] = viewChangeMessages[i];
    viewChangeMessages[i] = nullptr;
  }

  myLatestPendingView = v;

  return true;
}

void ViewsManager::computeRestrictionsOfNewView(ViewNum v) {
  ConcordAssert(stat == Stat::PENDING);
  ConcordAssert(myLatestPendingView == v);

  ConcordAssert(newViewMsgOfOfPendingView != nullptr);
  ConcordAssert(newViewMsgOfOfPendingView->newView() == v);

  viewChangeSafetyLogic->computeRestrictions(
      // Inputs
      viewChangeMsgsOfPendingView,
      lowerBoundStableForPendingView,
      // Outputs
      minRestrictionOfPendingView,
      maxRestrictionOfPendingView,
      restrictionsOfPendingView);

  // add items to prePrepareMsgsOfRestrictions
  // if we have restrictions
  if (minRestrictionOfPendingView != 0) {
    for (SeqNum i = minRestrictionOfPendingView; i <= maxRestrictionOfPendingView; i++) {
      const int64_t idx = (i - minRestrictionOfPendingView);

      ConcordAssert(idx < kWorkWindowSize);

      ConcordAssert(prePrepareMsgsOfRestrictions[idx] == nullptr);

      if (restrictionsOfPendingView[idx].isNull) continue;

      auto it = collectionOfPrePrepareMsgs.find(i);

      if (it != collectionOfPrePrepareMsgs.end() &&
          it->second->digestOfRequests() == restrictionsOfPendingView[idx].digest)
        prePrepareMsgsOfRestrictions[idx] = it->second;
    }
  }
}

// TODO(GG): consider to optimize
bool ViewsManager::hasMissingMsgs(SeqNum currentLastStable) {
  ConcordAssert(stat == Stat::PENDING_WITH_RESTRICTIONS);

  if (minRestrictionOfPendingView == 0) return false;

  const SeqNum firstRelevant = std::max(minRestrictionOfPendingView, currentLastStable + 1);

  for (SeqNum i = firstRelevant; i <= maxRestrictionOfPendingView; i++) {
    int64_t idx = i - minRestrictionOfPendingView;

    if ((!restrictionsOfPendingView[idx].isNull) && (prePrepareMsgsOfRestrictions[idx] == nullptr)) return true;
  }

  return false;
}

// TODO(GG): consider to optimize
bool ViewsManager::getNumbersOfMissingPP(SeqNum currentLastStable, std::vector<SeqNum>* outMissingPPNumbers) {
  ConcordAssert(outMissingPPNumbers->size() == 0);

  if (stat != Stat::PENDING_WITH_RESTRICTIONS) return false;

  if (minRestrictionOfPendingView == 0) return false;

  const SeqNum firstRelevant = std::max(minRestrictionOfPendingView, currentLastStable + 1);

  for (SeqNum i = firstRelevant; i <= maxRestrictionOfPendingView; i++) {
    int64_t idx = i - minRestrictionOfPendingView;

    if ((!restrictionsOfPendingView[idx].isNull) && (prePrepareMsgsOfRestrictions[idx] == nullptr))
      outMissingPPNumbers->push_back(i);
  }

  return (outMissingPPNumbers->size() > 0);
}

void ViewsManager::resetDataOfLatestPendingAndKeepMyViewChange() {
  if (newViewMsgOfOfPendingView == nullptr) return;  // no data to reset

  // we don't want to delete my VC message
  if (viewChangeMsgsOfPendingView[myId] != nullptr) {
    // move my ViewChangeMsg to viewChangeMessages[myId]
    viewChangeMessages[myId] = viewChangeMsgsOfPendingView[myId];
    viewChangeMsgsOfPendingView[myId] = nullptr;
  }

  // delete messages in prePrepareMsgsOfRestrictions
  if (minRestrictionOfPendingView != 0) {
    for (SeqNum i = minRestrictionOfPendingView; i <= maxRestrictionOfPendingView; i++) {
      int64_t idx = i - minRestrictionOfPendingView;
      ConcordAssert(idx < kWorkWindowSize);
      auto pos = collectionOfPrePrepareMsgs.find(i);
      if (pos == collectionOfPrePrepareMsgs.end() || pos->second != prePrepareMsgsOfRestrictions[idx])
        delete prePrepareMsgsOfRestrictions[idx];
      prePrepareMsgsOfRestrictions[idx] = nullptr;
    }

    minRestrictionOfPendingView = 0;
    maxRestrictionOfPendingView = 0;
  }

  for (uint16_t i = 0; i < N; i++) {
    delete viewChangeMsgsOfPendingView[i];
    viewChangeMsgsOfPendingView[i] = nullptr;
  }

  delete newViewMsgOfOfPendingView;
  newViewMsgOfOfPendingView = nullptr;
}

bool ViewsManager::addPotentiallyMissingPP(PrePrepareMsg* p, SeqNum currentLastStable) {
  ConcordAssert(stat == Stat::PENDING_WITH_RESTRICTIONS);
  ConcordAssert(!p->isNull());

  const SeqNum s = p->seqNumber();

  bool hasRelevantRestriction =
      (minRestrictionOfPendingView != 0) && (s >= minRestrictionOfPendingView) && (s <= maxRestrictionOfPendingView);

  if (hasRelevantRestriction) {
    const int64_t idx = s - minRestrictionOfPendingView;
    ConcordAssert(idx < kWorkWindowSize);

    ViewChangeSafetyLogic::Restriction& r = restrictionsOfPendingView[idx];

    // if we need this message
    if (prePrepareMsgsOfRestrictions[idx] == nullptr && !r.isNull && r.digest == p->digestOfRequests()) {
      prePrepareMsgsOfRestrictions[idx] = p;

      return true;
    }
  }

  delete p;  // p is not needed

  return false;
}

PrePrepareMsg* ViewsManager::getPrePrepare(SeqNum s) {
  ConcordAssert(stat != Stat::IN_VIEW);

  if (stat == Stat::PENDING_WITH_RESTRICTIONS) {
    bool hasRelevantRestriction =
        (minRestrictionOfPendingView != 0) && (s >= minRestrictionOfPendingView) && (s <= maxRestrictionOfPendingView);

    if (!hasRelevantRestriction) return nullptr;

    const int64_t idx = s - minRestrictionOfPendingView;
    ConcordAssert(idx < kWorkWindowSize);

    ViewChangeSafetyLogic::Restriction& r = restrictionsOfPendingView[idx];

    if (r.isNull) return nullptr;

    PrePrepareMsg* p = prePrepareMsgsOfRestrictions[idx];

    ConcordAssert(p == nullptr || ((p->seqNumber() == s) && (!p->isNull())));

    return p;
  } else {
    auto pos = collectionOfPrePrepareMsgs.find(s);

    if (pos == collectionOfPrePrepareMsgs.end()) return nullptr;

    PrePrepareMsg* p = pos->second;

    ConcordAssert(p == nullptr || ((p->seqNumber() == s) && (!p->isNull())));

    return p;
  }
}

void ViewsManager::storeComplaint(std::unique_ptr<ReplicaAsksToLeaveViewMsg>&& complaintMessage) {
  complainedReplicas.store(std::move(complaintMessage));
}

void ViewsManager::storeComplaintForHigherView(std::unique_ptr<ReplicaAsksToLeaveViewMsg>&& complaintMessage) {
  complainedReplicasForHigherView.store(std::move(complaintMessage));
}

void ViewsManager::addComplaintsToStatusMessage(ReplicaStatusMsg& replicaStatusMessage) const {
  for (const auto& i : complainedReplicas.getAllMsgs()) {
    replicaStatusMessage.setComplaintFromReplica(i.first);
  }
}

ViewChangeMsg* ViewsManager::prepareViewChangeMsgAndSetHigherView(ViewNum nextView,
                                                                  const bool wasInPrevViewNumber,
                                                                  SeqNum lastStableSeqNum,
                                                                  SeqNum lastExecutedSeqNum,
                                                                  const std::vector<PrevViewInfo>* const prevViewInfo) {
  ConcordAssertLT(myCurrentView, nextView);

  ViewChangeMsg* pVC = nullptr;

  if (!wasInPrevViewNumber) {
    pVC = getMyLatestViewChangeMsg();
    ConcordAssertNE(pVC, nullptr);
    pVC->setNewViewNumber(nextView);
    pVC->clearAllComplaints();
  } else {
    ConcordAssertNE(prevViewInfo, nullptr);
    pVC = exitFromCurrentView(lastStableSeqNum, lastExecutedSeqNum, *prevViewInfo);
    ConcordAssertNE(pVC, nullptr);
    pVC->setNewViewNumber(nextView);
  }

  for (const auto& i : complainedReplicas.getAllMsgs()) {
    pVC->addComplaint(i.second.get());
    const auto& complaint = i.second;
    LOG_DEBUG(VC_LOG,
              "Putting complaint in VC msg: " << KVLOG(
                  getCurrentView(), nextView, complaint->idOfGeneratedReplica(), complaint->viewNumber()));
  }

  complainedReplicas.clear();
  setHigherView(nextView);

  pVC->finalizeMessage();

  return pVC;
}

void ViewsManager::processComplaintsFromViewChangeMessage(ViewChangeMsg* msg,
                                                          std::function<bool(MessageBase*)> msgValidator) {
  ViewChangeMsg::ComplaintsIterator iter(msg);
  char* complaint = nullptr;
  MsgSize size = 0;
  int numberOfProcessedComplaints = 0;

  while (!(hasQuorumToLeaveView() || hasQuorumToJumpToHigherView()) && iter.getAndGoToNext(complaint, size) &&
         numberOfProcessedComplaints <= F + 1) {
    numberOfProcessedComplaints++;

    auto baseMsg = MessageBase(msg->senderId(), (MessageBase::Header*)complaint, size, true);
    auto complaintMsg = std::make_unique<ReplicaAsksToLeaveViewMsg>(&baseMsg);
    LOG_INFO(VC_LOG,
             "Got complaint in ViewChangeMsg" << KVLOG(getCurrentView(),
                                                       msg->senderId(),
                                                       msg->newView(),
                                                       msg->idOfGeneratedReplica(),
                                                       complaintMsg->senderId(),
                                                       complaintMsg->viewNumber(),
                                                       complaintMsg->idOfGeneratedReplica()));
    if (msg->newView() == getCurrentView() + 1) {
      if (getComplaintFromReplica(complaintMsg->idOfGeneratedReplica()) != nullptr) {
        LOG_INFO(VC_LOG,
                 "Already have a valid complaint from Replica " << complaintMsg->idOfGeneratedReplica() << " for View "
                                                                << complaintMsg->viewNumber());
      } else if (complaintMsg->viewNumber() == getCurrentView() && msgValidator(complaintMsg.get())) {
        storeComplaint(std::unique_ptr<ReplicaAsksToLeaveViewMsg>(complaintMsg.release()));
      } else {
        LOG_WARN(VC_LOG, "Invalid complaint in ViewChangeMsg for current View.");
      }
    } else {
      if (complaintMsg->viewNumber() + 1 == msg->newView() && msgValidator(complaintMsg.get())) {
        storeComplaintForHigherView(std::move(complaintMsg));
      } else {
        LOG_WARN(VC_LOG, "Invalid complaint in ViewChangeMsg for a higher View.");
      }
    }
  }
}

bool ViewsManager::tryToJumpToHigherViewAndMoveComplaintsOnQuorum(const ViewChangeMsg* const msg) {
  if (complainedReplicasForHigherView.hasQuorumToLeaveView()) {
    ConcordAssert(msg->newView() > getCurrentView() + 1);
    complainedReplicas = std::move(complainedReplicasForHigherView);
    LOG_INFO(
        VC_LOG,
        "Got quorum of Replicas complaining for a higher View in VCMsg: " << KVLOG(msg->newView(), getCurrentView()));
    return true;
  }

  return false;
}
}  // namespace impl
}  // namespace bftEngine
