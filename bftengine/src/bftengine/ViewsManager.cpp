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

#include "PrimitiveTypes.hpp"
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "PrePrepareMsg.hpp"
#include "ViewChangeMsg.hpp"
#include "NewViewMsg.hpp"
#include "SignedShareMsgs.hpp"

namespace bftEngine {
namespace impl {

ViewsManager::ViewsManager(
    const ReplicasInfo* const r,
    IThresholdVerifier* const preparedCertificateVerifier)
    : replicasInfo(r),
      N(r->numberOfReplicas()),
      F(r->fVal()),
      C(r->cVal()),
      myId(r->myId()) {
  Assert(preparedCertificateVerifier != nullptr);
  Assert(N == (3 * F + 2 * C + 1));

  viewChangeSafetyLogic =
      new ViewChangeSafetyLogic(N,
                                F,
                                C,
                                preparedCertificateVerifier,
                                PrePrepareMsg::digestOfNullPrePrepareMsg());

  stat = Stat::IN_VIEW;

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
    for (SeqNum i = minRestrictionOfPendingView;
         i <= maxRestrictionOfPendingView;
         i++) {
      int64_t idx = (i - minRestrictionOfPendingView);
      if (prePrepareMsgsOfRestrictions[idx] == nullptr) continue;
      if (collectionOfPrePrepareMsgs.count(i) == 0 ||
          collectionOfPrePrepareMsgs.at(i) != prePrepareMsgsOfRestrictions[idx])
        delete prePrepareMsgsOfRestrictions[idx];
    }
  }

  for (auto it : collectionOfPrePrepareMsgs) delete it.second;
}

ViewChangeMsg* ViewsManager::getMyLatestViewChangeMsg() const {
  ViewChangeMsg* vc = viewChangeMsgsOfPendingView[myId];
  if (vc == nullptr) vc = viewChangeMessages[myId];

  Assert(vc != nullptr || myLatestPendingView == 0);

  return vc;
}

bool ViewsManager::add(NewViewMsg* m) {
  const uint16_t sId = m->senderId();
  const ViewNum v = m->newView();

  // these asserts should be verified before accepting this message
  Assert(sId != myId);
  Assert(sId == replicasInfo->primaryOfView(v));

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

  Assert(id != myId);

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
        if (relatedNewView->includesViewChangeFromReplica(id, prevDigest))
          ignoreMsg = true;
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
void ViewsManager::computeCorrectRelevantViewNumbers(
    ViewNum* outMaxKnownCorrectView, ViewNum* outMaxKnownAgreedView) const {
  const uint16_t CORRECT = (F + 1);
  const uint16_t SMAJOR = (2 * F + 2 * C + 1);

  *outMaxKnownCorrectView = myLatestPendingView;
  *outMaxKnownAgreedView = myLatestPendingView;

  size_t numOfVC = 0;
  ViewNum* viewNumbers =
      reinterpret_cast<ViewNum*>(alloca(N * sizeof(ViewNum)));
  for (uint16_t i = 0; i < N; i++) {
    ViewChangeMsg* vc = viewChangeMessages[i];
    if (i == myId) vc = getMyLatestViewChangeMsg();

    if ((vc != nullptr) && (vc->newView() > myLatestPendingView)) {
      viewNumbers[numOfVC] = vc->newView();
      numOfVC++;
    }
  }

  if (numOfVC < CORRECT) return;

  qsort(viewNumbers, numOfVC, sizeof(ViewNum), compareViews);

  Assert(viewNumbers[0] >= viewNumbers[numOfVC - 1]);

  *outMaxKnownCorrectView = viewNumbers[CORRECT - 1];

  if (numOfVC < SMAJOR) return;

  *outMaxKnownAgreedView = viewNumbers[SMAJOR - 1];
}

bool ViewsManager::hasNewViewMessage(ViewNum v) {
  Assert(v >= myLatestPendingView);

  if (v == myLatestPendingView) return true;

  const uint16_t relPrimary = replicasInfo->primaryOfView(v);

  NewViewMsg* nv = newViewMessages[relPrimary];

  return (nv != nullptr && nv->newView() == v);
}

bool ViewsManager::hasViewChangeMessageForFutureView(uint16_t repId) {
  Assert(repId < N);

  if (stat != Stat::NO_VIEW) return true;

  ViewChangeMsg* vc = viewChangeMessages[repId];

  return ((vc != nullptr) && (vc->newView() > myLatestPendingView));
}

NewViewMsg* ViewsManager::getMyNewViewMsgForCurrentView() {
  Assert(stat == Stat::IN_VIEW);
  Assert(myId == replicasInfo->primaryOfView(myLatestActiveView));

  NewViewMsg* r = newViewMsgOfOfPendingView;

  Assert(r != nullptr);
  Assert(r->senderId() == myId);
  Assert(r->newView() == myLatestActiveView);

  return r;
}

SeqNum ViewsManager::stableLowerBoundWhenEnteredToView() const {
  Assert(stat == Stat::IN_VIEW);
  return lowerBoundStableForPendingView;
}

ViewChangeMsg* ViewsManager::exitFromCurrentView(
    SeqNum currentLastStable,
    SeqNum currentLastExecuted,
    const std::vector<PrevViewInfo>& prevViewInfo) {
  Assert(stat == Stat::IN_VIEW);
  Assert(myLatestActiveView == myLatestPendingView);
  Assert(prevViewInfo.size() <= kWorkWindowSize);
  Assert(collectionOfPrePrepareMsgs.empty());

  Assert((currentLastStable >= debugHighestKnownStable));
  debugHighestKnownStable = currentLastStable;

  stat = Stat::NO_VIEW;

  // get my previous ViewChangeMsg message
  ViewChangeMsg* myPreviousVC = viewChangeMsgsOfPendingView[myId];
  if (myPreviousVC == nullptr) myPreviousVC = viewChangeMessages[myId];

  Assert(myLatestActiveView == 0 || myPreviousVC != nullptr);
  Assert(myLatestActiveView == 0 ||
         myPreviousVC->newView() == myLatestActiveView);

  ViewChangeMsg* myNewVC =
      new ViewChangeMsg(myId, myLatestActiveView + 1, currentLastStable);

  ViewChangeMsg::ElementsIterator iterPrevVC(myPreviousVC);

  SeqNum debugExpected = currentLastStable;
  for (auto& it : prevViewInfo) {
    PrePrepareMsg* pp = it.prePrepare;

    Assert(pp != nullptr);
    Assert(pp->viewNumber() == myLatestActiveView);

    const SeqNum s = pp->seqNumber();
    Assert(s > lowerBoundStableForPendingView);

    Assert(s > debugExpected);  // ensures that the elements are sorted
    debugExpected = s;

    const PrepareFullMsg* pf = it.prepareFull;
    const bool allRequests = it.hasAllRequests;
    // assert ((pf != nullptr) ==> allRequests)
    Assert(pf == nullptr || allRequests);

    const Digest& digest = pp->digestOfRequests();

    ViewChangeMsg::PreparedCertificate* preparedCertInPrev = nullptr;

    if (allRequests && (pf == nullptr) && !iterPrevVC.end()) {
      ViewChangeMsg::Element* elemInPrev = nullptr;
      iterPrevVC.goToAtLeast(s);
      if (iterPrevVC.getCurrent(elemInPrev) &&
          (elemInPrev->hasPreparedCertificate) && (elemInPrev->seqNum == s) &&
          (elemInPrev->prePrepreDigest == digest)) {
        Assert(elemInPrev->originView <= myLatestActiveView);

        // convert to PreparedCertificate
        char* x = reinterpret_cast<char*>(elemInPrev);
        x = x + sizeof(ViewChangeMsg::Element);
        preparedCertInPrev = (ViewChangeMsg::PreparedCertificate*)x;

        Assert(preparedCertInPrev->certificateView <= elemInPrev->originView);
      }
    }

    if (pf != nullptr) {
      // if we have prepare certificate from the recent view
      Assert(allRequests);
      Assert(s == pf->seqNumber());
      Assert(pf->viewNumber() == myLatestActiveView);

      myNewVC->addElement(*replicasInfo,
                          s,
                          digest,
                          myLatestActiveView,
                          true,
                          myLatestActiveView,
                          pf->signatureLen(),
                          pf->signatureBody());
    } else if ((preparedCertInPrev != nullptr) && allRequests) {
      // if we have a prepared certificate from previous VC + we didn't find a
      // conflicted pre-prepare-digest

      // If !allRequests, then it means that the PrePrepareDigest of this
      // certificate was not safe at beginning of the last k>=1 views ==> it was
      // not executed before the beginning of the recent view ==> we don't have
      // to use the prepared certificate

      char* sig = reinterpret_cast<char*>(preparedCertInPrev) +
                  sizeof(ViewChangeMsg::PreparedCertificate);

      myNewVC->addElement(*replicasInfo,
                          s,
                          digest,
                          myLatestActiveView,
                          true,
                          preparedCertInPrev->certificateView,
                          preparedCertInPrev->certificateSigLength,
                          sig);
    } else if (allRequests && !pp->isNull()) {
      // we don't add null-operation to a VC message, becuase our default for
      // non-safe operations is null-operation (notice that, in the above
      // lines, we still may add prepared certificate for null-op)
      myNewVC->addElement(
          *replicasInfo, s, digest, myLatestActiveView, false, 0, 0, nullptr);
    } else {
      Assert((s > currentLastExecuted) || (pp->isNull()));
    }

    delete pf;  // we can't use this prepared certificate in the new view

    if ((allRequests) && (!pp->isNull()))
      collectionOfPrePrepareMsgs[s] = pp;  // we may need pp for the next views
    else
      delete pp;
  }

  Assert((debugExpected - currentLastStable) <= kWorkWindowSize);

  resetDataOfLatestPendingAndKeepMyViewChange();

  // delete my previous VC
  delete viewChangeMessages[myId];

  // store my new VC
  viewChangeMessages[myId] = myNewVC;

  return myNewVC;
}

bool ViewsManager::tryToEnterView(
    ViewNum v,
    SeqNum currentLastStable,
    SeqNum currentLastExecuted,
    std::vector<PrePrepareMsg*>* outPrePrepareMsgsOfView) {
  Assert(stat != Stat::IN_VIEW);
  Assert(v > myLatestActiveView);
  Assert(v >= myLatestPendingView);
  Assert(outPrePrepareMsgsOfView->empty());

  // debug lines
  Assert((v >= debugHighestViewNumberPassedByClient) &&
         (currentLastStable >= debugHighestKnownStable));
  debugHighestViewNumberPassedByClient = v;
  debugHighestKnownStable = currentLastStable;

  if (currentLastExecuted < currentLastStable)
    // we don't have state, let's wait for state synchronization...
    return false;

  if (currentLastStable < lowerBoundStableForPendingView)
    // we don't have the latest stable point, let's wait for more information
    return false;

  // if we need a new pending view
  if (v > myLatestPendingView) {
    stat = Stat::NO_VIEW;

    bool newPendingView = false;

    if (replicasInfo->primaryOfView(v) == myId)
      newPendingView = tryMoveToPendingViewAsPrimary(v);
    else
      newPendingView = tryMoveToPendingViewAsNonPrimary(v);

    if (!newPendingView) return false;

    SeqNum t1 =
        viewChangeSafetyLogic->calcLBStableForView(viewChangeMsgsOfPendingView);
    SeqNum t2 = std::max(currentLastStable, lowerBoundStableForPendingView);
    lowerBoundStableForPendingView = std::max(t1, t2);

    stat = Stat::PENDING;

    if (currentLastStable < lowerBoundStableForPendingView)
      // we don't have the latest stable point, let's wait for more information
      return false;
  }

  Assert(v == myLatestPendingView);

  if (stat == Stat::PENDING) {
    computeRestrictionsOfNewView(v);

    stat = Stat::PENDING_WITH_RESTRICTIONS;

    // BEGIN DEBUG CODE

    printf("\n\n\nRestrictions for pending view %" PRId64 ":",
           this->myLatestPendingView);

    if (minRestrictionOfPendingView == 0) {
      printf("None\n");
    } else {
      for (SeqNum i = minRestrictionOfPendingView;
           i <= maxRestrictionOfPendingView;
           i++) {
        uint64_t idx = i - minRestrictionOfPendingView;
        printf("\n");
        printf("Seqnum=%" PRId64 ", isNull=%d, digestPrefix=%d .     ",
               i,
               static_cast<int>(restrictionsOfPendingView[idx].isNull),
               *reinterpret_cast<int*>(
                   restrictionsOfPendingView[idx].digest.content()));
        if (prePrepareMsgsOfRestrictions[idx] == nullptr)
          printf("PP=null .");
        else
          printf("PP seq=%" PRId64 ", digestPrefix=%d .",
                 prePrepareMsgsOfRestrictions[idx]->seqNumber(),
                 *reinterpret_cast<int*>(prePrepareMsgsOfRestrictions[idx]
                                             ->digestOfRequests()
                                             .content()));
        printf("\n");
      }
    }

    // END DEBUG CODE
  }

  // return if we don't have restrictions, or some messages are missing
  if ((stat != Stat::PENDING_WITH_RESTRICTIONS) ||
      hasMissingMsgs(currentLastStable))
    return false;

  ///////////////////////////////////////////////////////////////////////////
  // enter to view v
  ///////////////////////////////////////////////////////////////////////////

  myLatestActiveView = v;
  stat = Stat::IN_VIEW;

  ///////////////////////////////////////////////////////////////////////////
  // fill outPrePrepareMsgsOfView
  // also clear prePrepareMsgsOfRestrictions and collectionOfPrePrepareMsgs
  ///////////////////////////////////////////////////////////////////////////
  if (minRestrictionOfPendingView != 0) {
    const SeqNum firstRelevant =
        std::min(maxRestrictionOfPendingView + 1,
                 std::max(minRestrictionOfPendingView, currentLastStable + 1));

    for (SeqNum i = minRestrictionOfPendingView; i <= (firstRelevant - 1);
         i++) {
      const int64_t idx = i - minRestrictionOfPendingView;

      if (restrictionsOfPendingView[idx].isNull) continue;

      PrePrepareMsg* pp = prePrepareMsgsOfRestrictions[idx];
      if (pp == nullptr) continue;
      Assert(pp->seqNumber() == i);

      prePrepareMsgsOfRestrictions[idx] = nullptr;

      auto pos = collectionOfPrePrepareMsgs.find(i);
      if ((pos == collectionOfPrePrepareMsgs.end()) || (pos->second != pp))
        // if pp is not in collectionOfPrePrepareMsgs
        delete pp;
    }

    for (SeqNum i = firstRelevant; i <= maxRestrictionOfPendingView; i++) {
      const int64_t idx = i - minRestrictionOfPendingView;
      if (restrictionsOfPendingView[idx].isNull) {
        Assert(prePrepareMsgsOfRestrictions[idx] == nullptr);
        // TODO(GG): do we want to start from the slow path in these cases?
        PrePrepareMsg* pp =
            PrePrepareMsg::createNullPrePrepareMsg(myId, myLatestActiveView, i);
        outPrePrepareMsgsOfView->push_back(pp);
      } else {
        PrePrepareMsg* pp = prePrepareMsgsOfRestrictions[idx];
        Assert(pp != nullptr && pp->seqNumber() == i);
        // TODO(GG): do we want to start from the slow path in these cases?
        pp->updateView(myLatestActiveView);
        outPrePrepareMsgsOfView->push_back(pp);
        prePrepareMsgsOfRestrictions[idx] = nullptr;

        // if needed, remove from collectionOfPrePrepareMsgs (becuase we don't
        // want to delete messages returned in outPrePrepareMsgsOfView)
        auto pos = collectionOfPrePrepareMsgs.find(i);
        if ((pos != collectionOfPrePrepareMsgs.end()) && (pos->second == pp))
          // if pp is also in collectionOfPrePrepareMsgs
          collectionOfPrePrepareMsgs.erase(pos);
      }
    }
  }

  for (auto it : collectionOfPrePrepareMsgs) delete it.second;
  collectionOfPrePrepareMsgs.clear();

  return true;
}

bool ViewsManager::tryMoveToPendingViewAsPrimary(ViewNum v) {
  Assert(v > myLatestPendingView);

  const uint16_t relevantPrimary = replicasInfo->primaryOfView(v);
  Assert(relevantPrimary == myId);

  const uint16_t SMAJOR = (2 * F + 2 * C + 1);

  resetDataOfLatestPendingAndKeepMyViewChange();

  // debug: check my last VC message
  ViewChangeMsg* myLastVC = viewChangeMessages[myId];
  Assert(myLastVC != nullptr && myLastVC->newView() == v);

  // remove my old NV message
  NewViewMsg* prevNewView = newViewMessages[myId];
  Assert(prevNewView == nullptr || prevNewView->newView() < v);
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

  if (relatedVCMsgs.size() < SMAJOR) return false;

  Assert(relatedVCMsgs.size() == SMAJOR);

  // create NewViewMsg
  NewViewMsg* nv = new NewViewMsg(myId, v);

  for (uint16_t i : relatedVCMsgs) {
    ViewChangeMsg* vc = viewChangeMessages[i];

    Assert(vc->newView() == v);

    Digest d;
    vc->getMsgDigest(d);

    Assert(!d.isZero());

    nv->addElement(i, d);

    Assert(viewChangeMsgsOfPendingView[i] == nullptr);

    viewChangeMsgsOfPendingView[i] = vc;
    viewChangeMessages[i] = nullptr;
  }

  Assert(newViewMsgOfOfPendingView == nullptr);

  newViewMsgOfOfPendingView = nv;

  myLatestPendingView = v;

  return true;
}

bool ViewsManager::tryMoveToPendingViewAsNonPrimary(ViewNum v) {
  Assert(v > myLatestPendingView);

  const uint16_t relevantPrimary = replicasInfo->primaryOfView(v);
  Assert(relevantPrimary != myId);

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

  if (relatedVCMsgs.size() < MAJOR) return false;

  Assert(relatedVCMsgs.size() == MAJOR);

  Assert(newViewMsgOfOfPendingView == nullptr);

  newViewMsgOfOfPendingView = nv;

  newViewMessages[relevantPrimary] = nullptr;

  for (uint16_t i : relatedVCMsgs) {
    Assert(viewChangeMsgsOfPendingView[i] == nullptr);
    viewChangeMsgsOfPendingView[i] = viewChangeMessages[i];
    viewChangeMessages[i] = nullptr;
  }

  myLatestPendingView = v;

  return true;
}

void ViewsManager::computeRestrictionsOfNewView(ViewNum v) {
  Assert(stat == Stat::PENDING);
  Assert(myLatestPendingView == v);

  Assert(newViewMsgOfOfPendingView != nullptr);
  Assert(newViewMsgOfOfPendingView->newView() == v);

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
    for (SeqNum i = minRestrictionOfPendingView;
         i <= maxRestrictionOfPendingView;
         i++) {
      const int64_t idx = (i - minRestrictionOfPendingView);

      Assert(idx < kWorkWindowSize);

      Assert(prePrepareMsgsOfRestrictions[idx] == nullptr);

      if (restrictionsOfPendingView[idx].isNull) continue;

      auto it = collectionOfPrePrepareMsgs.find(i);

      if (it != collectionOfPrePrepareMsgs.end() &&
          it->second->digestOfRequests() ==
              restrictionsOfPendingView[idx].digest)
        prePrepareMsgsOfRestrictions[idx] = it->second;
    }
  }
}

// TODO(GG): consider to optimize
bool ViewsManager::hasMissingMsgs(SeqNum currentLastStable) {
  Assert(stat == Stat::PENDING_WITH_RESTRICTIONS);

  if (minRestrictionOfPendingView == 0) return false;

  const SeqNum firstRelevant =
      std::max(minRestrictionOfPendingView, currentLastStable + 1);

  for (SeqNum i = firstRelevant; i <= maxRestrictionOfPendingView; i++) {
    int64_t idx = i - minRestrictionOfPendingView;

    if ((!restrictionsOfPendingView[idx].isNull) &&
        (prePrepareMsgsOfRestrictions[idx] == nullptr))
      return true;
  }

  return false;
}

// TODO(GG): consider to optimize
bool ViewsManager::getNumbersOfMissingPP(
    SeqNum currentLastStable, std::vector<SeqNum>* outMissingPPNumbers) {
  Assert(outMissingPPNumbers->size() == 0);

  if (stat != Stat::PENDING_WITH_RESTRICTIONS) return false;

  if (minRestrictionOfPendingView == 0) return false;

  const SeqNum firstRelevant =
      std::max(minRestrictionOfPendingView, currentLastStable + 1);

  for (SeqNum i = firstRelevant; i <= maxRestrictionOfPendingView; i++) {
    int64_t idx = i - minRestrictionOfPendingView;

    if ((!restrictionsOfPendingView[idx].isNull) &&
        (prePrepareMsgsOfRestrictions[idx] == nullptr))
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
    for (SeqNum i = minRestrictionOfPendingView;
         i <= maxRestrictionOfPendingView;
         i++) {
      int64_t idx = i - minRestrictionOfPendingView;
      Assert(idx < kWorkWindowSize);
      auto pos = collectionOfPrePrepareMsgs.find(i);
      if (pos == collectionOfPrePrepareMsgs.end() ||
          pos->second != prePrepareMsgsOfRestrictions[idx])
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

bool ViewsManager::addPotentiallyMissingPP(PrePrepareMsg* p,
                                           SeqNum currentLastStable) {
  Assert(stat == Stat::PENDING_WITH_RESTRICTIONS);
  Assert(!p->isNull());

  const SeqNum s = p->seqNumber();

  bool hasRelevantRestriction = (minRestrictionOfPendingView != 0) &&
                                (s >= minRestrictionOfPendingView) &&
                                (s <= maxRestrictionOfPendingView);

  if (hasRelevantRestriction) {
    const int64_t idx = s - minRestrictionOfPendingView;
    Assert(idx < kWorkWindowSize);

    ViewChangeSafetyLogic::Restriction& r = restrictionsOfPendingView[idx];

    // if we need this message
    if (prePrepareMsgsOfRestrictions[idx] == nullptr && !r.isNull &&
        r.digest == p->digestOfRequests()) {
      prePrepareMsgsOfRestrictions[idx] = p;

      return true;
    }
  }

  delete p;  // p is not needed

  return false;
}

PrePrepareMsg* ViewsManager::getPrePrepare(SeqNum s) {
  Assert(stat != Stat::IN_VIEW);

  if (stat == Stat::PENDING_WITH_RESTRICTIONS) {
    bool hasRelevantRestriction = (minRestrictionOfPendingView != 0) &&
                                  (s >= minRestrictionOfPendingView) &&
                                  (s <= maxRestrictionOfPendingView);

    if (!hasRelevantRestriction) return nullptr;

    const int64_t idx = s - minRestrictionOfPendingView;
    Assert(idx < kWorkWindowSize);

    ViewChangeSafetyLogic::Restriction& r = restrictionsOfPendingView[idx];

    if (r.isNull) return nullptr;

    PrePrepareMsg* p = prePrepareMsgsOfRestrictions[idx];

    Assert(p == nullptr || ((p->seqNumber() == s) && (!p->isNull())));

    return p;
  } else {
    auto pos = collectionOfPrePrepareMsgs.find(s);

    if (pos == collectionOfPrePrepareMsgs.end()) return nullptr;

    PrePrepareMsg* p = pos->second;

    Assert(p == nullptr || ((p->seqNumber() == s) && (!p->isNull())));

    return p;
  }
}

}  // namespace impl
}  // namespace bftEngine
