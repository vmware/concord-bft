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

#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include "ViewChangeSafetyLogic.hpp"

namespace bftEngine {
namespace impl {

class PrePrepareMsg;
class PrepareFullMsg;
class ViewChangeMsg;
class NewViewMsg;
class ViewChangeSafetyLogic;

using std::vector;

class ViewsManager {
  friend class ViewChangeMsg;
  friend class ReplicaAsksToLeaveViewMsg;

 public:
  struct PrevViewInfo {
    PrePrepareMsg *prePrepare = nullptr;
    PrepareFullMsg *prepareFull = nullptr;
    bool hasAllRequests = true;

    PrevViewInfo() = default;

    PrevViewInfo(PrePrepareMsg *prePrep, PrepareFullMsg *prepFull, bool allRequests)
        : prePrepare(prePrep), prepareFull(prepFull), hasAllRequests(allRequests) {}

    bool equals(const PrevViewInfo &other) const;

    static uint32_t maxSize();
  };

  ViewsManager(const ReplicasInfo *const r);  // TODO(GG): move to protected
  ~ViewsManager();

  static ViewsManager *createOutsideView(const ReplicasInfo *const r,
                                         ViewNum lastActiveView,
                                         SeqNum lastStable,
                                         SeqNum lastExecuted,
                                         SeqNum stableLowerBound,
                                         ViewChangeMsg *myLastViewChange,
                                         std::vector<PrevViewInfo> &elementsOfPrevView);

  static ViewsManager *createInsideViewZero(const ReplicasInfo *const r);

  static ViewsManager *createInsideView(const ReplicasInfo *const r,
                                        ViewNum view,
                                        SeqNum stableLowerBound,
                                        NewViewMsg *newViewMsg,
                                        ViewChangeMsg *myLastViewChange,  // nullptr IFF the replica has a VC message
                                                                          // in viewChangeMsgs
                                        std::vector<ViewChangeMsg *> viewChangeMsgs);

  ViewNum getCurrentView() const { return myCurrentView; }
  void setHigherView(ViewNum higherViewNum);
  void setViewFromRecovery(ViewNum explicitViewNum) { myCurrentView = explicitViewNum; }
  ViewNum latestActiveView() const { return myLatestActiveView; }
  bool viewIsActive(ViewNum v) const { return (inView() && (myLatestActiveView == v)); }
  bool viewIsPending(ViewNum v) const {
    return ((v == myLatestPendingView) && (v > myLatestActiveView));
    // TODO(GG): try to simply use the status
  }
  bool waitingForMsgs() const { return (stat == Stat::PENDING_WITH_RESTRICTIONS); }

  // should always return non-null (unless we are at the first view)
  ViewChangeMsg *getMyLatestViewChangeMsg() const;

  bool add(NewViewMsg *m);
  bool add(ViewChangeMsg *m);

  void computeCorrectRelevantViewNumbers(ViewNum *outMaxKnownCorrectView, ViewNum *outMaxKnownAgreedView) const;

  // should only be called when v >= myLatestPendingView
  bool hasNewViewMessage(ViewNum v);

  ///////////////////////////////////////////////////////////////////////////
  // Can only be used when the current view is active
  ///////////////////////////////////////////////////////////////////////////

  // should only be called by the primary of the current active view
  NewViewMsg *getMyNewViewMsgForCurrentView();

  vector<ViewChangeMsg *> getViewChangeMsgsForCurrentView();

  vector<ViewChangeMsg *> getViewChangeMsgsForView(ViewNum v);

  NewViewMsg *getNewViewMsgForCurrentView();

  SeqNum stableLowerBoundWhenEnteredToView() const;

  ViewChangeMsg *exitFromCurrentView(SeqNum currentLastStable,
                                     SeqNum currentLastExecuted,
                                     const std::vector<PrevViewInfo> &prevViewInfo);
  // TODO(GG): prevViewInfo is defined and used in a confusing way (because it
  // contains both executed and non-executed items) - TODO: improve by using two
  // different arguments

  ///////////////////////////////////////////////////////////////////////////
  // Can be used when we don't have an active view
  ///////////////////////////////////////////////////////////////////////////

  bool tryToEnterView(ViewNum v,
                      SeqNum currentLastStable,
                      SeqNum currentLastExecuted,
                      std::vector<PrePrepareMsg *> *outPrePrepareMsgsOfView);

  bool addPotentiallyMissingPP(PrePrepareMsg *p, SeqNum currentLastStable);

  PrePrepareMsg *getPrePrepare(SeqNum s);

  // TODO(GG): we should also handle large Requests

  bool getNumbersOfMissingPP(SeqNum currentLastStable, std::vector<SeqNum> *outMissingPPNumbers);

  bool hasViewChangeMessageForFutureView(uint16_t repId);

 protected:
  bool inView() const { return (stat == Stat::IN_VIEW); }

  bool tryMoveToPendingViewAsPrimary(ViewNum v);
  bool tryMoveToPendingViewAsNonPrimary(ViewNum v);

  void computeRestrictionsOfNewView(ViewNum v);

  void resetDataOfLatestPendingAndKeepMyViewChange();

  bool hasMissingMsgs(SeqNum currentLastStable);

  ///////////////////////////////////////////////////////////////////////////
  // consts
  ///////////////////////////////////////////////////////////////////////////

  const ReplicasInfo *const replicasInfo;

  const uint16_t N;  // number of replicas
  const uint16_t F;  // f
  const uint16_t C;  // c
  const uint16_t myId;

  const ViewChangeSafetyLogic *viewChangeSafetyLogic;

  ///////////////////////////////////////////////////////////////////////////
  // Types
  ///////////////////////////////////////////////////////////////////////////

  enum class Stat { NO_VIEW, PENDING, PENDING_WITH_RESTRICTIONS, IN_VIEW };

  ///////////////////////////////////////////////////////////////////////////
  // Member variables
  ///////////////////////////////////////////////////////////////////////////

  Stat stat;
  ViewNum myCurrentView;
  // myLatestPendingView always >=  myLatestActiveView
  ViewNum myLatestActiveView;
  ViewNum myLatestPendingView;

  // for each replica it holds the latest ViewChangeMsg message
  ViewChangeMsg **viewChangeMessages;
  // for each replica it holds the latest NewViewMsg message
  NewViewMsg **newViewMessages;

  // holds PrePrepareMsg messages from last view
  // messages are added when we leave a view
  // some message are deleted when we enter a new view (we don't delete messages
  // that are passed to the new view)
  // not empty, only if inView==false
  std::map<SeqNum, PrePrepareMsg *> collectionOfPrePrepareMsgs;

  ///////////////////////////////////////////////////////////////////////////
  // If inView=false, these members refere to the current pending view
  // Otherwise, they refer to the current active view
  ///////////////////////////////////////////////////////////////////////////

  ViewChangeMsg **viewChangeMsgsOfPendingView;
  NewViewMsg *newViewMsgOfOfPendingView;  // (null for v==0)

  SeqNum minRestrictionOfPendingView;
  SeqNum maxRestrictionOfPendingView;
  ViewChangeSafetyLogic::Restriction restrictionsOfPendingView[kWorkWindowSize];
  PrePrepareMsg *prePrepareMsgsOfRestrictions[kWorkWindowSize];

  SeqNum lowerBoundStableForPendingView;  // monotone increasing

  ///////////////////////////////////////////////////////////////////////////
  // for debug
  ///////////////////////////////////////////////////////////////////////////
  SeqNum debugHighestKnownStable;
  ViewNum debugHighestViewNumberPassedByClient;
};

}  // namespace impl
}  // namespace bftEngine

// TODO(GG): types for checkpoint (?)
// TODO(GG): do not use execution path after view-change (?)
