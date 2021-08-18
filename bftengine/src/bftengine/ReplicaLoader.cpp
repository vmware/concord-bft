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

#include "ReplicaLoader.hpp"
#include "PersistentStorage.hpp"
#include "ReplicaImp.hpp"
#include "ViewsManager.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "CryptoManager.hpp"
#include "Logger.hpp"

#define Verify(expr, errorCode) \
  {                             \
    ConcordAssert(expr);        \
    if ((expr) != true) {       \
      return errorCode;         \
    }                           \
  }

#define VerifyOR(expr1, expr2, errorCode)     \
  {                                           \
    ConcordAssertOR(expr1, expr2);            \
    if ((expr1) != true && (expr2) != true) { \
      return errorCode;                       \
    }                                         \
  }

#define Succ ReplicaLoader::ErrorCode::Success
#define InconsistentErr ReplicaLoader::ErrorCode::InconsistentData
#define NoDataErr ReplicaLoader::ErrorCode::NoDataInStorage

namespace bftEngine {
namespace impl {

// most code of ReplicaLoader is encapsulated in this file
namespace {

ReplicaLoader::ErrorCode loadConfig(LoadedReplicaData &ld) {
  ld.repsInfo = new ReplicasInfo(ld.repConfig, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
  auto &config = ld.repConfig;

  ld.sigManager = SigManager::init(config.replicaId,
                                   config.replicaPrivateKey,
                                   config.publicKeysOfReplicas,
                                   concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                   config.clientTransactionSigningEnabled ? &config.publicKeysOfClients : nullptr,
                                   concord::util::crypto::KeyFormat::PemFormat,
                                   *ld.repsInfo);

  std::unique_ptr<Cryptosystem> cryptoSys = std::make_unique<Cryptosystem>(ld.repConfig.thresholdSystemType_,
                                                                           ld.repConfig.thresholdSystemSubType_,
                                                                           ld.repConfig.numReplicas,
                                                                           ld.repConfig.numReplicas);
  cryptoSys->loadKeys(ld.repConfig.thresholdPublicKey_, ld.repConfig.thresholdVerificationKeys_);
  cryptoSys->loadPrivateKey(ld.repConfig.replicaId + 1, ld.repConfig.thresholdPrivateKey_);
  bftEngine::CryptoManager::instance(std::move(cryptoSys));

  return Succ;
}

ReplicaLoader::ErrorCode checkViewDesc(const DescriptorOfLastExitFromView *exitDesc,
                                       const DescriptorOfLastNewView *newDesc) {
  // TODO: check consistency of descriptors
  return Succ;
}

ReplicaLoader::ErrorCode loadViewInfo(shared_ptr<PersistentStorage> &p, LoadedReplicaData &ld) {
  ConcordAssert(p != nullptr);
  ConcordAssert(ld.viewsManager == nullptr);

  DescriptorOfLastExitFromView descriptorOfLastExitFromView;
  DescriptorOfLastNewView descriptorOfLastNewView;

  const bool hasDescLastExitFromView = p->hasDescriptorOfLastExitFromView();
  const bool hasDescOfLastNewView = p->hasDescriptorOfLastNewView();

  if (hasDescLastExitFromView) descriptorOfLastExitFromView = p->getAndAllocateDescriptorOfLastExitFromView();

  if (hasDescOfLastNewView) descriptorOfLastNewView = p->getAndAllocateDescriptorOfLastNewView();

  const auto replicaId = ld.repConfig.replicaId;
  const auto lastExitedView = descriptorOfLastExitFromView.view;
  const auto lastNewView = descriptorOfLastNewView.view;

  LOG_INFO(GL,
           "View change descriptors: " << KVLOG(
               replicaId, hasDescLastExitFromView, hasDescOfLastNewView, lastExitedView, lastNewView));

  ReplicaLoader::ErrorCode stat = checkViewDesc(hasDescLastExitFromView ? &descriptorOfLastExitFromView : nullptr,
                                                hasDescOfLastNewView ? &descriptorOfLastNewView : nullptr);

  Verify((stat == Succ), stat);

  ViewsManager *viewsManager = nullptr;
  if (!hasDescLastExitFromView && !hasDescOfLastNewView) {
    viewsManager = ViewsManager::createInsideViewZero(ld.repsInfo);

    ConcordAssert(viewsManager->latestActiveView() == 0);
    ConcordAssert(viewsManager->viewIsActive(0));

    ld.maxSeqNumTransferredFromPrevViews = 0;
  } else if (hasDescLastExitFromView && !hasDescOfLastNewView) {
    Verify((descriptorOfLastExitFromView.view == 0), InconsistentErr);

    viewsManager = ViewsManager::createOutsideView(ld.repsInfo,
                                                   descriptorOfLastExitFromView.view,
                                                   descriptorOfLastExitFromView.lastStable,
                                                   descriptorOfLastExitFromView.lastExecuted,
                                                   descriptorOfLastExitFromView.stableLowerBoundWhenEnteredToView,
                                                   descriptorOfLastExitFromView.myViewChangeMsg,
                                                   descriptorOfLastExitFromView.elements);

    Verify((viewsManager->latestActiveView() == 0), InconsistentErr);
    Verify((!viewsManager->viewIsActive(0)), InconsistentErr);

    ld.maxSeqNumTransferredFromPrevViews = 0;
  } else if (hasDescLastExitFromView && hasDescOfLastNewView &&
             (descriptorOfLastExitFromView.view == descriptorOfLastNewView.view)) {
    Verify((descriptorOfLastExitFromView.view >= 1), InconsistentErr);

    viewsManager = ViewsManager::createOutsideView(ld.repsInfo,
                                                   descriptorOfLastExitFromView.view,
                                                   descriptorOfLastExitFromView.lastStable,
                                                   descriptorOfLastExitFromView.lastExecuted,
                                                   descriptorOfLastExitFromView.stableLowerBoundWhenEnteredToView,
                                                   descriptorOfLastExitFromView.myViewChangeMsg,
                                                   descriptorOfLastExitFromView.elements);

    Verify((viewsManager->latestActiveView() == descriptorOfLastExitFromView.view), InconsistentErr);
    Verify((!viewsManager->viewIsActive(descriptorOfLastExitFromView.view)), InconsistentErr);

    ld.maxSeqNumTransferredFromPrevViews = descriptorOfLastNewView.maxSeqNumTransferredFromPrevViews;
  } else if (hasDescLastExitFromView && hasDescOfLastNewView &&
             (descriptorOfLastExitFromView.view < descriptorOfLastNewView.view)) {
    Verify((descriptorOfLastExitFromView.view >= 0), InconsistentErr);
    Verify((descriptorOfLastNewView.view >= 1), InconsistentErr);

    viewsManager = ViewsManager::createInsideView(ld.repsInfo,
                                                  descriptorOfLastNewView.view,
                                                  descriptorOfLastNewView.stableLowerBoundWhenEnteredToView,
                                                  descriptorOfLastNewView.newViewMsg,
                                                  descriptorOfLastNewView.myViewChangeMsg,
                                                  descriptorOfLastNewView.viewChangeMsgs);

    ld.maxSeqNumTransferredFromPrevViews = descriptorOfLastNewView.maxSeqNumTransferredFromPrevViews;
    // we have not used descriptorOfLastExitFromView, therefore
    // we need to clean up the messages we have allocated inside it.
    descriptorOfLastExitFromView.clean();
  } else {
    LOG_ERROR(GL,
              "Failed to load view (inconsistent state): " << KVLOG(
                  replicaId, hasDescLastExitFromView, hasDescOfLastNewView, lastExitedView, lastNewView));
    return InconsistentErr;
  }

  ld.viewsManager = viewsManager;
  return Succ;
}

ReplicaLoader::ErrorCode loadReplicaData(shared_ptr<PersistentStorage> p, LoadedReplicaData &ld) {
  ConcordAssert(p != nullptr);

  ReplicaLoader::ErrorCode stat = loadConfig(ld);

  Verify((stat == Succ), stat);

  stat = loadViewInfo(p, ld);

  Verify((stat == Succ), stat);

  ld.primaryLastUsedSeqNum = p->getPrimaryLastUsedSeqNum();
  ld.lastStableSeqNum = p->getLastStableSeqNum();
  ld.lastExecutedSeqNum = p->getLastExecutedSeqNum();
  ld.strictLowerBoundOfSeqNums = p->getStrictLowerBoundOfSeqNums();

  LOG_INFO(GL,
           "loadReplicaData primaryLastUsedSeqNum=" << ld.primaryLastUsedSeqNum
                                                    << ", lastStableSeqNum=" << ld.lastStableSeqNum
                                                    << ", lastExecutedSeqNum=" << ld.lastExecutedSeqNum
                                                    << ", strictLowerBoundOfSeqNums=" << ld.strictLowerBoundOfSeqNums);

  Verify((ld.primaryLastUsedSeqNum >= 0), InconsistentErr);
  Verify((ld.primaryLastUsedSeqNum <= ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);
  Verify((ld.lastStableSeqNum >= 0), InconsistentErr);
  Verify((ld.lastExecutedSeqNum >= ld.lastStableSeqNum), InconsistentErr);
  Verify((ld.lastExecutedSeqNum <= ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);
  Verify((ld.strictLowerBoundOfSeqNums >= 0), InconsistentErr);
  Verify((ld.strictLowerBoundOfSeqNums < ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);

  const ViewNum lastView = ld.viewsManager->latestActiveView();
  const bool isInView = ld.viewsManager->viewIsActive(lastView);

  ld.lastViewThatTransferredSeqNumbersFullyExecuted = p->getLastViewThatTransferredSeqNumbersFullyExecuted();

  Verify((ld.lastViewThatTransferredSeqNumbersFullyExecuted >= 0), InconsistentErr);
  Verify((ld.lastViewThatTransferredSeqNumbersFullyExecuted <= lastView), InconsistentErr);

  Verify((ld.maxSeqNumTransferredFromPrevViews >= 0), InconsistentErr);
  Verify((ld.maxSeqNumTransferredFromPrevViews <= ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);

  if (isInView) {
    SeqNum curSeqNum = ld.lastStableSeqNum;
    for (size_t i = 0; i < sizeof(ld.seqNumWinArr) / sizeof(SeqNumData); i++) {
      curSeqNum++;
      SeqNumData &e = ld.seqNumWinArr[i];
      e.setPrePrepareMsg(p->getAndAllocatePrePrepareMsgInSeqNumWindow(curSeqNum));
      if (e.isPrePrepareMsgSet()) {
        e.setSlowStarted(p->getSlowStartedInSeqNumWindow(curSeqNum));
        e.setFullCommitProofMsg(p->getAndAllocateFullCommitProofMsgInSeqNumWindow(curSeqNum));
        e.setForceCompleted(p->getForceCompletedInSeqNumWindow(curSeqNum));
        e.setPrepareFullMsg(p->getAndAllocatePrepareFullMsgInSeqNumWindow(curSeqNum));
        e.setCommitFullMsg(p->getAndAllocateCommitFullMsgInSeqNumWindow(curSeqNum));

        Verify((e.getPrePrepareMsg()->seqNumber() == curSeqNum), InconsistentErr);
        Verify((e.getPrePrepareMsg()->viewNumber() == lastView), InconsistentErr);

        // TODO(GG): consider to check digests + signatures

        if (e.isFullCommitProofMsgSet()) {
          Verify((e.getFullCommitProofMsg()->seqNumber() == curSeqNum), InconsistentErr);
          Verify((e.getFullCommitProofMsg()->viewNumber() == lastView), InconsistentErr);
        }
        if (e.isPrepareFullMsgSet()) {
          Verify((e.getPrepareFullMsg()->seqNumber() == curSeqNum), InconsistentErr);
          Verify((e.getPrepareFullMsg()->viewNumber() == lastView), InconsistentErr);
        }
        if (e.isCommitFullMsgSet()) {
          Verify((e.getCommitFullMsg()->seqNumber() == curSeqNum), InconsistentErr);
          Verify((e.getCommitFullMsg()->viewNumber() == lastView), InconsistentErr);
        }
      }
    }
  }

  SeqNum stableSeqNum = ld.lastStableSeqNum;
  for (size_t i = 0; i < sizeof(ld.checkWinArr) / sizeof(CheckData); i++) {
    CheckData &e = ld.checkWinArr[i];
    e.setCheckpointMsg(p->getAndAllocateCheckpointMsgInCheckWindow(stableSeqNum));
    e.setCompletedMark(p->getCompletedMarkInCheckWindow(stableSeqNum));

    const auto chckWinBound = (ld.lastExecutedSeqNum / checkpointWindowSize) * checkpointWindowSize;
    if (stableSeqNum > 0 && stableSeqNum == chckWinBound) {
      Verify((e.isCheckpointMsgSet()), InconsistentErr);
      Verify((e.getCheckpointMsg()->seqNumber() == stableSeqNum), InconsistentErr);
      Verify((e.getCheckpointMsg()->senderId() == ld.repConfig.replicaId), InconsistentErr);
      VerifyOR(stableSeqNum > ld.lastStableSeqNum, e.getCheckpointMsg()->isStableState(), InconsistentErr);
    } else if (stableSeqNum > 0 && e.isCheckpointMsgSet()) {  // after ST previous stable ckeckpoints may be not set
      Verify((e.getCheckpointMsg()->seqNumber() == stableSeqNum), InconsistentErr);
      Verify((e.getCheckpointMsg()->senderId() == ld.repConfig.replicaId), InconsistentErr);
      VerifyOR(stableSeqNum > ld.lastStableSeqNum, e.getCheckpointMsg()->isStableState(), InconsistentErr);
    } else {
      Verify((!e.isCheckpointMsgSet()), InconsistentErr);
    }
    stableSeqNum = stableSeqNum + checkpointWindowSize;
  }

  if (p->hasDescriptorOfLastExecution()) {
    DescriptorOfLastExecution d = p->getDescriptorOfLastExecution();
    if (d.executedSeqNum > ld.lastExecutedSeqNum) {
      Verify((d.executedSeqNum == ld.lastExecutedSeqNum + 1), InconsistentErr);
      Verify(isInView, InconsistentErr);
      Verify(d.executedSeqNum > ld.lastStableSeqNum, InconsistentErr);
      Verify(d.executedSeqNum <= ld.lastStableSeqNum + kWorkWindowSize, InconsistentErr);

      uint64_t idx = d.executedSeqNum - ld.lastStableSeqNum - 1;
      ConcordAssert(idx < kWorkWindowSize);

      const SeqNumData &e = ld.seqNumWinArr[idx];

      Verify(e.isPrePrepareMsgSet(), InconsistentErr);
      Verify(e.getPrePrepareMsg()->seqNumber() == ld.lastExecutedSeqNum + 1, InconsistentErr);
      Verify(e.getPrePrepareMsg()->viewNumber() == ld.viewsManager->latestActiveView(), InconsistentErr);
      Verify(e.getPrePrepareMsg()->numberOfRequests() > 0, InconsistentErr);

      uint32_t numOfReqs = 0;
      for (uint32_t i = 0; i < d.validRequests.numOfBits(); i++) {
        if (d.validRequests.get(i)) numOfReqs++;
      }

      Verify(numOfReqs <= e.getPrePrepareMsg()->numberOfRequests(), InconsistentErr);

      ld.isExecuting = true;
      ld.validRequestsThatAreBeingExecuted = d.validRequests;
    }
  }
  const auto &desc = p->getDescriptorOfLastStableCheckpoint();
  ld.lastStableCheckpointProof = desc.checkpointMsgs;
  LOG_INFO(GL, "loadReplicaData Successfully loaded!");
  return Succ;
}

void freeReplicaData(LoadedReplicaData &ld) {
  for (size_t i = 0; i < sizeof(ld.checkWinArr) / sizeof(ld.checkWinArr[0]); i++) ld.checkWinArr[i].reset();

  for (size_t i = 0; i < sizeof(ld.seqNumWinArr) / sizeof(ld.seqNumWinArr[0]); i++) ld.seqNumWinArr[i].reset();

  delete ld.viewsManager;
  delete ld.repsInfo;
}
}  // namespace

LoadedReplicaData ReplicaLoader::loadReplica(shared_ptr<PersistentStorage> &p, ReplicaLoader::ErrorCode &outErrCode) {
  ConcordAssert(p != nullptr);
  LoadedReplicaData ld;
  outErrCode = loadReplicaData(p, ld);

  if (outErrCode != Succ) {
    freeReplicaData(ld);
  }

  return ld;
}

}  // namespace impl
}  // namespace bftEngine
