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

#define VerifyAND(expr1, expr2, errorCode)    \
  {                                           \
    ConcordAssertAND(expr1, expr2);           \
    if ((expr1) != true || (expr2) != true) { \
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

ReplicaLoader::ErrorCode checkReplicaConfig(const LoadedReplicaData &ld) {
  const ReplicaConfig &c = ld.repConfig;

  Verify(c.fVal >= 1, InconsistentErr);
  Verify(c.cVal >= 0, InconsistentErr);

  uint16_t numOfReplicas = 3 * c.fVal + 2 * c.cVal + 1;

  Verify(numOfReplicas <= MaxNumberOfReplicas, InconsistentErr);

  VerifyAND(c.replicaId >= 0, c.replicaId < numOfReplicas, InconsistentErr);

  Verify(c.numOfClientProxies >= 1, InconsistentErr);  // TODO(GG): TBD - do we want maximum number of client proxies?

  Verify(c.numOfExternalClients >= 0, InconsistentErr);

  Verify(c.statusReportTimerMillisec > 0,
         InconsistentErr);  // TODO(GG): TBD - do we want maximum for statusReportTimerMillisec?

  Verify(c.concurrencyLevel >= 1, InconsistentErr);
  Verify(c.concurrencyLevel <= (checkpointWindowSize / 5), InconsistentErr);
  Verify(c.concurrencyLevel < MaxConcurrentFastPaths, InconsistentErr);
  Verify(c.concurrencyLevel <= maxLegalConcurrentAgreementsByPrimary, InconsistentErr);

  std::set<uint16_t> repIDs;
  for (auto &v : c.publicKeysOfReplicas) {
    VerifyAND(v.first >= 0, v.first < (numOfReplicas + c.numRoReplicas), InconsistentErr);
    Verify(!v.second.empty(), InconsistentErr);  // TODO(GG): make sure that the key is valid
    repIDs.insert(v.first);
  }
  Verify(repIDs.size() == numOfReplicas + c.numRoReplicas, InconsistentErr);

  Verify(!c.replicaPrivateKey.empty(), InconsistentErr);  // TODO(GG): make sure that the key is valid

  return Succ;
}

void setDynamicallyConfigurableParameters(ReplicaConfig &config) {
  config.numOfClientProxies = ReplicaConfigSingleton::GetInstance().GetNumOfClientProxies();
  config.numOfExternalClients = ReplicaConfigSingleton::GetInstance().GetNumOfExternalClients();
  config.statusReportTimerMillisec = ReplicaConfigSingleton::GetInstance().GetStatusReportTimerMillisec();
  config.concurrencyLevel = ReplicaConfigSingleton::GetInstance().GetConcurrencyLevel();
  config.viewChangeProtocolEnabled = ReplicaConfigSingleton::GetInstance().GetViewChangeProtocolEnabled();
  config.viewChangeTimerMillisec = ReplicaConfigSingleton::GetInstance().GetViewChangeTimerMillisec();
  config.autoPrimaryRotationEnabled = ReplicaConfigSingleton::GetInstance().GetAutoPrimaryRotationEnabled();
  config.autoPrimaryRotationTimerMillisec = ReplicaConfigSingleton::GetInstance().GetAutoPrimaryRotationTimerMillisec();
  config.preExecutionFeatureEnabled = ReplicaConfigSingleton::GetInstance().GetPreExecutionFeatureEnabled();
  config.preExecReqStatusCheckTimerMillisec =
      ReplicaConfigSingleton::GetInstance().GetPreExecReqStatusCheckTimerMillisec();
  config.preExecConcurrencyLevel = ReplicaConfigSingleton::GetInstance().GetPreExecConcurrencyLevel();
  config.batchingPolicy = ReplicaConfigSingleton::GetInstance().GetBatchingPolicy();
  config.maxInitialBatchSize = ReplicaConfigSingleton::GetInstance().GetMaxInitialBatchSize();
  config.batchingFactorCoefficient = ReplicaConfigSingleton::GetInstance().GetBatchingFactorCoefficient();
  config.maxExternalMessageSize = ReplicaConfigSingleton::GetInstance().GetMaxExternalMessageSize();
  config.maxReplyMessageSize = ReplicaConfigSingleton::GetInstance().GetMaxReplyMessageSize();
  config.maxNumOfReservedPages = ReplicaConfigSingleton::GetInstance().GetMaxNumOfReservedPages();
  config.sizeOfReservedPage = ReplicaConfigSingleton::GetInstance().GetSizeOfReservedPage();
  config.debugStatisticsEnabled = ReplicaConfigSingleton::GetInstance().GetDebugStatisticsEnabled();
  config.metricsDumpIntervalSeconds = ReplicaConfigSingleton::GetInstance().GetMetricsDumpInterval();
  config.keyExchangeOnStart = ReplicaConfigSingleton::GetInstance().GetKeyExchangeOnStart();
  config.keyViewFilePath = ReplicaConfigSingleton::GetInstance().GetKeyViewFilePath();
}

ReplicaLoader::ErrorCode loadConfig(shared_ptr<PersistentStorage> &p, LoadedReplicaData &ld) {
  ConcordAssert(p != nullptr);

  Verify(p->hasReplicaConfig(), NoDataErr);

  ld.repConfig = p->getReplicaConfig();
  // Allow changing some parameters dynamically using configuration file
  setDynamicallyConfigurableParameters(ld.repConfig);

  ReplicaLoader::ErrorCode stat = checkReplicaConfig(ld);

  Verify((stat == Succ), stat);

  std::set<SigManager::PublicKeyDesc> replicasSigPublicKeys;

  for (const auto &e : ld.repConfig.publicKeysOfReplicas) {
    SigManager::PublicKeyDesc keyDesc = {e.first, e.second};
    replicasSigPublicKeys.insert(keyDesc);
  }

  uint16_t numOfReplicas = (uint16_t)(3 * ld.repConfig.fVal + 2 * ld.repConfig.cVal + 1);

  ld.sigManager = new SigManager(ld.repConfig.replicaId,
                                 numOfReplicas + ld.repConfig.numOfClientProxies + ld.repConfig.numOfExternalClients,
                                 ld.repConfig.replicaPrivateKey,
                                 replicasSigPublicKeys);

  ld.repsInfo = new ReplicasInfo(ld.repConfig, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);

  Cryptosystem *cryptoSys = new Cryptosystem(ld.repConfig.thresholdSystemType_,
                                             ld.repConfig.thresholdSystemSubType_,
                                             ld.repConfig.numReplicas,
                                             ld.repConfig.numReplicas);
  cryptoSys->loadKeys(ld.repConfig.thresholdPublicKey_, ld.repConfig.thresholdVerificationKeys_);
  cryptoSys->loadPrivateKey(ld.repConfig.replicaId + 1, ld.repConfig.thresholdPrivateKey_);
  bftEngine::CryptoManager::instance(&ld.repConfig, cryptoSys);

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
    viewsManager = ViewsManager::createInsideViewZero(
        ld.repsInfo, ld.sigManager, CryptoManager::instance().thresholdVerifierForSlowPathCommit());

    ConcordAssert(viewsManager->latestActiveView() == 0);
    ConcordAssert(viewsManager->viewIsActive(0));

    ld.maxSeqNumTransferredFromPrevViews = 0;
  } else if (hasDescLastExitFromView && !hasDescOfLastNewView) {
    Verify((descriptorOfLastExitFromView.view == 0), InconsistentErr);

    viewsManager = ViewsManager::createOutsideView(ld.repsInfo,
                                                   ld.sigManager,
                                                   CryptoManager::instance().thresholdVerifierForSlowPathCommit(),
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
                                                   ld.sigManager,
                                                   CryptoManager::instance().thresholdVerifierForSlowPathCommit(),
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
                                                  ld.sigManager,
                                                  CryptoManager::instance().thresholdVerifierForSlowPathCommit(),
                                                  descriptorOfLastNewView.view,
                                                  descriptorOfLastNewView.stableLowerBoundWhenEnteredToView,
                                                  descriptorOfLastNewView.newViewMsg,
                                                  descriptorOfLastNewView.myViewChangeMsg,
                                                  descriptorOfLastNewView.viewChangeMsgs);

    ld.maxSeqNumTransferredFromPrevViews = descriptorOfLastNewView.maxSeqNumTransferredFromPrevViews;
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

  ReplicaLoader::ErrorCode stat = loadConfig(p, ld);

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
  LOG_INFO(GL, "loadReplicaData Successfully loaded!");
  return Succ;
}

void freeReplicaData(LoadedReplicaData &ld) {
  for (size_t i = 0; i < sizeof(ld.checkWinArr) / sizeof(ld.checkWinArr[0]); i++) ld.checkWinArr[i].reset();

  for (size_t i = 0; i < sizeof(ld.seqNumWinArr) / sizeof(ld.seqNumWinArr[0]); i++) ld.seqNumWinArr[i].reset();

  delete ld.viewsManager;
  delete ld.repsInfo;
  delete ld.sigManager;
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
