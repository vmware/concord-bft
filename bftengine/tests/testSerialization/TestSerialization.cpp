// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "PersistentStorageImp.hpp"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "../simpleStorage/FileStorage.hpp"
#include "../../src/bftengine/PersistentStorage.hpp"
#include "../../src/bftengine/PersistentStorageWindows.hpp"
#include "../mocks/ReservedPagesMock.hpp"
#include "EpochManager.hpp"
#include "Logger.hpp"
#include "Serializable.h"
#include "helper.hpp"
#include "SigManager.hpp"
#include <string>
#include <cassert>

using namespace std;
using namespace bftEngine;
using namespace bftEngine::impl;
using namespace concord::serialize;

bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;

void printRawBuf(const UniquePtrToChar &buf, int64_t bufSize) {
  for (int i = 0; i < bufSize; ++i) {
    char c = buf.get()[i];
    if (c >= 48 && c <= 57)
      printf("%d\n", c);
    else
      printf("%c\n", c);
  }
}

uint16_t fVal = 2;
uint16_t cVal = 1;
uint16_t clientsNum = 2;
uint16_t batchSize = 2;
uint16_t numReplicas = 3 * fVal + 2 * cVal + 1;

const uint16_t msgsNum = 2 * fVal + 2 * cVal + 1;

typedef pair<uint16_t, string> IdToKeyPair;

ReplicaConfig &config = ReplicaConfig::instance();
bftEngine::impl::PersistentStorageImp *persistentStorageImp = nullptr;
unique_ptr<MetadataStorage> metadataStorage;

SeqNum lastExecutedSeqNum = 0;
SeqNum primaryLastUsedSeqNum = 0;
SeqNum strictLowerBoundOfSeqNums = 0;
ViewNum lastViewTransferredSeqNum = 0;
SeqNum lastStableSeqNum = 0;

DescriptorOfLastExitFromView *descriptorOfLastExitFromView = nullptr;
DescriptorOfLastNewView *descriptorOfLastNewView = nullptr;
DescriptorOfLastExecution *descriptorOfLastExecution = nullptr;

SeqNumWindow seqNumWindow{1};
CheckWindow checkWindow{0};

void testInit() {
  ConcordAssert(persistentStorageImp->getStoredVersion() == persistentStorageImp->getCurrentVersion());
  ConcordAssert(persistentStorageImp->getLastExecutedSeqNum() == lastExecutedSeqNum);
  ConcordAssert(persistentStorageImp->getPrimaryLastUsedSeqNum() == primaryLastUsedSeqNum);
  ConcordAssert(persistentStorageImp->getStrictLowerBoundOfSeqNums() == strictLowerBoundOfSeqNums);
  ConcordAssert(persistentStorageImp->getLastViewThatTransferredSeqNumbersFullyExecuted() == lastViewTransferredSeqNum);
  ConcordAssert(persistentStorageImp->getLastStableSeqNum() == lastStableSeqNum);

  DescriptorOfLastExitFromView lastExitFromView = persistentStorageImp->getAndAllocateDescriptorOfLastExitFromView();
  ConcordAssert(lastExitFromView.equals(*descriptorOfLastExitFromView));
  bool descHasSet = persistentStorageImp->hasDescriptorOfLastExitFromView();
  ConcordAssert(!descHasSet);

  DescriptorOfLastNewView lastNewView = persistentStorageImp->getAndAllocateDescriptorOfLastNewView();
  ConcordAssert(lastNewView.equals(*descriptorOfLastNewView));
  descHasSet = persistentStorageImp->hasDescriptorOfLastNewView();
  ConcordAssert(!descHasSet);

  DescriptorOfLastExecution lastExecution = persistentStorageImp->getDescriptorOfLastExecution();
  ConcordAssert(lastExecution.equals(*descriptorOfLastExecution));
  descHasSet = persistentStorageImp->hasDescriptorOfLastExecution();
  ConcordAssert(!descHasSet);

  descriptorOfLastExitFromView->clean();
  lastExitFromView.clean();
  descriptorOfLastNewView->clean();
  lastNewView.clean();

  SharedPtrCheckWindow storedCheckWindow = persistentStorageImp->getCheckWindow();
  ConcordAssert(storedCheckWindow.get()->equals(checkWindow));

  SharedPtrSeqNumWindow storedSeqNumWindow = persistentStorageImp->getSeqNumWindow();
  ConcordAssert(storedSeqNumWindow.get()->equals(seqNumWindow));
}

void testCheckWindowSetUp(const SeqNum shift, bool toSet) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  const SeqNum checkpointSeqNum0 = 0;
  const SeqNum checkpointSeqNum1 = 150;
  const SeqNum checkpointSeqNum2 = 300;
  ReplicaId sender = 3;
  Digest stateDigest;
  char rvbDataContent[DIGEST_SIZE] = "rvb_data_content";
  Digest rvbDataDigest(rvbDataContent, sizeof(rvbDataContent));
  const bool stateIsStable = true;
  CheckpointMsg checkpointInitialMsg0(
      sender, checkpointSeqNum0, 0, stateDigest, stateDigest, rvbDataDigest, stateIsStable);
  checkpointInitialMsg0.sign();
  CheckpointMsg checkpointInitialMsg1(
      sender, checkpointSeqNum1, 0, stateDigest, stateDigest, rvbDataDigest, stateIsStable);
  checkpointInitialMsg1.sign();
  CheckpointMsg checkpointInitialMsg2(
      sender, checkpointSeqNum2, 0, stateDigest, stateDigest, rvbDataDigest, stateIsStable);
  checkpointInitialMsg2.sign();

  const bool completed = true;

  if (toSet) {
    persistentStorageImp->beginWriteTran();
    persistentStorageImp->setCheckpointMsgInCheckWindow(checkpointSeqNum0, &checkpointInitialMsg0);
    persistentStorageImp->setCompletedMarkInCheckWindow(checkpointSeqNum0, completed);
    persistentStorageImp->setCheckpointMsgInCheckWindow(checkpointSeqNum1, &checkpointInitialMsg1);
    persistentStorageImp->setCompletedMarkInCheckWindow(checkpointSeqNum1, completed);
    persistentStorageImp->setCheckpointMsgInCheckWindow(checkpointSeqNum2, &checkpointInitialMsg2);
    persistentStorageImp->setCompletedMarkInCheckWindow(checkpointSeqNum2, completed);
    persistentStorageImp->endWriteTran();
  }

  auto checkWindowPtr = persistentStorageImp->getCheckWindow();
  CheckData &element0 = checkWindowPtr.get()->getByRealIndex(0);
  CheckData &element1 = checkWindowPtr.get()->getByRealIndex(1);
  CheckData &element2 = checkWindowPtr.get()->getByRealIndex(2);

  if (!shift) {
    ConcordAssert(element0.getCheckpointMsg()->equals(checkpointInitialMsg0));
    ConcordAssert(completed == element0.getCompletedMark());
  } else
    ConcordAssert(element0.equals(CheckData()));

  ConcordAssert(element1.getCheckpointMsg()->equals(checkpointInitialMsg1));
  ConcordAssert(element2.getCheckpointMsg()->equals(checkpointInitialMsg2));

  ConcordAssert(completed == element1.getCompletedMark());
  ConcordAssert(completed == element2.getCompletedMark());
}

void testSeqNumWindowSetUp(const SeqNum shift, bool toSet) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  const SeqNum prePrepareMsgSeqNum = 4;
  ReplicaId sender = 2;
  ViewNum view = 6;
  CommitPath firstPath = CommitPath::FAST_WITH_THRESHOLD;
  PrePrepareMsg prePrepareNullMsg(sender, view, prePrepareMsgSeqNum, firstPath, 0);

  const SeqNum slowStartedSeqNum = 144;
  bool slowStarted = true;

  const SeqNum prePrepareFullSeqNum = 101;
  PrepareFullMsg *prePrepareFullInitialMsg = PrepareFullMsg::create(view, prePrepareFullSeqNum, sender, nullptr, 0);

  const SeqNum fullCommitProofSeqNum = 160;
  FullCommitProofMsg fullCommitProofInitialMsg(sender, view, fullCommitProofSeqNum, nullptr, 0);

  const bool forceCompleted = true;

  const SeqNum commitFullSeqNum = 240;
  CommitFullMsg *commitFullInitialMsg = CommitFullMsg::create(view, commitFullSeqNum, sender, nullptr, 0);

  if (toSet) {
    persistentStorageImp->beginWriteTran();
    persistentStorageImp->setPrePrepareMsgInSeqNumWindow(prePrepareMsgSeqNum, &prePrepareNullMsg);
    persistentStorageImp->setSlowStartedInSeqNumWindow(slowStartedSeqNum, slowStarted);
    persistentStorageImp->setFullCommitProofMsgInSeqNumWindow(fullCommitProofSeqNum, &fullCommitProofInitialMsg);
    persistentStorageImp->setForceCompletedInSeqNumWindow(commitFullSeqNum, forceCompleted);
    persistentStorageImp->setPrepareFullMsgInSeqNumWindow(prePrepareFullSeqNum, prePrepareFullInitialMsg);
    persistentStorageImp->setCommitFullMsgInSeqNumWindow(commitFullSeqNum, commitFullInitialMsg);
    persistentStorageImp->endWriteTran();
  }

  const SeqNum prePrepareMsgSeqNumShifted = prePrepareMsgSeqNum + shift;
  const SeqNum prePrepareFullSeqNumShifted = prePrepareFullSeqNum + shift;
  const SeqNum fullCommitProofSeqNumShifted = fullCommitProofSeqNum + shift;
  const SeqNum commitFullSeqNumShifted = commitFullSeqNum + shift;
  const SeqNum slowStartedSeqNumShifted = slowStartedSeqNum + shift;

  auto seqNumWindowPtr = persistentStorageImp->getSeqNumWindow();
  SeqNum shiftedSeqNum = shift;
  const SeqNumData emptySeqNumData;
  for (SeqNum i = 0; i < kWorkWindowSize; ++i) {
    ++shiftedSeqNum;
    SeqNumData &element = seqNumWindowPtr.get()->getByRealIndex(i);
    PrePrepareMsg *prePrepareMsg = element.getPrePrepareMsg();
    FullCommitProofMsg *fullCommitProofMsg = element.getFullCommitProofMsg();
    PrepareFullMsg *prepareFullMsg = element.getPrepareFullMsg();
    CommitFullMsg *commitFullMsg = element.getCommitFullMsg();

    if (!shift) {
      if (prePrepareMsgSeqNumShifted == shiftedSeqNum - 1) {
        ConcordAssert(prePrepareMsg->equals(prePrepareNullMsg));
      } else
        ConcordAssert(!prePrepareMsg);

      if (prePrepareFullSeqNumShifted == shiftedSeqNum - 1) {
        ConcordAssert(prepareFullMsg->equals(*prePrepareFullInitialMsg));
      } else
        ConcordAssert(!prepareFullMsg);

      if (commitFullSeqNumShifted == shiftedSeqNum - 1) ConcordAssert(forceCompleted == element.getForceCompleted());
      if (slowStartedSeqNumShifted == shiftedSeqNum - 1) ConcordAssert(slowStarted == element.getSlowStarted());
    } else {
      if ((fullCommitProofSeqNumShifted != shiftedSeqNum - 1) && (commitFullSeqNumShifted != shiftedSeqNum - 1))
        ConcordAssert(element.equals(emptySeqNumData));
    }
    if (fullCommitProofSeqNumShifted == shiftedSeqNum - 1) {
      ConcordAssert(fullCommitProofMsg->equals(fullCommitProofInitialMsg));
    } else
      ConcordAssert(!fullCommitProofMsg);

    if (commitFullSeqNumShifted == shiftedSeqNum - 1) {
      ConcordAssert(commitFullMsg->equals(*commitFullInitialMsg));
    } else
      ConcordAssert(!commitFullMsg);
  }

  delete prePrepareFullInitialMsg;
  delete commitFullInitialMsg;
}

void testWindows(bool init) {
  testCheckWindowSetUp(0, init);
  testSeqNumWindowSetUp(0, init);
}

void testWindowsAdvance() {
  const SeqNum moveToSeqNum = 150;
  persistentStorageImp->beginWriteTran();
  persistentStorageImp->setLastStableSeqNum(moveToSeqNum);
  persistentStorageImp->endWriteTran();

  ConcordAssert(moveToSeqNum == persistentStorageImp->getLastStableSeqNum());
  testCheckWindowSetUp(moveToSeqNum, false);
  testSeqNumWindowSetUp(moveToSeqNum, false);
}

void testSetDescriptors(bool toSet) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  SeqNum lastExecutionSeqNum = 33;
  Bitmap requests(100);
  ConsensusTickRep timeInTicks = 1638860598;
  DescriptorOfLastExecution lastExecutionDesc(lastExecutionSeqNum, requests, timeInTicks);
  ViewNum viewNum = 0;
  SeqNum lastExitExecNum = 65;
  PrevViewInfoElements elements;
  ViewsManager::PrevViewInfo element;
  ReplicaId senderId = 1;
  element.hasAllRequests = true;
  element.prePrepare = new PrePrepareMsg(senderId, viewNum, lastExitExecNum, CommitPath::OPTIMISTIC_FAST, 0);
  element.prepareFull = PrepareFullMsg::create(viewNum, lastExitExecNum, senderId, nullptr, 0);
  for (uint32_t i = 0; i < kWorkWindowSize; ++i) {
    elements.push_back(element);
  }
  SeqNum lastExitStableNum = 60;
  SeqNum lastExitStableLowerBound = 50;
  SeqNum lastStable = 48;
  auto *viewChangeMsg = new ViewChangeMsg(senderId, viewNum + 1, lastStable);
  DescriptorOfLastExitFromView lastExitFromViewDesc(
      viewNum, lastExitStableNum, lastExitExecNum, elements, viewChangeMsg, lastExitStableLowerBound, {});

  ViewChangeMsgsVector msgs;
  ViewNum newViewNum = 1;
  for (auto i = 1; i <= msgsNum; i++) msgs.push_back(new ViewChangeMsg(i, newViewNum, lastExitStableNum));
  SeqNum maxSeqNum = 200;
  auto *newViewMsg = new NewViewMsg(0x01234, newViewNum);
  SeqNum lastNewViewStableLowerBound = 51;
  auto *lastNewViewViewChangeMsg = new ViewChangeMsg(senderId, newViewNum, lastStable + 2);
  DescriptorOfLastNewView lastNewViewDesc(
      newViewNum, newViewMsg, msgs, lastNewViewViewChangeMsg, lastNewViewStableLowerBound, maxSeqNum);

  if (toSet) {
    persistentStorageImp->beginWriteTran();
    persistentStorageImp->setDescriptorOfLastExecution(lastExecutionDesc);
    persistentStorageImp->setDescriptorOfLastExitFromView(lastExitFromViewDesc);
    persistentStorageImp->setDescriptorOfLastNewView(lastNewViewDesc);
    persistentStorageImp->endWriteTran();
  }

  DescriptorOfLastExecution lastExecution = persistentStorageImp->getDescriptorOfLastExecution();
  ConcordAssert(lastExecution.equals(lastExecutionDesc));
  bool descHasSet = persistentStorageImp->hasDescriptorOfLastExecution();
  ConcordAssert(descHasSet);

  DescriptorOfLastNewView lastNewView = persistentStorageImp->getAndAllocateDescriptorOfLastNewView();
  ConcordAssert(lastNewView.equals(lastNewViewDesc));
  descHasSet = persistentStorageImp->hasDescriptorOfLastNewView();
  ConcordAssert(descHasSet);

  DescriptorOfLastExitFromView lastExitFromView = persistentStorageImp->getAndAllocateDescriptorOfLastExitFromView();
  ConcordAssert(lastExitFromView.equals(lastExitFromViewDesc));
  descHasSet = persistentStorageImp->hasDescriptorOfLastExitFromView();
  ConcordAssert(descHasSet);

  for (auto *v : msgs) {
    delete v;
  }
  delete element.prePrepare;
  delete element.prepareFull;
  lastExitFromView.clean();
  lastNewView.clean();
  delete viewChangeMsg;
  delete newViewMsg;
  delete lastNewViewViewChangeMsg;
}

void testSetSimpleParams(bool toSet) {
  SeqNum lastExecSeqNum = 32;
  SeqNum lastUsedSeqNum = 212;
  SeqNum lowBoundSeqNum = 102;
  ViewNum view = 800;

  if (toSet) {
    persistentStorageImp->beginWriteTran();
    persistentStorageImp->setLastExecutedSeqNum(lastExecSeqNum);
    persistentStorageImp->setPrimaryLastUsedSeqNum(lastUsedSeqNum);
    persistentStorageImp->setStrictLowerBoundOfSeqNums(lowBoundSeqNum);
    persistentStorageImp->setLastViewThatTransferredSeqNumbersFullyExecuted(view);
    persistentStorageImp->endWriteTran();
  }

  ConcordAssert(persistentStorageImp->getLastExecutedSeqNum() == lastExecSeqNum);
  ConcordAssert(persistentStorageImp->getPrimaryLastUsedSeqNum() == lastUsedSeqNum);
  ConcordAssert(persistentStorageImp->getStrictLowerBoundOfSeqNums() == lowBoundSeqNum);
  ConcordAssert(persistentStorageImp->getLastViewThatTransferredSeqNumbersFullyExecuted() == view);
}

void testCheckDescriptorOfLastStableCheckpoint(bool init) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  const SeqNum checkpointSeqNum0 = 0;
  const SeqNum checkpointSeqNum1 = 150;
  const SeqNum checkpointSeqNum2 = 300;
  const ReplicaId sender = 3;
  const Digest stateDigest('d');
  char rvbDataContent[DIGEST_SIZE] = "rvb_data_content";
  Digest rvbDataDigest(rvbDataContent, sizeof(rvbDataContent));
  const bool stateIsStable = true;
  CheckpointMsg checkpointInitialMsg0(
      sender, checkpointSeqNum0, 0, stateDigest, stateDigest, rvbDataDigest, stateIsStable);
  checkpointInitialMsg0.sign();
  CheckpointMsg checkpointInitialMsg1(
      sender, checkpointSeqNum1, 0, stateDigest, stateDigest, rvbDataDigest, stateIsStable);
  checkpointInitialMsg1.sign();
  CheckpointMsg checkpointInitialMsg2(
      sender, checkpointSeqNum2, 0, stateDigest, stateDigest, rvbDataDigest, stateIsStable);
  checkpointInitialMsg2.sign();
  std::vector<CheckpointMsg *> msgs;
  msgs.push_back(&checkpointInitialMsg0);
  msgs.push_back(&checkpointInitialMsg1);
  msgs.push_back(&checkpointInitialMsg2);

  if (init) {
    auto initialDesc = persistentStorageImp->getDescriptorOfLastStableCheckpoint();
    ConcordAssert(initialDesc.checkpointMsgs.size() == 0);
    ConcordAssert(initialDesc.numMsgs == 0);

    DescriptorOfLastStableCheckpoint desc{numReplicas, msgs};

    persistentStorageImp->beginWriteTran();
    persistentStorageImp->setDescriptorOfLastStableCheckpoint(desc);
    persistentStorageImp->endWriteTran();
  } else {
    auto desc = persistentStorageImp->getDescriptorOfLastStableCheckpoint();
    ConcordAssert(desc.checkpointMsgs.size() == msgs.size());
    for (size_t i = 0; i < msgs.size(); i++) {
      ConcordAssert(desc.checkpointMsgs[i]->equals(*msgs[i]));
    }
    for (auto m : desc.checkpointMsgs) {
      delete m;
    }
    desc.checkpointMsgs.clear();
  }
}

int main() {
  auto &config = createReplicaConfig();
  ReplicasInfo replicaInfo(config, false, false);
  std::unique_ptr<SigManager> sigManager(createSigManager(config.replicaId,
                                                          config.replicaPrivateKey,
                                                          concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                                          config.publicKeysOfReplicas,
                                                          replicaInfo));
  DescriptorOfLastNewView::setViewChangeMsgsNum(fVal, cVal);
  descriptorOfLastExitFromView = new DescriptorOfLastExitFromView();
  descriptorOfLastNewView = new DescriptorOfLastNewView();
  descriptorOfLastExecution = new DescriptorOfLastExecution();

  persistentStorageImp = new PersistentStorageImp(numReplicas, fVal, cVal, numReplicas + clientsNum, batchSize);
  logging::Logger logger = logging::getLogger("testSerialization.replica");
  // uncomment if needed
  // log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("serializable")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);
  const string dbFile = "testPersistency.txt";
  remove(dbFile.c_str());  // Required for the init testing.

  PersistentStorageImp persistentStorage(numReplicas, fVal, cVal, numReplicas + clientsNum, batchSize);
  metadataStorage.reset(new FileStorage(logger, dbFile));
  uint16_t numOfObjects = 0;
  ObjectDescMap objectDescArray = persistentStorage.getDefaultMetadataObjectDescriptors(numOfObjects);
  metadataStorage->initMaxSizeOfObjects(objectDescArray, numOfObjects);
  persistentStorageImp->init(move(metadataStorage));

  testInit();

  bool init = true;
  for (auto i = 1; i <= 2; ++i) {
    if (!init) {
      // Re-open existing DB file
      metadataStorage.reset();
      metadataStorage.reset(new FileStorage(logger, dbFile));
      metadataStorage->initMaxSizeOfObjects(objectDescArray, numOfObjects);
      persistentStorageImp->init(move(metadataStorage));
    }
    testSetSimpleParams(init);
    testSetDescriptors(init);
    testWindows(init);
    testCheckDescriptorOfLastStableCheckpoint(init);
    if (!init) testWindowsAdvance();
    init = false;
  }

  delete descriptorOfLastExitFromView;
  delete descriptorOfLastNewView;
  delete descriptorOfLastExecution;
  delete persistentStorageImp;

  return 0;
}
