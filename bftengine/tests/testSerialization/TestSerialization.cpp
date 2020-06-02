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

#include "ReplicaConfigSerializer.hpp"
#include "PersistentStorageImp.hpp"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "../simpleStorage/FileStorage.hpp"
#include "../../src/bftengine/PersistentStorage.hpp"
#include "../../src/bftengine/PersistentStorageWindows.hpp"
#include "Logger.hpp"
#include "Serializable.h"
#include <string>
#include <cassert>

using namespace std;
using namespace bftEngine;
using namespace bftEngine::impl;
using namespace concord::serialize;

const char replicaPrivateKey[] =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100C55B8F7979BF24B335017082BF33EE2960E3A0"
    "68DCDB45CA301721"
    "4BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B2347488B0FA3F99B05CFFB"
    "B45F80960669594B"
    "58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE50BEF374FCDCE1C8CEBCA1EF12AF492402A"
    "6F56DC9338834162"
    "F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B484F72A2BC8C89F86DB52ECC40763B6298879DE686C9A2A78604A5"
    "03609BA34B779C4F"
    "55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF88292D4C01F99EFE3CC912D09B020111028201001D05EF73BF149474B4F8AEA9D0D2EE51"
    "61126A69C6203EF8"
    "162184E586D4967833E1F9BF56C89F68AD35D54D99D8DB4B7BB06C4EFD95E840BBD30C3FD7A5E890CEF6DB99E284576EEED07B6C8CEBB63B4B"
    "80DAD2311D1A706A"
    "5AC95DE768F017213B896B9EE38D2E3C2CFCE5BDF51ABD27391761245CDB3DCB686F05EA2FF654FA91F89DA699F14ACFA7F0D8030F74DBFEC2"
    "8D55C902A27E9C03"
    "AB1CA2770EFC5BE541560D86FA376B1A688D92124496BB3E7A3B78A86EBF1B694683CDB32BC49431990A18B570C104E47AC6B0DE5616851F43"
    "09CFE7D0E20B17C1"
    "54A3D85F33C7791451FFF73BFC4CDC8C16387D184F42AD2A31FCF545C3F9A498FAAC6E94E902818100F40CF9152ED4854E1BBF67C5EA185C52"
    "EBEA0C11875563AE"
    "E95037C2E61C8D988DDF71588A0B45C23979C5FBFD2C45F9416775E0A644CAD46792296FDC68A98148F7BD3164D9A5E0D6A0C2DF0141D82D61"
    "0D56CB7C53F3C674"
    "771ED9ED77C0B5BF3C936498218176DC9933F1215BC831E0D41285611F512F68327E4FBD9E5C5902818100CF05519FD69D7C6B61324F0A2015"
    "74C647792B80E5D4"
    "D56A51CF5988927A1D54DF9AE4EA656AE25961923A0EC046F1C569BAB53A64EB0E9F5AB2ABF1C9146935BA40F75E0EB68E0BE4BC29A5A0742B"
    "59DF5A55AB028F1C"
    "CC42243D2AEE4B74344CA33E72879EF2D1CDD874A7F237202AC7EB57AEDCBD539DEFDA094476EAE613028180396C76D7CEC897D624A581D437"
    "14CA6DDD2802D6F2"
    "AAAE0B09B885974533E514D6167505C620C51EA41CA70E1D73D43AA5FA39DA81799922EB3173296109914B98B2C31AAE515434E734E28ED31E"
    "8D37DA99BA11C2E6"
    "93B6398570ABBF6778A33C0E40CC6007E23A15C9B1DE6233B6A25304B91053166D7490FCD26D1D8EAC5102818079C6E4B86020674E392CA6F6"
    "E5B244B0DEBFBF3C"
    "C36E232F7B6AE95F6538C5F5B0B57798F05CFD9DFD28D6DB8029BB6511046A9AD1F3AE3F9EC37433DFB1A74CC7E9FAEC08A79ED9D1D8187F8B"
    "8FA107B08F7DAFE3"
    "633E1DCC8DC9A0C8689EB55A41E87F9B12347B6A06DB359D89D6AFC0E4CA2A9FF6E5E46EF8BA2845F396650281802A89B2BD4A665A0F07DCAF"
    "A6D9DB7669B1D127"
    "6FC3365173A53F0E0D5F9CB9C3E08E68503C62EA73EB8E0DA42CCF6B136BF4A85B0AC424730B4F3CAD8C31D34DD75EF2A39B6BCFE3985CCECC"
    "470CF479CF0E9B9D"
    "6C7CE1C6C70D853728925326A22352DF73B502D4D3CBC2A770DE276E1C5953DF7A9614C970C94D194CAE9188";
const char publicKeyValue1[] =
    "031ef8af0a33cd7b42a0b853847d9f275e8bab88dbe753b668309883a3e962ed72156ecabe1b83dcafbcef438bb334366f4e3e83f6b2564f3e"
    "02f1f4a670ec36fa";
const char publicKeyValue2[] =
    "03102c402140a917648f5be446e65fcf0eee2a9cc3d2f8a9489b98997b152cc1010381ff14e508ef7d0f01346805e8ad1f396dfa218ea525f6"
    "95debc4bf2d43f8d";
const char publicKeyValue3[] =
    "03172b38140843bfd7fe63b55d13045effcf597bc9e003102e4e160c74a9e3fd6f11e38cd307a23afd1da250f72f4e422d9863a8c3db713814"
    "32a5cf4171e50609";
const char publicKeyValue4[] =
    "02143bb5256bc80e9e1f048ef4c42f0c5e27f16e345b58482e0a4adf77b235d41d1e2c4b0636edf13b853f21b0ec738b70d47837389832498e"
    "cbea82c878ecb5ba";

class IShareSecretKeyDummy : public IShareSecretKey {
 public:
  string toString() const override { return "IShareSecretKeyDummy"; }
};

class IShareVerificationKeyDummy : public IShareVerificationKey {
 public:
  string toString() const override { return "IShareVerificationKeyDummy"; }
};

class IThresholdSignerDummy : public IThresholdSigner,
                              public concord::serialize::SerializableFactory<IThresholdSignerDummy> {
 public:
  int requiredLengthForSignedData() const override { return 2048; }
  void signData(const char *hash, int hashLen, char *outSig, int outSigLen) override {}

  const IShareSecretKey &getShareSecretKey() const override { return shareSecretKey; }
  const IShareVerificationKey &getShareVerificationKey() const override { return shareVerifyKey; }
  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream &outStream) const override {}
  void deserializeDataMembers(std::istream &outStream) override {}
  IShareSecretKeyDummy shareSecretKey;
  IShareVerificationKeyDummy shareVerifyKey;
};

class IThresholdAccumulatorDummy : public IThresholdAccumulator {
 public:
  int add(const char *sigShareWithId, int len) override { return 0; }
  void setExpectedDigest(const unsigned char *msg, int len) override {}
  bool hasShareVerificationEnabled() const override { return true; }
  int getNumValidShares() const override { return 0; }
  void getFullSignedData(char *outThreshSig, int threshSigLen) override {}
  IThresholdAccumulator *clone() override { return nullptr; }
};

class IThresholdVerifierDummy : public IThresholdVerifier,
                                public concord::serialize::SerializableFactory<IThresholdVerifierDummy> {
 public:
  IThresholdAccumulator *newAccumulator(bool withShareVerification) const override {
    return new IThresholdAccumulatorDummy;
  }
  void release(IThresholdAccumulator *acc) override {}
  bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const override { return true; }
  int requiredLengthForSignedData() const override { return 2048; }
  const IPublicKey &getPublicKey() const override { return shareVerifyKey; }
  const IShareVerificationKey &getShareVerificationKey(ShareID signer) const override { return shareVerifyKey; }

  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream &outStream) const override {}
  void deserializeDataMembers(std::istream &outStream) override {}
  IShareVerificationKeyDummy shareVerifyKey;
};

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

const uint16_t msgsNum = 2 * fVal + 2 * cVal + 1;

typedef pair<uint16_t, string> IdToKeyPair;

ReplicaConfig config;
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
  assert(persistentStorageImp->getStoredVersion() == persistentStorageImp->getCurrentVersion());
  assert(persistentStorageImp->getLastExecutedSeqNum() == lastExecutedSeqNum);
  assert(persistentStorageImp->getPrimaryLastUsedSeqNum() == primaryLastUsedSeqNum);
  assert(persistentStorageImp->getStrictLowerBoundOfSeqNums() == strictLowerBoundOfSeqNums);
  assert(persistentStorageImp->getLastViewThatTransferredSeqNumbersFullyExecuted() == lastViewTransferredSeqNum);
  assert(persistentStorageImp->getLastStableSeqNum() == lastStableSeqNum);

  DescriptorOfLastExitFromView lastExitFromView = persistentStorageImp->getAndAllocateDescriptorOfLastExitFromView();
  assert(lastExitFromView.equals(*descriptorOfLastExitFromView));
  bool descHasSet = persistentStorageImp->hasDescriptorOfLastExitFromView();
  Assert(!descHasSet);

  DescriptorOfLastNewView lastNewView = persistentStorageImp->getAndAllocateDescriptorOfLastNewView();
  assert(lastNewView.equals(*descriptorOfLastNewView));
  descHasSet = persistentStorageImp->hasDescriptorOfLastNewView();
  assert(!descHasSet);

  DescriptorOfLastExecution lastExecution = persistentStorageImp->getDescriptorOfLastExecution();
  assert(lastExecution.equals(*descriptorOfLastExecution));
  descHasSet = persistentStorageImp->hasDescriptorOfLastExecution();
  assert(!descHasSet);

  descriptorOfLastExitFromView->clean();
  lastExitFromView.clean();
  descriptorOfLastNewView->clean();
  lastNewView.clean();

  SharedPtrCheckWindow storedCheckWindow = persistentStorageImp->getCheckWindow();
  assert(storedCheckWindow.get()->equals(checkWindow));

  SharedPtrSeqNumWindow storedSeqNumWindow = persistentStorageImp->getSeqNumWindow();
  assert(storedSeqNumWindow.get()->equals(seqNumWindow));
}

void testCheckWindowSetUp(const SeqNum shift, bool toSet) {
  const SeqNum checkpointSeqNum0 = 0;
  const SeqNum checkpointSeqNum1 = 150;
  const SeqNum checkpointSeqNum2 = 300;

  ReplicaId sender = 3;
  Digest stateDigest;
  const bool stateIsStable = true;
  CheckpointMsg checkpointInitialMsg0(sender, checkpointSeqNum0, stateDigest, stateIsStable);
  CheckpointMsg checkpointInitialMsg1(sender, checkpointSeqNum1, stateDigest, stateIsStable);
  CheckpointMsg checkpointInitialMsg2(sender, checkpointSeqNum2, stateDigest, stateIsStable);

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
    Assert(element0.getCheckpointMsg()->equals(checkpointInitialMsg0));
    Assert(completed == element0.getCompletedMark());
  } else
    Assert(element0.equals(CheckData()));

  Assert(element1.getCheckpointMsg()->equals(checkpointInitialMsg1));
  Assert(element2.getCheckpointMsg()->equals(checkpointInitialMsg2));

  Assert(completed == element1.getCompletedMark());
  Assert(completed == element2.getCompletedMark());
}

void testSeqNumWindowSetUp(const SeqNum shift, bool toSet) {
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
        Assert(prePrepareMsg->equals(prePrepareNullMsg));
      } else
        Assert(!prePrepareMsg);

      if (prePrepareFullSeqNumShifted == shiftedSeqNum - 1) {
        Assert(prepareFullMsg->equals(*prePrepareFullInitialMsg));
      } else
        Assert(!prepareFullMsg);

      if (commitFullSeqNumShifted == shiftedSeqNum - 1) Assert(forceCompleted == element.getForceCompleted());
      if (slowStartedSeqNumShifted == shiftedSeqNum - 1) Assert(slowStarted == element.getSlowStarted());
    } else {
      if ((fullCommitProofSeqNumShifted != shiftedSeqNum - 1) && (commitFullSeqNumShifted != shiftedSeqNum - 1))
        Assert(element.equals(emptySeqNumData));
    }
    if (fullCommitProofSeqNumShifted == shiftedSeqNum - 1) {
      Assert(fullCommitProofMsg->equals(fullCommitProofInitialMsg));
    } else
      Assert(!fullCommitProofMsg);

    if (commitFullSeqNumShifted == shiftedSeqNum - 1) {
      Assert(commitFullMsg->equals(*commitFullInitialMsg));
    } else
      Assert(!commitFullMsg);
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

  Assert(moveToSeqNum == persistentStorageImp->getLastStableSeqNum());
  testCheckWindowSetUp(moveToSeqNum, false);
  testSeqNumWindowSetUp(moveToSeqNum, false);
}

void testSetDescriptors(bool toSet) {
  SeqNum lastExecutionSeqNum = 33;
  Bitmap requests(100);
  DescriptorOfLastExecution lastExecutionDesc(lastExecutionSeqNum, requests);

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
      viewNum, lastExitStableNum, lastExitExecNum, elements, viewChangeMsg, lastExitStableLowerBound);

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
  assert(lastExecution.equals(lastExecutionDesc));
  bool descHasSet = persistentStorageImp->hasDescriptorOfLastExecution();
  Assert(descHasSet);

  DescriptorOfLastNewView lastNewView = persistentStorageImp->getAndAllocateDescriptorOfLastNewView();
  assert(lastNewView.equals(lastNewViewDesc));
  descHasSet = persistentStorageImp->hasDescriptorOfLastNewView();
  assert(descHasSet);

  DescriptorOfLastExitFromView lastExitFromView = persistentStorageImp->getAndAllocateDescriptorOfLastExitFromView();
  assert(lastExitFromView.equals(lastExitFromViewDesc));
  descHasSet = persistentStorageImp->hasDescriptorOfLastExitFromView();
  assert(descHasSet);

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

  assert(persistentStorageImp->getLastExecutedSeqNum() == lastExecSeqNum);
  assert(persistentStorageImp->getPrimaryLastUsedSeqNum() == lastUsedSeqNum);
  assert(persistentStorageImp->getStrictLowerBoundOfSeqNums() == lowBoundSeqNum);
  assert(persistentStorageImp->getLastViewThatTransferredSeqNumbersFullyExecuted() == view);
}

void fillReplicaConfig() {
  config.fVal = fVal;
  config.cVal = cVal;
  config.replicaId = 3;
  config.numOfClientProxies = 8;
  config.statusReportTimerMillisec = 15;
  config.concurrencyLevel = 5;
  config.viewChangeProtocolEnabled = true;
  config.viewChangeTimerMillisec = 12;
  config.autoPrimaryRotationEnabled = false;
  config.autoPrimaryRotationTimerMillisec = 42;
  config.maxExternalMessageSize = 2048;
  config.maxNumOfReservedPages = 256;
  config.maxReplyMessageSize = 1024;
  config.sizeOfReservedPage = 2048;
  config.debugStatisticsEnabled = true;

  config.replicaPrivateKey = replicaPrivateKey;
  config.publicKeysOfReplicas.insert(IdToKeyPair(0, publicKeyValue1));
  config.publicKeysOfReplicas.insert(IdToKeyPair(1, publicKeyValue2));
  config.publicKeysOfReplicas.insert(IdToKeyPair(2, publicKeyValue3));
  config.publicKeysOfReplicas.insert(IdToKeyPair(3, publicKeyValue4));

  config.thresholdSignerForExecution = nullptr;
  config.thresholdVerifierForExecution = new IThresholdVerifierDummy;
  config.thresholdSignerForSlowPathCommit = new IThresholdSignerDummy;
  config.thresholdVerifierForSlowPathCommit = new IThresholdVerifierDummy;
  config.thresholdSignerForCommit = new IThresholdSignerDummy;
  config.thresholdVerifierForCommit = new IThresholdVerifierDummy;
  config.thresholdSignerForOptimisticCommit = new IThresholdSignerDummy;
  config.thresholdVerifierForOptimisticCommit = new IThresholdVerifierDummy;
  config.singletonFromThis();
  config.singletonFromThis();
}

void testSetReplicaConfig(bool toSet) {
  if (toSet) {
    persistentStorageImp->beginWriteTran();
    persistentStorageImp->setReplicaConfig(config);
    persistentStorageImp->endWriteTran();
  }
  ReplicaConfig storedConfig = persistentStorageImp->getReplicaConfig();
  ReplicaConfigSerializer storedConfigSerializer(&storedConfig);
  ReplicaConfigSerializer replicaConfigSerializer(&config);
  Assert(storedConfigSerializer == replicaConfigSerializer);
}

int main() {
  DescriptorOfLastNewView::setViewChangeMsgsNum(fVal, cVal);
  descriptorOfLastExitFromView = new DescriptorOfLastExitFromView();
  descriptorOfLastNewView = new DescriptorOfLastNewView();
  descriptorOfLastExecution = new DescriptorOfLastExecution();

  persistentStorageImp = new PersistentStorageImp(fVal, cVal);
  logging::Logger logger = logging::getLogger("testSerialization.replica");
  // uncomment if needed
  // log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("serializable")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);
  const string dbFile = "testPersistency.txt";
  remove(dbFile.c_str());  // Required for the init testing.

  PersistentStorageImp persistentStorage(fVal, cVal);
  metadataStorage.reset(new FileStorage(logger, dbFile));
  uint16_t numOfObjects = 0;
  fillReplicaConfig();
  ObjectDescUniquePtr objectDescArray = persistentStorage.getDefaultMetadataObjectDescriptors(numOfObjects);
  metadataStorage->initMaxSizeOfObjects(objectDescArray.get(), numOfObjects);
  persistentStorageImp->init(move(metadataStorage));

  testInit();

  bool init = true;
  for (auto i = 1; i <= 2; ++i) {
    if (!init) {
      // Re-open existing DB file
      metadataStorage.reset();
      metadataStorage.reset(new FileStorage(logger, dbFile));
      metadataStorage->initMaxSizeOfObjects(objectDescArray.get(), numOfObjects);
      persistentStorageImp->init(move(metadataStorage));
    }
    testSetReplicaConfig(init);
    testSetSimpleParams(init);
    testSetDescriptors(init);
    testWindows(init);
    if (!init) testWindowsAdvance();
    init = false;
  }

  delete descriptorOfLastExitFromView;
  delete descriptorOfLastNewView;
  delete descriptorOfLastExecution;
  delete persistentStorageImp;

  return 0;
}
