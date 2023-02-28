// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#define CONCORD_BFT_TESTING

#include "PreProcessor.hpp"
#include "util/OpenTracing.hpp"
#include "util/Timers.hpp"
#include "messages/PreProcessBatchRequestMsg.hpp"
#include "InternalReplicaApi.hpp"
#include "communication/CommFactory.hpp"
#include "log/logger.hpp"
#include "util/assertUtils.hpp"
#include "RequestProcessingState.hpp"
#include "ReplicaConfig.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "gtest/gtest.h"
#include "crypto/threshsign/eddsa/EdDSAMultisigFactory.h"
#include "CryptoManager.hpp"
#include "tests/messages/helper.hpp"
#include "tests/config/test_comm_config.hpp"
#include "bftengine/MsgsCommunicator.hpp"

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

using namespace std;
using namespace bft::communication;
using namespace bftEngine;
using namespace preprocessor;

namespace {

const uint16_t numOfReplicas_4 = 4;
const uint16_t numOfReplicas_7 = 7;
const uint16_t numOfInternalClients = 4;
const uint16_t fVal_4 = 1;
const uint16_t fVal_7 = 2;
const uint16_t cVal = 0;
const uint16_t numOfRequiredReplies = fVal_4 + 1;
const uint32_t bufLen = 1024;
const uint64_t reqTimeoutMilli = 10;
const uint64_t waitForExecTimerMillisec = 105;
const uint64_t preExecReqStatusCheckTimerMillisec = 10;
const uint64_t viewChangeTimerMillisec = 80;
const uint16_t reqWaitTimeoutMilli = 50;
const ReqId reqSeqNum = 123456789;
const uint16_t clientId = 12;
string cid = "abcd";
const concordUtils::SpanContext span;
const NodeIdType replica_0 = 0;
const NodeIdType replica_1 = 1;
const NodeIdType replica_2 = 2;
const NodeIdType replica_3 = 3;
const NodeIdType replica_4 = 4;
const ViewNum viewNum = 1;
PreProcessorRecorder preProcessorRecorder;
std::shared_ptr<concord::performance::PerformanceManager> sdm = make_shared<concord::performance::PerformanceManager>();

uint64_t reqRetryId = 20;

ReplicaConfig& replicaConfig = ReplicaConfig::instance();
char buf[bufLen];
std::shared_ptr<SigManager> sigManager[numOfReplicas_4];
std::shared_ptr<CryptoManager> cryptoManager[numOfReplicas_4];
std::unique_ptr<ReplicasInfo> replicasInfo[numOfReplicas_4];

class DummyRequestsHandler : public IRequestsHandler {
  void execute(ExecutionRequestsQueue& requests,
               std::optional<Timestamp> timestamp,
               const std::string& batchCid,
               concordUtils::SpanWrapper& parent_span) override {
    for (auto& req : requests) {
      req.outActualReplySize = 256;
      req.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);
    }
  }
  void preExecute(IRequestsHandler::ExecutionRequest& req,
                  std::optional<Timestamp> timestamp,
                  const std::string& batchCid,
                  concordUtils::SpanWrapper& parent_span) override {
    req.outActualReplySize = 256;
    req.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);
  }
};

class DummyReceiver : public IReceiver {
 public:
  virtual ~DummyReceiver() = default;

  void onNewMessage(const NodeNum sourceNode,
                    const char* const message,
                    const size_t messageLength,
                    NodeNum endpointNum) override {}
  void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override {}
};

shared_ptr<IncomingMsgsStorage> msgsStorage;
shared_ptr<DummyReceiver> msgReceiver = make_shared<DummyReceiver>();
shared_ptr<MsgHandlersRegistrator> msgHandlersRegPtr = make_shared<MsgHandlersRegistrator>();
shared_ptr<MsgsCommunicator> msgsCommunicator;
shared_ptr<ICommunication> communicatorPtr;
DummyRequestsHandler requestsHandler;

class DummyReplica : public InternalReplicaApi {
 public:
  explicit DummyReplica(bftEngine::impl::ReplicasInfo& repInfo) : replicasInfo_(repInfo) {}

  const bftEngine::impl::ReplicasInfo& getReplicasInfo() const override { return replicasInfo_; }
  bool isValidClient(NodeIdType) const override { return true; }
  bool isIdOfReplica(NodeIdType) const override { return false; }
  const set<ReplicaId>& getIdsOfPeerReplicas() const override { return replicaIds_; }
  ViewNum getCurrentView() const override { return 1; }
  ReplicaId currentPrimary() const override { return replicaConfig.replicaId; }

  bool isCurrentPrimary() const override { return primary_; }
  bool currentViewIsActive() const override { return true; }
  bool isReplyAlreadySentToClient(NodeIdType, ReqId) const override { return false; }
  bool isClientRequestInProcess(NodeIdType, ReqId) const override { return false; }

  IncomingMsgsStorage& getIncomingMsgsStorage() override { return *incomingMsgsStorage_; }
  concord::util::SimpleThreadPool& getInternalThreadPool() override { return pool_; }
  bool isCollectingState() const override { return false; }

  const ReplicaConfig& getReplicaConfig() const override { return replicaConfig; }
  SeqNum getPrimaryLastUsedSeqNum() const override { return 0; }
  uint64_t getRequestsInQueue() const override { return 0; }
  SeqNum getLastExecutedSeqNum() const override { return 0; }
  void registerStopCallback(std::function<void(void)> stopCallback) override { stopCallback_ = stopCallback; };
  void stop() override { stopCallback_(); }
  void setPrimary(bool primary) { primary_ = primary; };

 private:
  bool primary_ = true;
  IncomingMsgsStorage* incomingMsgsStorage_ = nullptr;
  concord::util::SimpleThreadPool pool_{""};
  bftEngine::impl::ReplicasInfo replicasInfo_;
  set<ReplicaId> replicaIds_;
  std::function<void()> stopCallback_;
};

class DummyPreProcessor : public PreProcessor {
 public:
  using PreProcessor::PreProcessor;

  bool validateRequestMsgCorrectness(const PreProcessRequestMsgSharedPtr& requestMsg) {
    return checkPreProcessRequestMsgCorrectness(requestMsg);
  }
  bool validateReplyMsgCorrectness(const PreProcessReplyMsgSharedPtr& replyMsg) {
    return checkPreProcessReplyMsgCorrectness(replyMsg);
  }
  bool validateBatchRequestMsgCorrectness(const PreProcessBatchReqMsgUniquePtr& batchReq) {
    return checkPreProcessBatchReqMsgCorrectness(batchReq);
  }
  bool validateBatchReplyMsgCorrectness(const PreProcessBatchReplyMsgUniquePtr& batchReply) {
    return checkPreProcessBatchReplyMsgCorrectness(batchReply);
  }
};

// clang-format off
const unordered_map<NodeIdType, string> replicaPrivKeys = {
    {replica_0, "61498efe1764b89357a02e2887d224154006ceacf26269f8695a4af561453eef"},
    {replica_1, "247a74ab3620ec6b9f5feab9ee1f86521da3fa2804ad45bb5bf2c5b21ef105bc"},
    {replica_2, "fb539bc3d66deda55524d903da26dbec1f4b6abf41ec5db521e617c64eb2c341"},
    {replica_3, "55ea66e855b83ec4a02bd8fcce6bb4426ad3db2a842fa2a2a6777f13e40a4717"},
    {replica_4, "f2f3d43da68329bfe31419636072e27cfd1a8fff259be4bfada667080eb00556"}
};

const unordered_map<NodeIdType, string> replicaPubKeys = {
    {replica_0, "386f4fb049a5d8bb0706d3793096c8f91842ce380dfc342a2001d50dfbc901f4"},
    {replica_1, "3f9e7dbde90477c24c1bacf14e073a356c1eca482d352d9cc0b16560a4e7e469"},
    {replica_2, "2311c6013ff657844669d8b803b2e1ed33fe06eed445f966a800a8fbb8d790e8"},
    {replica_3, "1ba7449655784fc9ce193a7887de1e4d3d35f7c82b802440c4f28bf678a34b34"},
    {replica_4, "c426c524c92ad9d0b740f68ee312abf0298051a7e0364a867b940e9693ae6095"}
};
// clang-format on

void switchReplicaContext(NodeIdType id) {
  LOG_INFO(GL, "Switching replica context to replica " << id);
  SigManager::reset(sigManager[id]);
  CryptoManager::reset(cryptoManager[id]);
}

void setUpConfiguration_4() {
  replicaConfig.replicaId = replica_0;
  replicaConfig.numReplicas = numOfReplicas_4;
  replicaConfig.fVal = fVal_4;
  replicaConfig.cVal = cVal;
  replicaConfig.numOfClientProxies = numOfInternalClients;
  replicaConfig.viewChangeTimerMillisec = viewChangeTimerMillisec;
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.numOfExternalClients = 15;
  replicaConfig.clientBatchingEnabled = true;

  std::vector<std::string> publicKeysVector;
  for (NodeIdType i = replica_0; i < replica_4; ++i) {
    replicaConfig.publicKeysOfReplicas.emplace(i, replicaPubKeys.at(i));
    publicKeysVector.push_back(replicaPubKeys.at(i));
  }
  replicaConfig.replicaPrivateKey = replicaPrivKeys.at(replica_0);

  for (auto i = 0; i < replicaConfig.numReplicas; i++) {
    replicaConfig.replicaId = i;
    replicasInfo[i] = std::make_unique<ReplicasInfo>(replicaConfig, true, true);
    sigManager[i] = SigManager::init(i,
                                     replicaPrivKeys.at(i),
                                     replicaConfig.publicKeysOfReplicas,
                                     concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                     nullptr,
                                     concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                     {},
                                     *replicasInfo[i].get());
    cryptoManager[i] =
        CryptoManager::init(std::make_unique<TestMultisigCryptoSystem>(i, publicKeysVector, replicaPrivKeys.at(i)));
  }
  replicaConfig.replicaId = replica_0;
}

void setUpConfiguration_7() {
  replicaConfig.replicaId = replica_4;
  replicaConfig.numReplicas = numOfReplicas_7;
  replicaConfig.fVal = fVal_7;

  replicaConfig.publicKeysOfReplicas.insert(pair<NodeIdType, const string>(replica_4, replicaPubKeys.at(replica_4)));
  replicaConfig.replicaPrivateKey = replicaPrivKeys.at(replica_4);
}

void setUpCommunication() {
  auto logger = logging::getLogger("preprocessor_test");
  TestCommConfig testCommConfig(logger);
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(
      true, replicaConfig.replicaId, replicaConfig.numOfClientProxies, replicaConfig.numReplicas, "");
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
      true, replicaConfig.replicaId, replicaConfig.numOfClientProxies, replicaConfig.numReplicas, "");
#elif USE_COMM_UDP
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(
      true, replicaConfig.replicaId, replicaConfig.numOfClientProxies, replicaConfig.numReplicas, "");
#endif

  communicatorPtr.reset(CommFactory::create(conf), [](ICommunication* c) {
    c->stop();
    delete c;
  });
  msgsCommunicator.reset(new MsgsCommunicator(communicatorPtr.get(), msgsStorage, msgReceiver));
  msgsCommunicator->startCommunication(replicaConfig.replicaId);
}

PreProcessReplyMsgSharedPtr preProcessNonPrimary(NodeIdType replicaId,
                                                 const bftEngine::impl::ReplicasInfo& repInfo,
                                                 ReplyStatus status,
                                                 OperationResult opResult) {
  switchReplicaContext(replicaId);
  auto preProcessReplyMsg = make_shared<PreProcessReplyMsg>(
      replicaId, clientId, 0, reqSeqNum, reqRetryId, buf, bufLen, "", status, opResult, viewNum);
  switchReplicaContext(repInfo.myId());
  preProcessReplyMsg->validate(repInfo);
  return preProcessReplyMsg;
}

void clearDiagnosticsHandlers() {
  auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.clear();
  registrar.status.clear();
}

class PreprocessingStateTest : public testing::Test {
 public:
  void SetUp() override { switchReplicaContext(replica_0); }
};

TEST_F(PreprocessingStateTest, notEnoughRepliesReceived) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  for (auto i = 1; i < numOfRequiredReplies - 1; i++) {
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
    ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  }
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, filterDuplication) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  auto numReplicas = 3;
  for (auto i = 1; i < numReplicas; i++) {
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
  }
  ConcordAssertEQ(reqState.getNumOfReceivedReplicas(), numReplicas - 1);
  // try to add the same reply
  for (auto i = 1; i < numReplicas; i++) {
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
  }
  ConcordAssertEQ(reqState.getNumOfReceivedReplicas(), numReplicas - 1);
  reqState.handlePreProcessReplyMsg(preProcessNonPrimary(numReplicas, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
  ConcordAssertEQ(reqState.getNumOfReceivedReplicas(), numReplicas);
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, notEnoughErrorRepliesReceived) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  for (auto i = 1; i < numOfRequiredReplies - 1; i++) {
    reqState.handlePreProcessReplyMsg(
        preProcessNonPrimary(i, repInfo, STATUS_FAILED, OperationResult::EXEC_DATA_TOO_LARGE));
    ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  }
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::EXEC_DATA_TOO_LARGE);
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, changePrimaryBlockId) {
  memset(buf, '5', bufLen);
  uint64_t blockId = 0;
  uint64_t primaryBlockId = 420;
  memcpy(buf + bufLen - sizeof(uint64_t), reinterpret_cast<char*>(&blockId), sizeof(uint64_t));
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  auto reply = preProcessNonPrimary(1, repInfo, STATUS_GOOD, OperationResult::SUCCESS);
  for (auto i = 1; i < numOfRequiredReplies + 1; i++) {
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
    ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  }
  // Set block id to other than the replicas
  memcpy(buf + bufLen - sizeof(uint64_t), reinterpret_cast<char*>(&primaryBlockId), sizeof(uint64_t));
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);

  concord::crypto::SHA3_256::Digest replicasHash = *((concord::crypto::SHA3_256::Digest*)reply->resultsHash());
  ConcordAssertNE(replicasHash, reqState.getResultHash());
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
  ConcordAssertEQ(replicasHash, reqState.getResultHash());
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, allRepliesReceivedButNotEnoughSameHashesCollected) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);
  for (auto i = 1; i < replicaConfig.numReplicas; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    memset(buf, i, bufLen);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
  }
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CANCEL);
}

TEST_F(PreprocessingStateTest, primarySucceededWhileNonPrimariesFailed_ForcedSuccess) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);
  for (auto i = 1; i < replicaConfig.numReplicas; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    reqState.handlePreProcessReplyMsg(
        preProcessNonPrimary(i, repInfo, STATUS_FAILED, OperationResult::EXEC_DATA_TOO_LARGE));
  }
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
}

TEST_F(PreprocessingStateTest, enoughSameRepliesReceived) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != numOfRequiredReplies - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
  }
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, enoughSameErrorRepliesReceived) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != numOfRequiredReplies - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    reqState.handlePreProcessReplyMsg(
        preProcessNonPrimary(i, repInfo, STATUS_FAILED, OperationResult::EXEC_DATA_TOO_LARGE));
  }
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::EXEC_DATA_TOO_LARGE);
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, primaryReplicaPreProcessingIsDifferentThanOneThatPassedConsensus) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    reqState.handlePreProcessReplyMsg(
        preProcessNonPrimary(i, repInfo, STATUS_FAILED, OperationResult::EXEC_DATA_TOO_LARGE));
  }
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, primaryReplicaDidNotCompletePreProcessingWhileNonPrimariesDid) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo, STATUS_GOOD, OperationResult::SUCCESS));
  }
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::SUCCESS);
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
}

TEST_F(PreprocessingStateTest, primaryReplicaDidNotCompleteErrorPreProcessingWhileNonPrimariesDid) {
  RequestProcessingState reqState(replicaConfig.replicaId,
                                  replicaConfig.numReplicas,
                                  "",
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
    reqState.handlePreProcessReplyMsg(
        preProcessNonPrimary(i, repInfo, STATUS_FAILED, OperationResult::EXEC_DATA_TOO_LARGE));
  }
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), CONTINUE);
  reqState.handlePrimaryPreProcessed(buf, bufLen, OperationResult::EXEC_DATA_TOO_LARGE);
  ConcordAssertEQ(reqState.definePreProcessingConsensusResult(), COMPLETE);
}

TEST_F(PreprocessingStateTest, validatePreProcessBatchRequestMsg) {
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);

  PreProcessReqMsgsList batch;
  uint overallReqSize = 0;
  const auto numOfMsgs = 3;
  const auto senderId = 2;
  for (uint i = 0; i < numOfMsgs; i++) {
    auto preProcessReqMsg = make_shared<PreProcessRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                                              senderId,
                                                              clientId,
                                                              i,
                                                              reqSeqNum + i,
                                                              i,
                                                              bufLen,
                                                              buf,
                                                              cid + to_string(i + 1),
                                                              nullptr,
                                                              0,
                                                              GlobalData::current_block_id,
                                                              viewNum);
    batch.push_back(preProcessReqMsg);
    overallReqSize += preProcessReqMsg->size();
  }
  auto preProcessBatchReqMsg = make_shared<PreProcessBatchRequestMsg>(
      REQ_TYPE_PRE_PROCESS, clientId, senderId, batch, cid, overallReqSize, viewNum);
  preProcessBatchReqMsg->validate(repInfo);
  const auto msgs = preProcessBatchReqMsg->getPreProcessRequestMsgs();
  ConcordAssertEQ(preProcessBatchReqMsg->clientId(), clientId);
  ConcordAssertEQ(preProcessBatchReqMsg->senderId(), senderId);
  ConcordAssertEQ(preProcessBatchReqMsg->getCid(), cid);
  ConcordAssertEQ(msgs.size(), numOfMsgs);
  uint i = 0;
  for (const auto& msg : msgs) {
    msg->validate(repInfo);
    ConcordAssertEQ(msg->clientId(), clientId);
    ConcordAssertEQ(msg->senderId(), senderId);
    ConcordAssertEQ(msg->reqSeqNum(), (int64_t)reqSeqNum + i);
    ConcordAssertEQ(msg->reqOffsetInBatch(), i);
    ConcordAssertEQ(msg->getCid(), cid + to_string(i + 1));
    ConcordAssertEQ(msg->requestLength(), bufLen);
    i++;
  }
}

TEST_F(PreprocessingStateTest, validatePreProcessBatchReplyMsg) {
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);

  PreProcessReplyMsgsList batch;
  uint overallRepliesSize = 0;
  const auto numOfMsgs = 3;
  const auto senderId = 2;
  switchReplicaContext(senderId);
  for (uint i = 0; i < numOfMsgs; i++) {
    auto preProcessReplyMsg = make_shared<PreProcessReplyMsg>(senderId,
                                                              clientId,
                                                              i,
                                                              reqSeqNum + i,
                                                              i,
                                                              buf,
                                                              bufLen,
                                                              cid + to_string(i + 1),
                                                              STATUS_GOOD,
                                                              OperationResult::SUCCESS,
                                                              viewNum);
    batch.push_back(preProcessReplyMsg);
    overallRepliesSize += preProcessReplyMsg->size();
  }
  auto preProcessBatchReplyMsg =
      make_shared<PreProcessBatchReplyMsg>(clientId, senderId, batch, cid, overallRepliesSize, viewNum);
  preProcessBatchReplyMsg->validate(repInfo);
  const auto msgs = preProcessBatchReplyMsg->getPreProcessReplyMsgs();
  ConcordAssertEQ(preProcessBatchReplyMsg->clientId(), clientId);
  ConcordAssertEQ(preProcessBatchReplyMsg->senderId(), senderId);
  ConcordAssertEQ(preProcessBatchReplyMsg->getCid(), cid);
  ConcordAssertEQ(msgs.size(), numOfMsgs);
  uint i = 0;
  switchReplicaContext(repInfo.myId());
  for (const auto& msg : msgs) {
    msg->validate(repInfo);
    ConcordAssertEQ(msg->clientId(), clientId);
    ConcordAssertEQ(msg->senderId(), senderId);
    ConcordAssertEQ(msg->reqSeqNum(), (int64_t)reqSeqNum + i);
    ConcordAssertEQ(msg->reqOffsetInBatch(), i);
    ConcordAssertEQ(msg->getCid(), cid + to_string(i + 1));
    i++;
  }
}

// Verify that requests and the batch have been successfully released in case no pre-processing consensus reached
TEST_F(PreprocessingStateTest, batchCancelledNoConsensusReached) {
  const uint numOfReplicas = 4;

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(true);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_0;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  // Create the client batch request message
  auto reqMsgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientBatchRequest);
  deque<ClientRequestMsg*> reqBatch;
  const uint numOfMsgsInBatch = 2;
  uint batchSize = 0;
  for (uint i = 0; i < numOfMsgsInBatch; i++) {
    auto* clientReqMsg = new ClientRequestMsg(clientId, 2, i + 5, bufLen, buf, reqTimeoutMilli, to_string(i + 5));
    reqBatch.push_back(clientReqMsg);
    batchSize += clientReqMsg->size();
  }
  auto clientBatchReqMsg = make_unique<ClientBatchRequestMsg>(clientId, reqBatch, batchSize, cid);
  // Call the PreProcessor callback to handle ClientBatchRequest message
  reqMsgHandlerCallback(std::move(clientBatchReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  const auto& ongoingBatch = preProcessor.getOngoingBatchForClient(clientId);
  // Verify that the requests/batch have been registered
  ConcordAssertEQ(ongoingBatch->isBatchInProcess(cid), true);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 5);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 6);

  // Create the client batch reply message
  auto replyMsgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::PreProcessBatchReply);
  uint overallRepliesSize = 0;
  for (uint senderId = 1; senderId < numOfReplicas; senderId++) {
    switchReplicaContext(senderId);
    PreProcessReplyMsgsList repliesList;
    for (uint i = 0; i < numOfMsgsInBatch; i++) {
      auto preProcessReplyMsg = make_shared<PreProcessReplyMsg>(senderId,
                                                                clientId,
                                                                i,
                                                                i + 5,
                                                                i,
                                                                buf,
                                                                bufLen,
                                                                to_string(i + 5),
                                                                STATUS_GOOD,
                                                                OperationResult::SUCCESS,
                                                                viewNum);
      repliesList.push_back(preProcessReplyMsg);
      overallRepliesSize += preProcessReplyMsg->size();
    }
    // Call the PreProcessor callback to handle PreProcessBatchReplyMsg message
    replyMsgHandlerCallback(
        std::make_unique<PreProcessBatchReplyMsg>(clientId, senderId, repliesList, cid, overallRepliesSize, viewNum));
  }

  usleep(reqTimeoutMilli * 10000);
  // Verify that the requests/batch have been released
  ASSERT_EQ(ongoingBatch->isBatchInProcess(), false);
  ASSERT_EQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  ASSERT_EQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 0);

  clearDiagnosticsHandlers();
  for (auto& req : reqBatch) delete req;
  replica.stop();
}

TEST_F(PreprocessingStateTest, requestTimedOut) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientPreProcessRequest);
  auto clientReqMsg = make_unique<ClientPreProcessRequestMsg>(clientId, reqSeqNum, bufLen, buf, reqTimeoutMilli, cid);
  msgHandlerCallback(std::move(clientReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), reqSeqNum);
  usleep(replicaConfig.preExecReqStatusCheckTimerMillisec * 1000);
  timers.evaluate();
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  replica.stop();
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, primaryCrashDetected) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(false);
  concordUtil::Timers timers;
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientPreProcessRequest);
  auto clientReqMsg = make_unique<ClientPreProcessRequestMsg>(clientId, reqSeqNum, bufLen, buf, reqTimeoutMilli, cid);
  msgHandlerCallback(std::move(clientReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), reqSeqNum);

  usleep(reqWaitTimeoutMilli * 1000);
  timers.evaluate();
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  replica.stop();
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, primaryCrashNotDetected) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(false);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientPreProcessRequest);
  auto clientReqMsg = make_unique<ClientPreProcessRequestMsg>(clientId, reqSeqNum, bufLen, buf, reqTimeoutMilli, cid);
  msgHandlerCallback(std::move(clientReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), reqSeqNum);

  auto preProcessReqMsg = make_unique<PreProcessRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                                            replica.currentPrimary(),
                                                            clientId,
                                                            0,
                                                            reqSeqNum,
                                                            reqRetryId,
                                                            bufLen,
                                                            buf,
                                                            cid,
                                                            nullptr,
                                                            0,
                                                            GlobalData::current_block_id,
                                                            replica.getCurrentView(),
                                                            span);
  msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::PreProcessRequest);
  msgHandlerCallback(std::move(preProcessReqMsg));
  usleep(reqWaitTimeoutMilli * 1000 / 2);  // Wait for the pre-execution completion
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  replica.stop();
  clearDiagnosticsHandlers();
}

// Verify that in case of timed out requests, the requests and the batch get properly released
TEST_F(PreprocessingStateTest, batchMsgTimedOutOnNonPrimary) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(false);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientBatchRequest);
  deque<ClientRequestMsg*> batch;
  uint batchSize = 0;
  for (uint i = 0; i < 3; i++) {
    auto* clientReqMsg = new ClientRequestMsg(clientId, 2, i + 5, bufLen, buf, reqTimeoutMilli, to_string(i + 5));
    batch.push_back(clientReqMsg);
    batchSize += clientReqMsg->size();
  }
  auto clientBatchReqMsg = make_unique<ClientBatchRequestMsg>(clientId, batch, batchSize, cid);
  msgHandlerCallback(std::move(clientBatchReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  const auto& ongoingBatch = preProcessor.getOngoingBatchForClient(clientId);
  // Verify that the requests and the batch have been registered
  ConcordAssertEQ(ongoingBatch->isBatchRegistered(cid), true);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 5);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 6);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 2), 7);

  usleep(replicaConfig.preExecReqStatusCheckTimerMillisec * 1000);
  timers.evaluate();
  // Verify that the requests and the batch have been released
  ConcordAssertEQ(ongoingBatch->isBatchInProcess(), false);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 2), 0);
  clearDiagnosticsHandlers();

  for (auto& b : batch) {
    delete b;
  }
  replica.stop();
}

// Verify that in case of timed out requests, the requests and the batch get properly released
TEST_F(PreprocessingStateTest, batchMsgTimedOutOnPrimary) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(true);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientBatchRequest);
  deque<ClientRequestMsg*> batch;
  uint batchSize = 0;
  for (uint i = 0; i < 3; i++) {
    auto* clientReqMsg = new ClientRequestMsg(clientId, 2, i + 5, bufLen, buf, reqTimeoutMilli, to_string(i + 5));
    batch.push_back(clientReqMsg);
    batchSize += clientReqMsg->size();
  }
  auto clientBatchReqMsg = make_unique<ClientBatchRequestMsg>(clientId, batch, batchSize, cid);
  msgHandlerCallback(std::move(clientBatchReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  const auto& ongoingBatch = preProcessor.getOngoingBatchForClient(clientId);
  // Verify that the requests and the batch have been registered
  ConcordAssertEQ(ongoingBatch->isBatchInProcess(cid), true);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 5);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 6);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 2), 7);

  usleep(reqTimeoutMilli * 1000 * 4);
  timers.evaluate();
  // Verify that the requests and the batch have been released
  ConcordAssertEQ(ongoingBatch->isBatchInProcess(), false);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 2), 0);
  clearDiagnosticsHandlers();

  for (auto& b : batch) {
    delete b;
  }
  replica.stop();
}

TEST_F(PreprocessingStateTest, handlePreProcessBatchRequestMsgByNonPrimary) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(false);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  PreProcessReqMsgsList batch;
  uint overallReqSize = 0;
  const auto numOfMsgs = 4;
  for (uint i = 0; i < numOfMsgs; i++) {
    auto preProcessReqMsg = make_shared<PreProcessRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                                              1,
                                                              clientId,
                                                              i,
                                                              i + 5,
                                                              i,
                                                              bufLen,
                                                              buf,
                                                              to_string(i + 1),
                                                              nullptr,
                                                              0,
                                                              0,
                                                              replica.getCurrentView());
    batch.push_back(preProcessReqMsg);
    overallReqSize += preProcessReqMsg->size();
  }
  auto preProcessBatchReqMsg = make_unique<PreProcessBatchRequestMsg>(
      REQ_TYPE_PRE_PROCESS, clientId, 1, batch, cid, overallReqSize, replica.getCurrentView());
  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::PreProcessBatchRequest);
  msgHandlerCallback(std::move(preProcessBatchReqMsg));
  usleep(waitForExecTimerMillisec * 1000);
  const auto& ongoingBatch = preProcessor.getOngoingBatchForClient(clientId);
  // Verify that the requests and the batch have been released
  ConcordAssertEQ(ongoingBatch->isBatchInProcess(), false);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 0), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 1), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 2), 0);
  ConcordAssertEQ(preProcessor.getOngoingReqIdForClient(clientId, 3), 0);
  replica.stop();
  clearDiagnosticsHandlers();
}

TEST_F(PreprocessingStateTest, rejectMsgWithInvalidView) {
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, false, false);
  DummyReplica replica(repInfo);
  replica.setPrimary(false);
  concordUtil::Timers timers;
  DummyPreProcessor preProcessor(
      msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  ViewNum oldViewNum = 0;
  ViewNum currentView = replica.getCurrentView();
  NodeIdType senderId = 2;

  auto requestMsgValidView = make_shared<PreProcessRequestMsg>(
      REQ_TYPE_PRE_PROCESS, senderId, clientId, 0, reqSeqNum, reqRetryId, bufLen, buf, cid, nullptr, 0, 0, currentView);

  auto requestMsgInvalidView = make_shared<PreProcessRequestMsg>(
      REQ_TYPE_PRE_PROCESS, senderId, clientId, 0, reqSeqNum, reqRetryId, bufLen, buf, cid, nullptr, 0, 0, oldViewNum);

  ConcordAssert(preProcessor.validateRequestMsgCorrectness(requestMsgValidView));
  ConcordAssert(!preProcessor.validateRequestMsgCorrectness(requestMsgInvalidView));

  auto replyMsgValidView = make_shared<PreProcessReplyMsg>(senderId,
                                                           clientId,
                                                           0,
                                                           reqSeqNum,
                                                           reqRetryId,
                                                           buf,
                                                           bufLen,
                                                           cid,
                                                           STATUS_GOOD,
                                                           OperationResult::SUCCESS,
                                                           currentView);
  auto replyMsgInvalidView = make_shared<PreProcessReplyMsg>(senderId,
                                                             clientId,
                                                             0,
                                                             reqSeqNum,
                                                             reqRetryId,
                                                             buf,
                                                             bufLen,
                                                             cid,
                                                             STATUS_GOOD,
                                                             OperationResult::SUCCESS,
                                                             oldViewNum);

  ConcordAssert(preProcessor.validateReplyMsgCorrectness(replyMsgValidView));
  ConcordAssert(!preProcessor.validateReplyMsgCorrectness(replyMsgInvalidView));

  PreProcessReqMsgsList reqBatch;
  uint overallReqSize = 0;
  const auto numOfMsgs = 3;
  for (uint i = 0; i < numOfMsgs; i++) {
    auto preProcessReqMsg = make_shared<PreProcessRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                                              senderId,
                                                              clientId,
                                                              i,
                                                              i + 5,
                                                              i,
                                                              bufLen,
                                                              buf,
                                                              to_string(i + 1),
                                                              nullptr,
                                                              0,
                                                              0,
                                                              currentView);
    reqBatch.push_back(preProcessReqMsg);
    overallReqSize += preProcessReqMsg->size();
  }

  auto batchRequestValidView = make_unique<PreProcessBatchRequestMsg>(
      REQ_TYPE_PRE_PROCESS, clientId, senderId, reqBatch, cid, overallReqSize, currentView);
  auto batchRequestInvalidView = make_unique<PreProcessBatchRequestMsg>(
      REQ_TYPE_PRE_PROCESS, clientId, senderId, reqBatch, cid, overallReqSize, oldViewNum);

  ConcordAssert(preProcessor.validateBatchRequestMsgCorrectness(batchRequestValidView));
  ConcordAssert(!preProcessor.validateBatchRequestMsgCorrectness(batchRequestInvalidView));

  switchReplicaContext(senderId);
  PreProcessReplyMsgsList batch;
  uint overallRepliesSize = 0;
  for (uint i = 0; i < numOfMsgs; i++) {
    auto preProcessReplyMsg = make_shared<PreProcessReplyMsg>(senderId,
                                                              clientId,
                                                              i,
                                                              reqSeqNum + i,
                                                              i,
                                                              buf,
                                                              bufLen,
                                                              cid + to_string(i + 1),
                                                              STATUS_GOOD,
                                                              OperationResult::SUCCESS,
                                                              currentView);
    batch.push_back(preProcessReplyMsg);
    overallRepliesSize += preProcessReplyMsg->size();
  }

  auto batchReplyValidView =
      make_unique<PreProcessBatchReplyMsg>(clientId, senderId, batch, cid, overallRepliesSize, currentView);
  auto batchReplyInvalidView =
      make_unique<PreProcessBatchReplyMsg>(clientId, senderId, batch, cid, overallRepliesSize, oldViewNum);

  replica.setPrimary(true);
  switchReplicaContext(repInfo.myId());
  ConcordAssert(preProcessor.validateBatchReplyMsgCorrectness(batchReplyValidView));
  ConcordAssert(!preProcessor.validateBatchReplyMsgCorrectness(batchReplyInvalidView));
  replica.stop();
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  logging::initLogger("logging.properties");
  logging::Logger::getInstance("preprocessor_test").setLogLevel(log4cplus::ERROR_LOG_LEVEL);
  setUpConfiguration_4();
  RequestProcessingState::init(numOfRequiredReplies, &preProcessorRecorder);
  PreProcessReplyMsg::setPreProcessorHistograms(&preProcessorRecorder);
  const chrono::milliseconds msgTimeOut(20000);
  msgsStorage = make_shared<IncomingMsgsStorageImp>(msgHandlersRegPtr, msgTimeOut, replicaConfig.replicaId);
  setUpCommunication();
  memset(buf, bufLen, 5);
  int res = RUN_ALL_TESTS();
  communicatorPtr.reset();
  return res;
}
