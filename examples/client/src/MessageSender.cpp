// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "MessageSender.hpp"
#include "KVMessageConsumer.hpp"
#include "KVMessageGenerator.hpp"

using namespace concord::osexample;

std::unique_ptr<IMessageGenerator> MessageSender::getMessageGenerator(uint16_t executionEngineType) {
  switch (executionEngineType) {
    case ExecutionEngineType::KV:
      return std::make_unique<KVMessageGenerator>(msgConfig_);
    /*case ExecutionEngineType::WASM:               // TODO: Need to implement in phase 2
      return std::make_unique<WasmMessageGenerator>();
    case ExecutionEngineType::ETHEREUM:
      return std::make_unique<EthereumMessageGenerator> ();*/
    default:
      LOG_ERROR(getLogger(), "None supported execution engine " << KVLOG(executionEngineType));
      ConcordAssert(false);
      return nullptr;
  }
}

std::unique_ptr<IMessageConsumer> MessageSender::getMessageConsumer(uint16_t executionEngineType) {
  switch (executionEngineType) {
    case ExecutionEngineType::KV:
      return std::make_unique<KVMessageConsumer>();
    /*case ExecutionEngineType::WASM:               // TODO: Need to implement in phase 2
      return std::make_unique<WasmMessageConsumer>();
    case ExecutionEngineType::ETHEREUM:
      return std::make_unique<EthereumMessageConsumer> ();*/
    default:
      LOG_ERROR(getLogger(), "None supported execution engine " << KVLOG(executionEngineType));
      ConcordAssert(false);
      return nullptr;
  }
}

MessageSender::MessageSender(std::shared_ptr<SetupClient> setupClient, std::shared_ptr<MessageConfig> msgConfig)
    : msgConfig_(msgConfig) {
  // Create communication and start bft client
  std::unique_ptr<bft::communication::ICommunication> comm_ptr(setupClient->createCommunication());
  ConcordAssertNE(comm_ptr, nullptr);

  bftClient_ = std::make_unique<bft::client::Client>(std::move(comm_ptr), setupClient->setupClientConfig());
  ConcordAssertNE(bftClient_, nullptr);

  bftClient_->setAggregator(std::make_shared<concordMetrics::Aggregator>());

  // get message generator and message consumer based on execution engine type
  uint16_t executionEngineType = setupClient->getExecutionEngineType();
  msgGenerator_ = getMessageGenerator(executionEngineType);
  msgConsumer_ = getMessageConsumer(executionEngineType);

  ConcordAssertNE(msgGenerator_, nullptr);
  ConcordAssertNE(msgConsumer_, nullptr);
}

uint64_t MessageSender::generateUniqueSequenceNumber() {
  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

  if (milli > lastMilliOfUniqueFetchID_) {
    lastMilliOfUniqueFetchID_ = milli;
    lastCountOfUniqueFetchID_ = 0;
  } else {
    if (lastCountOfUniqueFetchID_ == lastCountLimit_) {
      LOG_WARN(getLogger(), "SeqNum Counter reached max value");
      lastMilliOfUniqueFetchID_++;
      lastCountOfUniqueFetchID_ = 0;
    } else {  // increase last count to preserve uniqueness.
      lastCountOfUniqueFetchID_++;
    }
  }
  // shift lastMilli by 22 (0x3FFFFF) in order to 'bitwise or' with lastCount
  // and preserve uniqueness and monotonicity.
  uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
  ConcordAssert(lastCountOfUniqueFetchID_ <= 0x3FFFFF);
  r = r | ((uint64_t)lastCountOfUniqueFetchID_);

  return r;
}

void MessageSender::sendRequests() {
  switch (msgConfig_->messageType) {
    case MessageType::READ:
      createAndSendReadRequest();
      break;
    case MessageType::WRITE:
      createAndSendWriteRequest();
      break;
    case MessageType::BOTH:
      createAndSendWriteRequest();
      createAndSendReadRequest();
      break;
    default:
      LOG_ERROR(getLogger(), "None supported message type " << KVLOG(msgConfig_->messageType));
      ConcordAssert(false);
      break;
  }
}

void MessageSender::createAndSendWriteRequest() {
  // Create write Request based on number of operations
  uint16_t numberOfRequest = msgConfig_->numberOfOperation;
  for (uint16_t index = 0; index < numberOfRequest; index++) {
    bft::client::Msg msg = msgGenerator_->createWriteRequest();
    sendWriteRequest(msg);
  }
}

void MessageSender::createAndSendReadRequest() {
  // Create read Request based on number of operations
  uint16_t numberOfRequest = msgConfig_->numberOfOperation;
  for (uint16_t index = 0; index < numberOfRequest; index++) {
    bft::client::Msg msg = msgGenerator_->createReadRequest();
    sendReadRequest(msg);
  }
}

void MessageSender::sendWriteRequest(bft::client::Msg& msg) {
  bft::client::Reply rep;

  try {
    // create bft client write config
    bft::client::RequestConfig request_config;
    uint64_t clientReqSeqNum = generateUniqueSequenceNumber();
    request_config.correlation_id = std::to_string(clientReqSeqNum) + "-" + std::to_string(msgConfig_->senderNodeId);
    request_config.sequence_number = clientReqSeqNum;
    request_config.timeout = std::chrono::milliseconds(msgConfig_->reqTimeoutMilli);
    bft::client::WriteConfig write_config{request_config, bft::client::LinearizableQuorum{}};
    {
      std::lock_guard<std::mutex> lock(bftClientLock_);
      rep = bftClient_->send(write_config, std::move(msg));
    }
    msgConsumer_->deserialize(rep, false);

  } catch (std::exception& e) {
    LOG_WARN(getLogger(), "error while initiating bft request " << e.what());
  }
}

void MessageSender::sendReadRequest(bft::client::Msg& msg) {
  bft::client::Reply rep;

  try {
    // create bft client read config
    bft::client::RequestConfig request_config;
    uint64_t clientReqSeqNum = generateUniqueSequenceNumber();
    request_config.correlation_id = std::to_string(clientReqSeqNum) + "-" + std::to_string(msgConfig_->senderNodeId);
    request_config.sequence_number = clientReqSeqNum;
    request_config.max_reply_size = 1024 * 1024;  // Max reply size is 1MB
    request_config.timeout = std::chrono::milliseconds(msgConfig_->reqTimeoutMilli);
    bft::client::ReadConfig read_config{request_config, bft::client::LinearizableQuorum{}};
    {
      std::lock_guard<std::mutex> lock(bftClientLock_);
      rep = bftClient_->send(read_config, std::move(msg));
    }
    msgConsumer_->deserialize(rep, true);

  } catch (std::exception& e) {
    LOG_WARN(getLogger(), "error while initiating bft request " << e.what());
  }
}
