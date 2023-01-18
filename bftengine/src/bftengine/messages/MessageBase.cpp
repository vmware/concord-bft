// Concord
//
// Copyright (c) 2018-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <cstring>

#include "MessageBase.hpp"

#include "log/logger.hpp"
#include "assertUtils.hpp"
#include "ReplicaConfig.hpp"

#ifdef DEBUG_MEMORY_MSG
#include <set>

#ifdef USE_TLS
#error DEBUG_MEMORY_MSG is not supported with USE_TLS
#endif

namespace bftEngine {
namespace impl {

static std::set<MessageBase *> liveMessagesDebug;  // GG: if needed, add debug information

void MessageBase::printLiveMessages() {
  printf("\nDumping all live messages:");
  for (std::set<MessageBase *>::iterator it = liveMessagesDebug.begin(); it != liveMessagesDebug.end(); it++) {
    printf("<type=%d, size=%d>", (*it)->type(), (*it)->size());
    printf("%8s", " ");  // space
  }
  printf("\n");
}

}  // namespace impl
}  // namespace bftEngine

#endif

namespace bftEngine {
namespace impl {

MessageBase::~MessageBase() {
#ifdef DEBUG_MEMORY_MSG
  liveMessagesDebug.erase(this);
#endif
}

void MessageBase::shrinkToFit() {
  ConcordAssert(msgBody_.get() != nullptr);

  msgBody_->resize(msgSize_);
  msgBody_->shrink_to_fit();
  storageSize_ = msgSize_;
}

bool MessageBase::reallocSize(uint32_t size) {
  ConcordAssert(msgBody_.get() != nullptr);
  ConcordAssert(size >= msgSize_);

  msgBody_->resize(size);
  if (msgBody_.get() == nullptr) {
    return false;
  } else {
    storageSize_ = size;
    msgSize_ = size;
    return true;
  }
}

MessageBase::MessageBase(NodeIdType sender, MsgType type, MsgSize size) : MessageBase(sender, type, 0u, size) {}

MessageBase::MessageBase(NodeIdType sender, MsgType type, SpanContextSize spanContextSize, MsgSize size) {
  ConcordAssert(size > 0);
  size = size + spanContextSize;
  msgBody_ = std::make_unique<MESSAGE_BODY>(size);
  storageSize_ = size;
  msgSize_ = size;
  sender_ = sender;
  reinterpret_cast<Header *>(msgBody_->data())->msgType = type;
  reinterpret_cast<Header *>(msgBody_->data())->spanContextSize = spanContextSize;

#ifdef DEBUG_MEMORY_MSG
  liveMessagesDebug.insert(this);
#endif
}

MessageBase::MessageBase(NodeIdType sender, std::unique_ptr<MESSAGE_BODY> body, MsgSize size) {
  msgBody_ = std::move(body);
  msgSize_ = size;
  storageSize_ = size;
  sender_ = sender;

#ifdef DEBUG_MEMORY_MSG
  liveMessagesDebug.insert(this);
#endif
}

void MessageBase::validate(const ReplicasInfo &) const {
  LOG_DEBUG(GL, "Calling MessageBase::validate on a message of type " << type());
}

bool MessageBase::shouldValidateAsync() const { return false; }
void MessageBase::setMsgSize(MsgSize size) {
  ConcordAssert(msgBody_.get() != nullptr);
  ConcordAssert(size <= storageSize_);

  msgSize_ = size;
}

MessageBase *MessageBase::cloneObjAndMsg() const {
  ConcordAssert(msgBody_.get() != nullptr);
  ConcordAssert(msgSize_ > 0);

  auto msgBody = std::make_unique<MESSAGE_BODY>(msgSize_);
  *msgBody = *msgBody_;
  MessageBase *otherMsg = new MessageBase(sender_, std::move(msgBody), msgSize_);
  return otherMsg;
}

void MessageBase::writeObjAndMsgToLocalBuffer(char *buffer, size_t bufferLength, size_t *actualSize) const {
  ConcordAssert(msgBody_.get() != nullptr);
  ConcordAssert(msgSize_ > 0);

  const size_t sizeNeeded = sizeof(RawHeaderOfObjAndMsg) + msgSize_;
  ConcordAssert(sizeNeeded <= bufferLength);

  RawHeaderOfObjAndMsg *pHeader = (RawHeaderOfObjAndMsg *)buffer;
  pHeader->magicNum = magicNumOfRawFormat;
  pHeader->msgSize = msgSize_;
  pHeader->sender = sender_;

  char *pRawMsg = buffer + sizeof(RawHeaderOfObjAndMsg);
  memcpy(pRawMsg, msgBody_->data(), msgSize_);
  if (actualSize) *actualSize = sizeNeeded;
}

size_t MessageBase::sizeNeededForObjAndMsgInLocalBuffer() const {
  ConcordAssert(msgBody_.get() != nullptr);
  ConcordAssert(msgSize_ > 0);

  const size_t sizeNeeded = sizeof(RawHeaderOfObjAndMsg) + msgSize_;
  return sizeNeeded;
}

MessageBase *MessageBase::createObjAndMsgFromLocalBuffer(char *buffer, size_t bufferLength, size_t *actualSize) {
  if (actualSize) *actualSize = 0;
  if (bufferLength <= sizeof(RawHeaderOfObjAndMsg)) return nullptr;

  RawHeaderOfObjAndMsg *pHeader = (RawHeaderOfObjAndMsg *)buffer;
  if (pHeader->magicNum != magicNumOfRawFormat) return nullptr;
  if (pHeader->msgSize == 0) return nullptr;
  if (pHeader->msgSize > ReplicaConfig::instance().getmaxExternalMessageSize()) return nullptr;
  if (pHeader->msgSize + sizeof(RawHeaderOfObjAndMsg) > bufferLength) return nullptr;

  char *pBodyInBuffer = buffer + sizeof(RawHeaderOfObjAndMsg);

  auto msgBody = std::make_unique<MESSAGE_BODY>(pHeader->msgSize);
  memcpy(msgBody->data(), pBodyInBuffer, pHeader->msgSize);
  MessageBase *msgObj = new MessageBase(pHeader->sender, std::move(msgBody), pHeader->msgSize);

  if (actualSize) *actualSize = (pHeader->msgSize + sizeof(RawHeaderOfObjAndMsg));
  return msgObj;
}

bool MessageBase::equals(const MessageBase &other) const {
  bool equals = (other.msgSize_ == msgSize_ && other.storageSize_ == storageSize_ && other.sender_ == sender_ &&
                 ((other.msgBody_.get() && msgBody_.get()) || (!other.msgBody_.get() && !msgBody_.get())));
  if (!equals) return false;
  return (memcmp(other.msgBody_->data(), msgBody_->data(), msgSize_) == 0);
}

size_t MessageBase::serializeMsg(char *&buf, char *msg) {
  return serializeMsg(buf, reinterpret_cast<const MessageBase *>(msg));
}

size_t MessageBase::serializeMsg(char *&buf, const MessageBase *msg) {
  // As messages could be empty (nullptr), an additional flag is required to
  // distinguish between empty and filled ones.
  uint8_t msgFilledFlag = (msg != nullptr) ? 1 : 0;
  uint32_t msgFilledFlagSize = sizeof(msgFilledFlag);
  std::memcpy(buf, &msgFilledFlag, msgFilledFlagSize);
  buf += msgFilledFlagSize;

  size_t actualMsgSize = 0;
  if (msg) {
    uint32_t msgSize = msg->sizeNeededForObjAndMsgInLocalBuffer();
    msg->writeObjAndMsgToLocalBuffer(buf, msgSize, &actualMsgSize);
    ConcordAssert(actualMsgSize != 0);
    buf += actualMsgSize;
  }
  return msgFilledFlagSize + actualMsgSize;
}

MessageBase *MessageBase::deserializeMsg(char *&buf, size_t bufLen, size_t &actualSize) {
  uint8_t msgFilledFlag = 1;
  uint32_t msgFilledFlagSize = sizeof(msgFilledFlag);
  std::memcpy(&msgFilledFlag, buf, msgFilledFlagSize);
  buf += msgFilledFlagSize;

  MessageBase *msg = nullptr;
  size_t msgSize = 0;
  if (msgFilledFlag) {
    msg = createObjAndMsgFromLocalBuffer(buf, bufLen, &msgSize);
    ConcordAssert(msgSize != 0);
    buf += msgSize;
  }
  actualSize = msgFilledFlagSize + msgSize;
  return msg;
}

}  // namespace impl
}  // namespace bftEngine
