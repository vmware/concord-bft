// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "MessageBase.hpp"
#include "log/logger.hpp"
#include "util/assertUtils.hpp"
#include "ReplicaConfig.hpp"

#include <cstring>
#include <sstream>

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

// static class members for diagnostics server
std::array<std::atomic<size_t>, MsgCode::LastMsgCodeVal> MessageBase::Statistics::AliveIncomingExtrnMsgsBufs{};
static_assert(MsgCode::LastMsgCodeVal < 2000,
              "MessageBase AliveIncomingExtrnMsgsBufs array too big (above 2000), check MsgCode enum definition");
Bitmap MessageBase::Statistics::IncomingExtrnMsgReceivedAtLeastOnceFlags{MsgCode::LastMsgCodeVal};
std::mutex MessageBase::Statistics::messagesStatsMonitoringMutex_{};
std::atomic<size_t> MessageBase::Statistics::numIncomingExtrnMsgsBufAllocs{0};
std::atomic<size_t> MessageBase::Statistics::numIncomingExtrnMsgsBufFrees{0};
// End of static class members for diagnostics server

MessageBase::~MessageBase() {
#ifdef DEBUG_MEMORY_MSG
  liveMessagesDebug.erase(this);
#endif

  // if this obj is not the owner of the buf (owner_ is false), the buf may have already be freed - hence access to it
  // is prohibited
  if (owner_) {
    if (isIncomingMsg_) {
      MsgCode::Type msg_code = static_cast<MsgCode::Type>(msgBody_->msgType);
      MessageBase::Statistics::updateDiagnosticsCountersOnBufRelease(msg_code);
    }
    std::free((char *)msgBody_);
  }
}

void MessageBase::Statistics::updateDiagnosticsCountersOnBufAlloc(MsgCode::Type msg_code) {
  if (!MessageBase::Statistics::IncomingExtrnMsgReceivedAtLeastOnceFlags.get(msg_code)) {
    // here is a very rare event - first time ever receiving msg of type msg_code, hence
    // using a lock here shouldn't affect performnce. in other places we avoid locks where possible.
    // It is possible that two threads will call the "set" function but it's ok since it's
    // just a flag telling us if the message was received at least once. setting the bit twice causes
    // no damage.
    std::lock_guard<std::mutex> lock(MessageBase::Statistics::messagesStatsMonitoringMutex_);
    MessageBase::Statistics::IncomingExtrnMsgReceivedAtLeastOnceFlags.set(msg_code);
  }

  // use "++" operator to ensure atomicity
  MessageBase::Statistics::numIncomingExtrnMsgsBufAllocs++;
  MessageBase::Statistics::AliveIncomingExtrnMsgsBufs[msg_code]++;
}

void MessageBase::Statistics::updateDiagnosticsCountersOnBufRelease(MsgCode::Type msg_code) {
  if (MessageBase::Statistics::AliveIncomingExtrnMsgsBufs[msg_code] == 0) {
    LOG_ERROR(GL, "Trying to dec a counter of a msg that hasn't been inserted yet, msg code: " << (msg_code));
    return;
  } else {
    // use "--" operator to ensure atomicity
    MessageBase::Statistics::AliveIncomingExtrnMsgsBufs[msg_code]--;
  }
  // use "++" operator to ensure atomicity
  MessageBase::Statistics::numIncomingExtrnMsgsBufFrees++;
}

void MessageBase::releaseOwnership() {
  if (!owner_) {
    LOG_ERROR(GL, "Trying to release ownership of a MessageBase obj that is already not the buffer owner");
  } else {
    owner_ = false;
  }
}

void MessageBase::shrinkToFit() {
  ConcordAssert(owner_);

  // TODO(GG): need to verify more conditions??

  void *p = (void *)msgBody_;
  p = std::realloc(p, msgSize_);
  // always shrinks allocated size, so no bytes should be 0'd

  msgBody_ = (MessageBase::Header *)p;
  storageSize_ = msgSize_;
}

bool MessageBase::reallocSize(uint32_t size) {
  ConcordAssert(owner_);
  ConcordAssert(size >= msgSize_);

  void *p = (void *)msgBody_;
  p = std::realloc(p, size);

  if (p == nullptr) {
    return false;
  } else {
    msgBody_ = (MessageBase::Header *)p;
    storageSize_ = size;
    msgSize_ = size;
    return true;
  }
}

MessageBase::MessageBase(NodeIdType sender, MsgType type, MsgSize size) : MessageBase(sender, type, 0u, size) {}

MessageBase::MessageBase(NodeIdType sender, MsgType type, SpanContextSize spanContextSize, MsgSize size) {
  ConcordAssert(size > 0);
  size = size + spanContextSize;
  msgBody_ = (MessageBase::Header *)std::malloc(size);
  memset(msgBody_, 0, size);
  storageSize_ = size;
  msgSize_ = size;
  owner_ = true;
  sender_ = sender;
  msgBody_->msgType = type;
  msgBody_->spanContextSize = spanContextSize;

#ifdef DEBUG_MEMORY_MSG
  liveMessagesDebug.insert(this);
#endif
}

MessageBase::MessageBase(NodeIdType sender, MessageBase::Header *body, MsgSize size, bool ownerOfStorage)
    : MessageBase(sender, body, size, ownerOfStorage, false) {}

MessageBase::MessageBase(
    NodeIdType sender, MessageBase::Header *body, MsgSize size, bool ownerOfStorage, bool isIncoming)
    : msgBody_(body),
      msgSize_(size),
      storageSize_(size),
      sender_(sender),
      owner_(ownerOfStorage),
      isIncomingMsg_(isIncoming) {
#ifdef DEBUG_MEMORY_MSG
  liveMessagesDebug.insert(this);
#endif
}

void MessageBase::validate(const ReplicasInfo &) const {
  LOG_DEBUG(GL, "Calling MessageBase::validate on a message of type " << type());
}

bool MessageBase::shouldValidateAsync() const { return false; }
void MessageBase::setMsgSize(MsgSize size) {
  ConcordAssert((msgBody_ != nullptr));
  ConcordAssert(size <= storageSize_);

  msgSize_ = size;
}

MessageBase *MessageBase::cloneObjAndMsg() const {
  ConcordAssert(owner_);
  ConcordAssert(msgSize_ > 0);

  void *msgBody = std::malloc(msgSize_);
  memcpy(msgBody, msgBody_, msgSize_);

  MessageBase *otherMsg = new MessageBase(sender_, (MessageBase::Header *)msgBody, msgSize_, true);

  return otherMsg;
}

void MessageBase::writeObjAndMsgToLocalBuffer(char *buffer, size_t bufferLength, size_t *actualSize) const {
  ConcordAssert(owner_);
  ConcordAssert(msgSize_ > 0);

  const size_t sizeNeeded = sizeof(RawHeaderOfObjAndMsg) + msgSize_;

  ConcordAssert(sizeNeeded <= bufferLength);

  RawHeaderOfObjAndMsg *pHeader = (RawHeaderOfObjAndMsg *)buffer;
  pHeader->magicNum = magicNumOfRawFormat;
  pHeader->msgSize = msgSize_;
  pHeader->sender = sender_;

  char *pRawMsg = buffer + sizeof(RawHeaderOfObjAndMsg);
  memcpy(pRawMsg, msgBody_, msgSize_);

  if (actualSize) *actualSize = sizeNeeded;
}

size_t MessageBase::sizeNeededForObjAndMsgInLocalBuffer() const {
  ConcordAssert(owner_);
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

  void *msgBody = std::malloc(pHeader->msgSize);
  memcpy(msgBody, pBodyInBuffer, pHeader->msgSize);

  MessageBase *msgObj = new MessageBase(pHeader->sender, (MessageBase::Header *)msgBody, pHeader->msgSize, true);

  if (actualSize) *actualSize = (pHeader->msgSize + sizeof(RawHeaderOfObjAndMsg));

  return msgObj;
}

bool MessageBase::equals(const MessageBase &other) const {
  bool equals = (other.msgSize_ == msgSize_ && other.storageSize_ == storageSize_ && other.sender_ == sender_ &&
                 other.owner_ == owner_);
  if (!equals) return false;
  return (memcmp(other.msgBody_, msgBody_, msgSize_) == 0);
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

// Start methods for diagnostics server:
std::string MessageBase::Statistics::getNumBuffsAllocatedForExtrnIncomingMsgs() {
  return (std::string(" Num buffer allocations for external incoming messages: " +
                      std::to_string(MessageBase::Statistics::numIncomingExtrnMsgsBufAllocs)));
}
std::string MessageBase::Statistics::getNumBuffsFreedForExtrnIncomingMsgs() {
  return (std::string(" Num buffer frees for external incoming messages: " +
                      std::to_string(MessageBase::Statistics::numIncomingExtrnMsgsBufFrees)));
}
std::string MessageBase::Statistics::getNumAliveExtrnIncomingMsgsObjsPerType() {
  std::ostringstream oss;
  oss << " Alive extrnal incoming message objects per type: " << std::endl;
  oss << " --------------------------------------- " << std::endl;

  for (int i = 0; i < MsgCode::LastMsgCodeVal; ++i) {
    bool wasMsgEverRecieved;

    {
      std::lock_guard<std::mutex> lock(messagesStatsMonitoringMutex_);
      wasMsgEverRecieved = MessageBase::Statistics::IncomingExtrnMsgReceivedAtLeastOnceFlags.get(i);
    }

    if (wasMsgEverRecieved) {
      oss << " " << static_cast<MsgCode::Type>(i) << ": " << MessageBase::Statistics::AliveIncomingExtrnMsgsBufs[i]
          << std::endl;
    }
  }

  oss << " ** All messages not on the list were never received by replica ** ";
  return oss.str();
}
// End methods for diagnostics server ^

}  // namespace impl
}  // namespace bftEngine
