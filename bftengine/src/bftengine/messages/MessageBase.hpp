// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "util/OpenTracing.hpp"
#include "SysConsts.hpp"
#include "PrimitiveTypes.hpp"
#include "MsgCode.hpp"
#include "ReplicasInfo.hpp"
#include "Bitmap.hpp"

#include <type_traits>
#include <atomic>
#include <mutex>
#include <array>

namespace bftEngine {
namespace impl {

template <typename MessageT>
size_t sizeOfHeader();

class MessageBase {
 public:
#pragma pack(push, 1)
  struct Header {
    MsgType msgType;
    SpanContextSize spanContextSize = 0u;
  };
#pragma pack(pop)

  static_assert(sizeof(Header) == 6, "MessageBase::Header is 6B");

  MessageBase(NodeIdType sender, MsgType type, MsgSize size);
  MessageBase(NodeIdType sender, MsgType type, SpanContextSize spanContextSize, MsgSize size);

  MessageBase(NodeIdType sender, Header *body, MsgSize size, bool ownerOfStorage);

  MessageBase(NodeIdType sender, Header *body, MsgSize size, bool ownerOfStorage, bool isIncoming);

  void releaseOwnership();

  virtual ~MessageBase();

  virtual void validate(const ReplicasInfo &) const;

  // This function will tell whether async validation of a message is enabled or not.
  virtual bool shouldValidateAsync() const;

  bool equals(const MessageBase &other) const;

  static size_t serializeMsg(char *&buf, const MessageBase *msg);

  static size_t serializeMsg(char *&buf, char *msg);

  static MessageBase *deserializeMsg(char *&buf, size_t bufLen, size_t &actualSize);

  MsgSize size() const { return msgSize_; }

  char *body() const { return reinterpret_cast<char *>(msgBody_); }

  NodeIdType senderId() const { return sender_; }

  MsgType type() const { return msgBody_->msgType; }

  SpanContextSize spanContextSize() const { return msgBody_->spanContextSize; }

  bool isIncomingMsg() const { return isIncomingMsg_; }

  template <typename MessageT>
  concordUtils::SpanContext spanContext() const {
    return concordUtils::SpanContext{std::string(body() + sizeOfHeader<MessageT>(), spanContextSize())};
  }

  MessageBase *cloneObjAndMsg() const;

  size_t sizeNeededForObjAndMsgInLocalBuffer() const;
#ifdef DEBUG_MEMORY_MSG
  static void printLiveMessages();
#endif

  struct Statistics {
    // Static methods for reporting messages' status to diagnostics server:
    static std::string getNumBuffsAllocatedForExtrnIncomingMsgs();
    static std::string getNumBuffsFreedForExtrnIncomingMsgs();
    static std::string getNumAliveExtrnIncomingMsgsObjsPerType();
    // Static methods called when incoming messages' buffer are allocated or released - for monitoring.
    static void updateDiagnosticsCountersOnBufRelease(MsgCode::Type msg_code);
    static void updateDiagnosticsCountersOnBufAlloc(MsgCode::Type msg_code);

   private:
    // Static data structures gathering statistics for diagnostics server:

    // although MsgCode::LastMsgCodeVal is much higher than the actual number of real message types,
    // we allocate an array of size MsgCode::LastMsgCodeVal for simplicity and avoiding using locks,
    // as this structures are a shared resource between threads.
    static std::array<std::atomic<size_t>, MsgCode::LastMsgCodeVal> AliveIncomingExtrnMsgsBufs;
    static Bitmap IncomingExtrnMsgReceivedAtLeastOnceFlags;
    // the mutex is only locked once per message type - on the first time it is received
    static std::mutex messagesStatsMonitoringMutex_;
    static std::atomic<size_t> numIncomingExtrnMsgsBufAllocs;
    static std::atomic<size_t> numIncomingExtrnMsgsBufFrees;
  };

 protected:
  void writeObjAndMsgToLocalBuffer(char *buffer, size_t bufferLength, size_t *actualSize) const;
  static MessageBase *createObjAndMsgFromLocalBuffer(char *buffer, size_t bufferLength, size_t *actualSize);
  void shrinkToFit();

  bool reallocSize(uint32_t size);

  void setMsgSize(MsgSize size);

  MsgSize internalStorageSize() const { return storageSize_; }

 protected:
  Header *msgBody_ = nullptr;
  MsgSize msgSize_ = 0;
  MsgSize storageSize_ = 0;
  // This might be the direct sender, but not the originator
  NodeIdType sender_;

  // true IFF this instance is not responsible for de-allocating the body:
  bool owner_ = true;

  static constexpr uint32_t magicNumOfRawFormat = 0x5555897BU;

  template <typename MessageT>
  friend size_t sizeOfHeader();

  template <typename MessageT>
  friend MsgSize maxMessageSize();

  template <typename MessageT>
  friend MsgSize maxMessageSizeInLocalBuffer();

  static constexpr uint64_t SPAN_CONTEXT_MAX_SIZE{1024};
  static constexpr uint32_t MAX_BATCH_SIZE{1024};

  bool isIncomingMsg_ = false;

#pragma pack(push, 1)
  struct RawHeaderOfObjAndMsg {
    uint32_t magicNum;
    MsgSize msgSize;
    NodeIdType sender;
    // TODO(GG): consider to add checksum
  };
#pragma pack(pop)
};

// Every subclass of MessageBase has to use this macro to generate a constructor for creation from MessageBase.
// During de-serialization we first place the raw char array that we receive with the actual message into the msgBody_
// of a MessageBase object to be able to get the msgType. Later during dispatch we need to create an object of the
// actual message type from the MessageBase object holding the msgBody_ of the actual message.
#define BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(TrueTypeName)               \
  TrueTypeName(MessageBase *msgBase)                                          \
      : MessageBase(msgBase->senderId(),                                      \
                    reinterpret_cast<MessageBase::Header *>(msgBase->body()), \
                    msgBase->size(),                                          \
                    true,                                                     \
                    msgBase->isIncomingMsg()) {                               \
    msgBase->releaseOwnership();                                              \
  }

template <typename MessageT>
size_t sizeOfHeader() {
  static_assert(std::is_convertible<MessageT *, MessageBase *>::value);
  return sizeof(typename MessageT::Header);
}

template <typename MessageT>
MsgSize maxMessageSize() {
  return sizeOfHeader<MessageT>() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

template <typename MessageT>
MsgSize maxMessageSizeInLocalBuffer() {
  return maxMessageSize<MessageT>() + sizeof(MessageBase::RawHeaderOfObjAndMsg);
}

}  // namespace impl
}  // namespace bftEngine
