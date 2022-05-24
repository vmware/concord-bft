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

#pragma once

#include <cstdint>

#include "PrimitiveTypes.hpp"
#include "assertUtils.hpp"
#include "Digest.hpp"
#include "MessageBase.hpp"
#include "ReplicaConfig.hpp"

using concord::util::digest::Digest;

namespace bftEngine {
namespace impl {
class RequestsIterator;

class PrePrepareMsg : public MessageBase {
 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    ViewNum viewNum;
    SeqNum seqNum;
    EpochNum epochNum;
    uint16_t flags;
    uint64_t batchCidLength;
    int64_t time;
    Digest digestOfRequests;

    uint16_t numberOfRequests;
    uint32_t endLocationOfLastRequest;

    // bits in flags
    // bit 0: 0=null , 1=non-null
    // bit 1: 0=not ready , 1=ready
    // bits 2-3: represent the first commit path that should be tried (00 = OPTIMISTIC_FAST, 01 = FAST_WITH_THRESHOLD,
    // 10 = SLOW) bits 4-15: zero
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + 8 + 2 + 8 + DIGEST_SIZE + 2 + 4 + 8), "Header is 86B");

  static const size_t prePrepareHeaderPrefix =
      sizeof(Header) - sizeof(Header::numberOfRequests) - sizeof(Header::endLocationOfLastRequest);

 public:
  // static

  static const Digest& digestOfNullPrePrepareMsg();

  void validate(const ReplicasInfo&) const override;

  bool shouldValidateAsync() const override { return true; }

  // size - total size of all requests that will be added
  PrePrepareMsg(ReplicaId sender, ViewNum v, SeqNum s, CommitPath firstPath, size_t size);

  PrePrepareMsg(ReplicaId sender,
                ViewNum v,
                SeqNum s,
                CommitPath firstPath,
                const concordUtils::SpanContext& spanContext,
                size_t size);

  PrePrepareMsg(ReplicaId sender,
                ViewNum v,
                SeqNum s,
                CommitPath firstPath,
                const concordUtils::SpanContext& spanContext,
                const std::string& batchCid,
                size_t size);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PrePrepareMsg)

  uint32_t remainingSizeForRequests() const;

  uint32_t requestsSize() const;

  void addRequest(const char* pRequest, uint32_t requestSize);

  void finishAddingRequests();

  // getter methods

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }
  void setSeqNumber(SeqNum s) { b()->seqNum = s; }
  int64_t getTime() const { return b()->time; }
  void setTime(int64_t time) { b()->time = time; }
  std::string getCid() const;
  void setCid(SeqNum s);

  /// This is actually the final commit path of the request
  CommitPath firstPath() const;

  bool isNull() const { return ((b()->flags & 0x1) == 0); }

  Digest& digestOfRequests() const { return b()->digestOfRequests; }

  uint16_t numberOfRequests() const { return b()->numberOfRequests; }

  // update view and first path

  void updateView(ViewNum v, CommitPath firstPath = CommitPath::SLOW);
  const std::string getClientCorrelationIdForMsg(int index) const;
  const std::string getBatchCorrelationIdAsString() const;

 protected:
  static int16_t computeFlagsForPrePrepareMsg(bool isNull, bool isReady, CommitPath firstPath);

  void calculateDigestOfRequests(Digest& d) const;

  bool isReady() const { return (((b()->flags >> 1) & 0x1) == 1); }

  bool checkRequests() const;

  Header* b() const { return (Header*)msgBody_; }

  uint32_t payloadShift() const;
  friend class RequestsIterator;
};

class RequestsIterator {
 public:
  RequestsIterator(const PrePrepareMsg* const m);

  void restart();

  bool getCurrent(char*& pRequest) const;

  bool end() const;

  void gotoNext();

  bool getAndGoToNext(char*& pRequest);

 protected:
  const PrePrepareMsg* const msg;
  uint32_t currLoc;
};

template <>
inline MsgSize maxMessageSize<PrePrepareMsg>() {
  return ReplicaConfig::instance().getmaxExternalMessageSize();
}

}  // namespace impl
}  // namespace bftEngine
