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

#include <locale>
#include <string>
#include "MessageBase.hpp"
#include "OpenTracing.hpp"

namespace bftEngine {
namespace impl {

class StartSlowCommitMsg : public MessageBase {
 public:
  StartSlowCommitMsg(ReplicaId senderId,
                     ViewNum v,
                     SeqNum s,
                     EpochNum e,
                     const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(StartSlowCommitMsg)

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  void validate(const ReplicasInfo&) const override;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    ViewNum viewNum;
    SeqNum seqNum;
    EpochNum epochNum;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + 8), "Header is 30B");

  Header* b() const { return (Header*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
