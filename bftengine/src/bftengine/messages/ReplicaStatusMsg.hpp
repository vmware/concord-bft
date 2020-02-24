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
#include "MessageBase.hpp"

namespace bftEngine {
namespace impl {
class ReplicaStatusMsg : public MessageBase {
 public:
  ReplicaStatusMsg(ReplicaId senderId,
                   ViewNum viewNumber,
                   SeqNum lastStableSeqNum,
                   SeqNum lastExecutedSeqNum,
                   bool viewIsActive,
                   bool hasNewChangeMsg,
                   bool listOfPrePrepareMsgsInActiveWindow,
                   bool listOfMissingViewChangeMsgForViewChange,
                   bool listOfMissingPrePrepareMsgForViewChange);

  ViewNum getViewNumber() const;

  SeqNum getLastStableSeqNum() const;

  SeqNum getLastExecutedSeqNum() const;

  bool currentViewIsActive() const;

  bool currentViewHasNewViewMessage() const;

  bool hasListOfPrePrepareMsgsInActiveWindow() const;

  bool hasListOfMissingViewChangeMsgForViewChange() const;

  bool hasListOfMissingPrePrepareMsgForViewChange() const;

  bool isPrePrepareInActiveWindow(SeqNum seqNum) const;

  bool isMissingViewChangeMsgForViewChange(ReplicaId replicaId) const;

  bool isMissingPrePrepareMsgForViewChange(SeqNum seqNum) const;

  void setPrePrepareInActiveWindow(SeqNum seqNum) const;

  void setMissingViewChangeMsgForViewChange(ReplicaId replicaId);

  void setMissingPrePrepareMsgForViewChange(SeqNum seqNum);

  void validate(const ReplicasInfo&) const override;

 protected:
#pragma pack(push, 1)
  struct ReplicaStatusMsgHeader {
    MessageBase::Header header;
    ViewNum viewNumber;
    SeqNum lastStableSeqNum;
    SeqNum lastExecutedSeqNum;

    // flags:
    // bit 0 == viewIsActive
    // bit 1 == hasNewChangeMsg
    // bit 2 == has list of PrePrepareMsg
    // bit 3 == has list of missing ViewChangeMsg (for view change)
    // bit 4 == has list of missing PrePrepareMsg (for view change)
    uint8_t flags;
  };
#pragma pack(pop)
  static_assert(sizeof(ReplicaStatusMsgHeader) == (2 + 8 + 8 + 8 + 1), "ReplicaStatusMsgHeader is 27B");

  static MsgSize calcSizeOfReplicaStatusMsg(bool listOfPrePrepareMsgsInActiveWindow,
                                            bool listOfMissingViewChangeMsgForViewChange,
                                            bool listOfMissingPrePrepareMsgForViewChange);

  ReplicaStatusMsgHeader* b() const { return (ReplicaStatusMsgHeader*)msgBody_; }
};
}  // namespace impl
}  // namespace bftEngine
