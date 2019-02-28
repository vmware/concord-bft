// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef BFTENGINE_SRC_BCSTATETRANSFER_MESSAGES_HPP_
#define BFTENGINE_SRC_BCSTATETRANSFER_MESSAGES_HPP_

#include <stdint.h>

#include "STDigest.hpp"
#include "IStateTransfer.hpp"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

#pragma pack(push, 1)

class MsgType {
 public:
  enum : uint16_t {
    None = 0,
    AskForCheckpointSummaries,
    CheckpointsSummary,
    FetchBlocks,
    FetchResPages,
    RejectFetching,
    ItemData
  };
};

struct BCStateTranBaseMsg {
  uint16_t type;
};

struct AskForCheckpointSummariesMsg : public BCStateTranBaseMsg {
  AskForCheckpointSummariesMsg() {
    memset(this, 0, sizeof(AskForCheckpointSummariesMsg));
    type = MsgType::AskForCheckpointSummaries;
  }

  uint64_t msgSeqNum;
  uint64_t minRelevantCheckpointNum;
};

struct CheckpointSummaryMsg : public BCStateTranBaseMsg {
  CheckpointSummaryMsg() {
    memset(this, 0, sizeof(CheckpointSummaryMsg));
    type = MsgType::CheckpointsSummary;
  }

  uint64_t checkpointNum;
  uint64_t lastBlock;
  STDigest digestOfLastBlock;
  STDigest digestOfResPagesDescriptor;
  uint64_t requestMsgSeqNum;

  static bool equivalent(const CheckpointSummaryMsg* a,
                         const CheckpointSummaryMsg* b) {
    return (memcmp(a, b, sizeof(CheckpointSummaryMsg)) == 0);
  }

  static void free(void* context, const CheckpointSummaryMsg* a) {
    IReplicaForStateTransfer* w =
        reinterpret_cast<IReplicaForStateTransfer*>(context);

    w->freeStateTransferMsg(
        const_cast<char*>(reinterpret_cast<const char*>(a)));
  }
};

struct FetchBlocksMsg : public BCStateTranBaseMsg {
  FetchBlocksMsg() {
    memset(this, 0, sizeof(FetchBlocksMsg));
    type = MsgType::FetchBlocks;
  }

  uint64_t msgSeqNum;
  uint64_t firstRequiredBlock;
  uint64_t lastRequiredBlock;
  uint16_t lastKnownChunkInLastRequiredBlock;
};

struct FetchResPagesMsg : public BCStateTranBaseMsg {
  FetchResPagesMsg() {
    memset(this, 0, sizeof(FetchResPagesMsg));
    type = MsgType::FetchResPages;
  }

  uint64_t msgSeqNum;
  uint64_t lastCheckpointKnownToRequester;
  uint64_t requiredCheckpointNum;
  uint16_t lastKnownChunk;
};

struct RejectFetchingMsg : public BCStateTranBaseMsg {
  RejectFetchingMsg() {
    memset(this, 0, sizeof(RejectFetchingMsg));
    type = MsgType::RejectFetching;
  }

  uint64_t requestMsgSeqNum;
};

struct ItemDataMsg : public BCStateTranBaseMsg {
  static ItemDataMsg* alloc(uint32_t dataSize) {
    size_t s = sizeof(ItemDataMsg) - 1 + dataSize;
    char* buff = reinterpret_cast<char*>(std::malloc(s));
    memset(buff, 0, s);
    ItemDataMsg* retVal = reinterpret_cast<ItemDataMsg*>(buff);
    retVal->type = MsgType::ItemData;
    retVal->dataSize = dataSize;

    return retVal;
  }

  static void free(ItemDataMsg* i) {
    void* buff = i;
    std::free(buff);
  }

  uint64_t requestMsgSeqNum;

  uint64_t blockNumber;

  uint16_t totalNumberOfChunksInBlock;

  uint16_t chunkNumber;

  uint32_t dataSize;
  char data[1];

  uint32_t size() const {
    return sizeof(ItemDataMsg) - 1 + dataSize;
    ;
  }
};

#pragma pack(pop)

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine

#endif  // BFTENGINE_SRC_BCSTATETRANSFER_MESSAGES_HPP_
