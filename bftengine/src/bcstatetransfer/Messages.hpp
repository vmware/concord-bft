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

#pragma once

#include <stdint.h>
#include <limits>

#include "IStateTransfer.hpp"
#include "Logger.hpp"
#include "hex_tools.h"

namespace bftEngine {
namespace bcst {
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
  CheckpointSummaryMsg() = delete;

  static CheckpointSummaryMsg* create(size_t rvbDataSize) {
    size_t totalByteSize = sizeof(CheckpointSummaryMsg) + rvbDataSize - 1;
    CheckpointSummaryMsg* msg{reinterpret_cast<CheckpointSummaryMsg*>(new char[totalByteSize])};
    memset(msg, 0, totalByteSize);
    msg->type = MsgType::CheckpointsSummary;
    msg->rvbDataSize = rvbDataSize;
    return msg;
  }

  static CheckpointSummaryMsg* create(const CheckpointSummaryMsg* rMsg) {
    auto msg = create(rMsg->rvbDataSize);
    msg->checkpointNum = rMsg->checkpointNum;
    msg->maxBlockId = rMsg->maxBlockId;
    msg->digestOfMaxBlockId = rMsg->digestOfMaxBlockId;
    msg->digestOfResPagesDescriptor = rMsg->digestOfResPagesDescriptor;
    msg->requestMsgSeqNum = rMsg->requestMsgSeqNum;
    memcpy(msg->data, rMsg->data, rMsg->rvbDataSize);
    return msg;
  }

  static void free(void* context, const CheckpointSummaryMsg* msg) {
    IReplicaForStateTransfer* rep = reinterpret_cast<IReplicaForStateTransfer*>(context);
    rep->freeStateTransferMsg(const_cast<char*>(reinterpret_cast<const char*>(msg)));
  }

  size_t size() const { return sizeof(CheckpointSummaryMsg) + rvbDataSize - 1; }
  size_t sizeofRvbData() const { return rvbDataSize; }

  uint64_t checkpointNum;
  uint64_t maxBlockId;
  Digest digestOfMaxBlockId;
  Digest digestOfResPagesDescriptor;
  uint64_t requestMsgSeqNum;

 private:
  uint32_t rvbDataSize;

 public:
  char data[1];

  static void logOnMismatch(const CheckpointSummaryMsg* a,
                            const CheckpointSummaryMsg* b,
                            uint16_t a_id = std::numeric_limits<uint16_t>::max(),
                            uint16_t b_id = std::numeric_limits<uint16_t>::max()) {
    auto logger = logging::getLogger("state-transfer");
    std::ostringstream oss;
    oss << "Mismatched Checkpoints for checkpointNum=" << a->checkpointNum << std::endl;

    if (a_id != std::numeric_limits<uint16_t>::max()) {
      oss << "Replica=" << a_id;
    }
    oss << " maxBlockId=" << a->maxBlockId << " digestOfMaxBlockId=" << a->digestOfMaxBlockId.toString()
        << " digestOfResPagesDescriptor=" << a->digestOfResPagesDescriptor.toString()
        << " requestMsgSeqNum=" << a->requestMsgSeqNum << " rvbDataSize=" << a->rvbDataSize << std::endl;

    if (b_id != std::numeric_limits<uint16_t>::max()) {
      oss << "Replica=" << b_id;
    }
    oss << " maxBlockId=" << b->maxBlockId << " digestOfMaxBlockId=" << b->digestOfMaxBlockId.toString()
        << " digestOfResPagesDescriptor=" << b->digestOfResPagesDescriptor.toString()
        << " requestMsgSeqNum=" << b->requestMsgSeqNum << " requestMsgSeqNum=" << b->rvbDataSize << std::endl;
    LOG_WARN(logger, oss.str());
    if (a->rvbDataSize > 0) {
      concordUtils::HexPrintBuffer adata{a->data, a->rvbDataSize};
      LOG_TRACE(logger, KVLOG(adata));
    }
    if (b->rvbDataSize > 0) {
      concordUtils::HexPrintBuffer bdata{b->data, b->rvbDataSize};
      LOG_TRACE(logger, KVLOG(bdata));
    }
  }

  static bool equivalent(const CheckpointSummaryMsg* a, const CheckpointSummaryMsg* b) {
    static_assert((sizeof(CheckpointSummaryMsg) - sizeof(requestMsgSeqNum) == 87),
                  "Should newly added field be compared below?");
    bool cmp1 =
        ((a->maxBlockId == b->maxBlockId) && (a->checkpointNum == b->checkpointNum) &&
         (a->digestOfMaxBlockId == b->digestOfMaxBlockId) &&
         (a->digestOfResPagesDescriptor == b->digestOfResPagesDescriptor) && (a->rvbDataSize == b->rvbDataSize));
    bool cmp2{true};
    if (cmp1 && (a->rvbDataSize > 0)) {
      cmp2 = (0 == memcmp(a->data, b->data, a->rvbDataSize));
    }
    bool equal = cmp1 && cmp2;
    if (!equal) {
      logOnMismatch(a, b);
    }
    return equal;
  }

  static bool equivalent(const CheckpointSummaryMsg* a, uint16_t a_id, const CheckpointSummaryMsg* b, uint16_t b_id) {
    static_assert((sizeof(CheckpointSummaryMsg) - sizeof(requestMsgSeqNum) == 87),
                  "Should newly added field be compared below?");
    if ((a->maxBlockId != b->maxBlockId) || (a->checkpointNum != b->checkpointNum) ||
        (a->digestOfMaxBlockId != b->digestOfMaxBlockId) ||
        (a->digestOfResPagesDescriptor != b->digestOfResPagesDescriptor) || (a->rvbDataSize != b->rvbDataSize) ||
        (0 != memcmp(a->data, b->data, a->rvbDataSize))) {
      logOnMismatch(a, b, a_id, b_id);
      return false;
    }
    return true;
  }
};

struct FetchBlocksMsg : public BCStateTranBaseMsg {
  FetchBlocksMsg() {
    memset(this, 0, sizeof(FetchBlocksMsg));
    type = MsgType::FetchBlocks;
  }

  uint64_t msgSeqNum;
  uint64_t minBlockId;
  uint64_t maxBlockId;
  uint16_t lastKnownChunkInLastRequiredBlock;
  uint64_t rvbGroupId;
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
  uint8_t lastInBatch;
  uint32_t rvbDigestsSize;  // if non-zero, size in bytes  which is dedicated to RVB
                            // digests from the total of dataSize (rvbDigestsSize < dataSize)
  char data[1];             // MSB[raw block of size dataSize-rvbDigestsSize|RVB DIGESTS of size rvbDigestsSize]LSB

  uint32_t size() const { return sizeof(ItemDataMsg) - 1 + dataSize; }
};

#pragma pack(pop)

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
