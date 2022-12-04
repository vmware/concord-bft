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
  BCStateTranBaseMsg(uint16_t type) : type(type) {}
  uint16_t type;
  // This struct and its derived structs are used to de/serialize message buffers sent over communication channels.
  // Since virtual methods modify the memory layout of a struct, there cannot be any for both this base struct and its
  // inheritors.
};

// VariableSizeMsg is useful for structs with a flexible array member
template <typename T>
class VariableSizeMsg {
 public:
  VariableSizeMsg(size_t dataSize) : buffer_(new concord::Byte[calcMsgSize(dataSize)]()) {}

  T* operator->() const { return reinterpret_cast<T*>(buffer_.get()); }

  char* getSerializedMsg() const { return reinterpret_cast<char*>(buffer_.get()); }

  static size_t calcMsgSize(size_t dataSize) { return sizeof(T) - 1 + dataSize; }

 private:
  std::unique_ptr<concord::Byte[]> buffer_;
};

struct AskForCheckpointSummariesMsg : public BCStateTranBaseMsg {
  AskForCheckpointSummariesMsg()
      : BCStateTranBaseMsg{MsgType::AskForCheckpointSummaries}, msgSeqNum{}, minRelevantCheckpointNum{} {}

  uint64_t msgSeqNum;
  uint64_t minRelevantCheckpointNum;
};

struct CheckpointSummaryMsg : public BCStateTranBaseMsg {
  CheckpointSummaryMsg() = delete;

  static VariableSizeMsg<CheckpointSummaryMsg> alloc(size_t rvbDataSize) {
    VariableSizeMsg<CheckpointSummaryMsg> msg{rvbDataSize};
    msg->type = MsgType::CheckpointsSummary;
    msg->rvbDataSize = rvbDataSize;
    return msg;
  }

  static void free(void* context, const CheckpointSummaryMsg* msg) {
    IReplicaForStateTransfer* rep = reinterpret_cast<IReplicaForStateTransfer*>(context);
    rep->freeStateTransferMsg(const_cast<char*>(reinterpret_cast<const char*>(msg)));
  }

  size_t size() const { return VariableSizeMsg<CheckpointSummaryMsg>::calcMsgSize(rvbDataSize); }
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
  FetchBlocksMsg()
      : BCStateTranBaseMsg{MsgType::FetchBlocks},
        msgSeqNum{},
        minBlockId{},
        maxBlockId{},
        maxBlockIdInCycle{},
        rvbGroupId{},
        lastKnownChunkInLastRequiredBlock{} {}

  uint64_t msgSeqNum;
  uint64_t minBlockId;
  uint64_t maxBlockId;
  uint64_t maxBlockIdInCycle;
  uint64_t rvbGroupId;
  uint16_t lastKnownChunkInLastRequiredBlock;
};

struct FetchResPagesMsg : public BCStateTranBaseMsg {
  FetchResPagesMsg()
      : BCStateTranBaseMsg{MsgType::FetchResPages},
        msgSeqNum{},
        lastCheckpointKnownToRequester{},
        requiredCheckpointNum{},
        lastKnownChunk{} {}

  uint64_t msgSeqNum;
  uint64_t lastCheckpointKnownToRequester;
  uint64_t requiredCheckpointNum;
  uint16_t lastKnownChunk;
};

struct RejectFetchingMsg : public BCStateTranBaseMsg {
  class Reason {
   public:
    enum : uint16_t {
      FIRST = 0,  // should not be used and must be first!

      RES_PAGE_NOT_FOUND = 1,
      IN_STATE_TRANSFER = 2,
      BLOCK_RANGE_NOT_FOUND = 3,
      IN_ACTIVE_SESSION = 4,
      INVALID_NUMBER_OF_BLOCKS_REQUESTED = 5,
      BLOCK_NOT_FOUND_IN_STORAGE = 6,
      DIGESTS_FOR_RVBGROUP_NOT_FOUND = 7,

      LAST,  // should not be used and must be last!
    };
  };
  RejectFetchingMsg() = delete;
  RejectFetchingMsg(uint16_t rejCode, uint64_t reqMsgSeqNum)
      : BCStateTranBaseMsg{MsgType::RejectFetching}, requestMsgSeqNum{reqMsgSeqNum}, rejectionCode{rejCode} {}

  uint64_t requestMsgSeqNum;
  uint16_t rejectionCode;

  static inline std::map<uint16_t, const char*> reasonMessages = {
      {Reason::RES_PAGE_NOT_FOUND, "Reserved page not found"},
      {Reason::IN_STATE_TRANSFER, "In state transfer"},
      {Reason::BLOCK_RANGE_NOT_FOUND, "Block range not found"},
      {Reason::IN_ACTIVE_SESSION, "In active session"},
      {Reason::INVALID_NUMBER_OF_BLOCKS_REQUESTED, "Invalid number of blocks requested"},
      {Reason::BLOCK_NOT_FOUND_IN_STORAGE, "Block not found in storage"},
      {Reason::DIGESTS_FOR_RVBGROUP_NOT_FOUND, "Digests for RVB group not found"}};
};

struct ItemDataMsg : public BCStateTranBaseMsg {
  ItemDataMsg() = delete;

  static VariableSizeMsg<ItemDataMsg> alloc(size_t dataSize) {
    VariableSizeMsg<ItemDataMsg> msg{dataSize};
    msg->type = MsgType::ItemData;
    msg->dataSize = dataSize;
    return msg;
  }

  uint64_t requestMsgSeqNum;
  uint64_t blockNumber;
  uint16_t totalNumberOfChunksInBlock;
  uint16_t chunkNumber;
  uint8_t lastInBatch;
  uint32_t rvbDigestsSize;  // if non-zero, size in bytes  which is dedicated to RVB
                            // digests from the total of dataSize (rvbDigestsSize < dataSize)
 private:
  uint32_t dataSize;

 public:
  char data[1];  // MSB[raw block of size dataSize-rvbDigestsSize|RVB DIGESTS of size rvbDigestsSize]LSB

  uint32_t size() const { return VariableSizeMsg<ItemDataMsg>::calcMsgSize(dataSize); }
  uint32_t getDataSize() const { return dataSize; }
};

#pragma pack(pop)

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
