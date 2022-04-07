// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>

#define PRE_PROCESS_REQUEST_MSG_TYPE (500)
#define REQUEST_MSG_TYPE (700)
#define BATCH_REQUEST_MSG_TYPE (750)
#define REPLY_MSG_TYPE (800)

namespace bftEngine {

#pragma pack(push, 1)

struct ClientBatchRequestMsgHeader {
  uint16_t msgType;  // always == BATCH_REQUEST_MSG_TYPE
  uint32_t cidSize;
  uint16_t clientId;
  uint32_t numOfMessagesInBatch;
  uint32_t dataSize;
};

struct ClientRequestMsgHeader {
  uint16_t msgType;  // always == REQUEST_MSG_TYPE
  uint32_t spanContextSize = 0u;
  uint16_t idOfClientProxy = 0;  // TODO - rename - now used mostly as id of external client
  uint64_t flags = 0;            // bit 0 == isReadOnly, bit 1 = preProcess ...
  uint32_t result = 1;           // UNKNOWN
  uint64_t reqSeqNum = 0;
  uint32_t requestLength = 0;
  uint64_t timeoutMilli = 0;
  uint32_t cidLength = 0;
  uint32_t participantIdLength = 0;
  uint16_t reqSignatureLength = 0;
  uint32_t extraDataLength = 0;

  // followed by the request (security information, such as signatures, should be part of the request)

  // TODO(GG): idOfClientProxy is not needed here
  // TODO(GG): add information about "suggested repliers"
};

struct ClientReplyMsgHeader {
  uint16_t msgType;  // always == REPLY_MSG_TYPE
  uint32_t spanContextSize = 0u;
  uint16_t currentPrimaryId;
  uint64_t reqSeqNum;
  uint32_t result = 0;  // Request execution result; SUCCESS
  // Reply length is the total length of the reply, including any replica specific info.
  uint32_t replyLength;
  // This is the size of the replica specific information. If it is 0, there is no replica specific
  // information. The offset of the replica specific information from the start of the reply message
  // is `replyLength - replicaSpecificInfoLength`.
  uint32_t replicaSpecificInfoLength = 0;
};

#pragma pack(pop)

}  // namespace bftEngine
