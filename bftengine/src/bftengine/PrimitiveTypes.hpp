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

#include <stdint.h>
#include <inttypes.h>
#include <string>

#ifdef max  // TODO(GG): remove
#undef max
#endif

#ifdef min  // TODO(GG): remove
#undef min
#endif

namespace bftEngine {
namespace impl {

typedef int64_t SeqNum;
typedef int64_t ViewNum;
typedef uint64_t ReqId;  // TODO(GG): more meaningful name ... ???

typedef uint16_t PrincipalId;  // ReplicaId or NodeIdType
typedef uint16_t ReplicaId;
typedef uint16_t NodeIdType;  // TODO(GG): change name

typedef uint32_t MsgSize;
typedef uint16_t MsgType;
typedef uint32_t SpanContextSize;

enum class CommitPath { NA = -1, OPTIMISTIC_FAST = 0, FAST_WITH_THRESHOLD = 1, SLOW = 2 };
enum class KeyFormat { HexaDecimalStrippedFormat, PemFormat };

std::string CommitPathToStr(CommitPath path);
std::string CommitPathToMDCString(CommitPath path);
}  // namespace impl
}  // namespace bftEngine
