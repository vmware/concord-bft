// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

namespace bftEngine {

// Definitions shared between client and replica

enum class OperationResult : uint32_t {
  SUCCESS,
  UNKNOWN,
  INVALID_REQUEST,
  NOT_READY,
  TIMEOUT,
  EXEC_DATA_TOO_LARGE,
  EXEC_DATA_EMPTY,
  CONFLICT_DETECTED,
  OVERLOADED,
  EXEC_ENGINE_REJECT_ERROR,
  INTERNAL_ERROR
};

}  // namespace bftEngine
