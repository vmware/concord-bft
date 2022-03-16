// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "UTTCommandsHandler.hpp"
#include "assertUtils.hpp"
#include "sliver.hpp"
#include "kv_types.hpp"
#include "block_metadata.hpp"
#include "sha_hash.hpp"
#include <unistd.h>
#include <algorithm>
#include <variant>
#include "ReplicaConfig.hpp"
#include "kvbc_key_types.hpp"

#include "utt_messages.cmf.hpp"

using namespace bftEngine;

void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue &requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string &batchCid,
                                 concordUtils::SpanWrapper &parent_span) {
  LOG_INFO(logger_, "UTTCommandsHandler: TODO execute");
}