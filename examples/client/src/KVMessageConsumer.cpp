// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "KVMessageConsumer.hpp"
#include "kv_replica_msgs.cmf.hpp"

using namespace concord::osexample;

void KVMessageConsumer::consumeMessage() { LOG_INFO(getLogger(), "consumeMessage() "); }

void KVMessageConsumer::deserialize(const bft::client::Reply& rep, bool isReadOnly) {
  try {
    if (isReadOnly) {
      kv::messages::KVReadReply rres;
      kv::messages::deserialize(rep.matched_data, rres);
    } else {
      kv::messages::KVWriteReply rres;
      kv::messages::deserialize(rep.matched_data, rres);
    }
  } catch (std::exception& ex) {
    LOG_WARN(getLogger(), "error while deserializing bft client response" << ex.what());
  }
}
