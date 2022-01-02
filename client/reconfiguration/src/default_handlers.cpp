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

#include "client/reconfiguration/default_handlers.hpp"
#include "concord.cmf.hpp"

#include <variant>
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

namespace concord::client::reconfiguration::handlers {

template <typename T>
bool validateInputState(const State& state) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::holds_alternative<T>(crep.response);
}

template <typename T>
T getCmdFromInputState(const State& state) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::get<T>(crep.response);
}
bool ReplicaMainKeyPublicationHandler::validate(const State& state) const {
  return validateInputState<concord::messages::ReplicaMainKeyUpdate>(state);
}
bool ReplicaMainKeyPublicationHandler::execute(const State& state, WriteState&) {
  auto cmd = getCmdFromInputState<concord::messages::ReplicaMainKeyUpdate>(state);
  fs::path path = fs::path(output_dir_) / std::to_string(cmd.sender_id);
  auto curr_key = file_handler_.decryptFile((path / "pub_key").string()).value_or("");
  if (curr_key != cmd.key) {
    if (!fs::exists(path)) fs::create_directories(path);
    file_handler_.encryptFile((path / "pub_key").string(), cmd.key);
  }
  latest_known_update_ = state.blockid;
  return true;
}
}  // namespace concord::client::reconfiguration::handlers