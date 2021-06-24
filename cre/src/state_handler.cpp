// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "state_handler.hpp"
#include "concord.cmf.hpp"
#include <variant>
namespace cre::state {
bool sendUpdate(
    bft::client::Client& bftclient, uint64_t id, uint64_t version, const std::string& data, const std::string& cid) {
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::ClientReconfigurationStateUpdate creq{id, version, data};
  rreq.command = creq;
  // (For now) we rely on the TLS keys to validate the client requests in concord-bft's reconfiguration handlers.
  std::vector<uint8_t> creq_vec;
  concord::messages::deserialize(creq_vec, creq);
  auto sig = bftclient.signMessage(creq_vec);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  bft::client::RequestConfig request_config;
  request_config.reconfiguration = true;
  request_config.correlation_id = cid;
  bft::client::WriteConfig write_config{request_config, bft::client::LinearizableQuorum{}};
  bft::client::Msg msg;
  concord::messages::serialize(msg, rreq);
  auto rep = bftclient.send(write_config, std::move(msg));

  // To have a valid quorum, we expect to have the reply in the additional_data of te reply.
  concord::messages::ReconfigurationResponse rres;
  concord::messages::deserialize(rep.matched_data, rres);
  return rres.success;
}
template <typename T>
bool hasValue(const State& state) {
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::holds_alternative<T>(crep.response);
}

template <typename T>
T getData(const State& state) {
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::get<T>(crep.response);
}
bool state::WedgeStateHandler::validate(const State& state) { return hasValue<concord::messages::WedgeCommand>(state); }
bool WedgeStateHandler::execute(const State& state) {
  concord::messages::WedgeCommand cmd = getData<concord::messages::WedgeCommand>(state);
  LOG_INFO(getLogger(), "got a wedge command " << KVLOG(cmd.noop, state.block));
  return true;
}
bool WedgeUpdateChainHandler::validate(const State& state) { return hasValue<concord::messages::WedgeCommand>(state); }
bool WedgeUpdateChainHandler::execute(const State& state) {
  return sendUpdate(
      *bftclient_, id_, state.block, "got wedge message", "WedgeUpdateChainHandler-" + std::to_string(state.block));
}

}  // namespace cre::state