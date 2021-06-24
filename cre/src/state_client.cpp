// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "state_client.hpp"
#include "concord.cmf.hpp"
#include "bftclient/quorums.h"

namespace cre::state {
State state::PullBasedStateClient::getNextState(uint64_t lastKnownBlockId) {
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::ClientReconfigurationStateRequest creq{id_, lastKnownBlockId};
  rreq.command = creq;
  // (For now) we rely on the TLS keys to validate the client requests in concord-bft's reconfiguration handlers.
  rreq.signature = {};
  bft::client::RequestConfig request_config;
  request_config.reconfiguration = true;
  request_config.correlation_id = "ClientReconfigurationStateRequest-" + std::to_string(lastKnownBlockId);
  bft::client::ReadConfig read_config{request_config, bft::client::LinearizableQuorum{}};
  bft::client::Msg msg;
  concord::messages::serialize(msg, rreq);
  auto rep = bftclient_->send(read_config, std::move(msg));

  // To have a valid quorum, we expect to have the reply in the additional_data of te reply.
  concord::messages::ReconfigurationResponse rres;
  concord::messages::deserialize(rep.matched_data, rres);
  if (!rres.success) {
    throw std::runtime_error{"unable to have a quorum, please check the replicas liveness"};
  }
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}
PullBasedStateClient::PullBasedStateClient(std::shared_ptr<bft::client::Client> client, const uint16_t id)
    : bftclient_{client}, id_{id} {}
}  // namespace cre::state