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
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "cre_interfaces.hpp"
#include "bftclient/bft_client.h"
#include "bftclient/seq_num_generator.h"
#include "concord.cmf.hpp"

namespace concord::client::reconfiguration {

class PollBasedStateClient : public IStateClient {
 public:
  PollBasedStateClient(bft::client::Client* client,
                       uint64_t interval_timeout_ms,
                       uint64_t last_known_block,
                       const uint16_t id_);
  State getNextState() const override;
  bool updateState(const WriteState& state) override;
  ~PollBasedStateClient();
  void start() override;
  void stop() override;
  std::vector<State> getStateUpdate(bool& succ) const;

 private:
  concord::messages::ReconfigurationResponse sendReconfigurationRequest(concord::messages::ReconfigurationRequest& rreq,
                                                                        const std::string& cid,
                                                                        uint64_t sn,
                                                                        bool read_request) const;
  logging::Logger getLogger() const {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.PollBasedStateClient"));
    return logger_;
  }

  std::unique_ptr<bft::client::Client> bftclient_;
  uint16_t id_;
  uint64_t interval_timeout_ms_;
  uint64_t last_known_block_;
  mutable std::queue<State> updates_;
  mutable bft::client::SeqNumberGenerator sn_gen_;
  mutable std::mutex lock_;
  mutable std::mutex bftclient_lock_;
  mutable std::condition_variable new_updates_;
  std::atomic_bool stopped{true};
  std::thread consumer_;
};

}  // namespace concord::client::reconfiguration