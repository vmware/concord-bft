// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "bftengine/IStateTransfer.hpp"
#include "db_interfaces.h"
#include "kvbc_key_types.hpp"
#include "concord.cmf.hpp"
#include "reconfiguration/ireconfiguration.hpp"
#include "SysConsts.hpp"

namespace concord::kvbc {
/*
 * The state transfer reconfiguration handler is meant to handler reconfiguration state changes by a replica that was
 * not responsive during the actual reconfiguration action.
 */
class StReconfigurationHandler {
 public:
  StReconfigurationHandler(bftEngine::IStateTransfer& st, IReader& ro_storage) : ro_storage_(ro_storage) {
    st.addOnTransferringCompleteCallback([&](uint64_t cp) { stCallBack(cp); },
                                         bftEngine::IStateTransfer::StateTransferCallBacksPriorities::HIGH);
  }

  void registerHandler(std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> handler) {
    orig_reconf_handlers_.push_back(handler);
  }

  void pruneOnStartup();

 private:
  void stCallBack(uint64_t);

  template <typename T>
  void deserializeCmfMessage(T& msg, const std::string& strval);
  template <typename T>
  bool handlerStoredCommand(const std::string& key, uint64_t current_cp_num);
  uint64_t getStoredBftSeqNum(BlockId bid);
  uint64_t getEpochNumber(uint64_t bid);

  bool handle(const concord::messages::WedgeCommand&, uint64_t, uint64_t, uint64_t) { return true; }
  bool handle(const concord::messages::DownloadCommand&, uint64_t, uint64_t, uint64_t) { return true; }
  bool handle(const concord::messages::InstallCommand& cmd, uint64_t, uint64_t, uint64_t) { return true; }
  bool handle(const concord::messages::KeyExchangeCommand&, uint64_t, uint64_t, uint64_t) { return true; }
  bool handle(const concord::messages::AddRemoveCommand&, uint64_t, uint64_t, uint64_t) { return true; }
  bool handle(const concord::messages::AddRemoveWithWedgeCommand&, uint64_t, uint64_t, uint64_t);
  bool handle(const concord::messages::PruneRequest&, uint64_t, uint64_t, uint64_t);

  >>>>>>> Consider the epoch number on state transfer handler
  kvbc::IReader& ro_storage_;
  std::vector<std::shared_ptr<concord::reconfiguration::IReconfigurationHandler>> orig_reconf_handlers_;
};
}  // namespace concord::kvbc