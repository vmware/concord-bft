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

#include "reconfiguration/ireconfiguration.hpp"
#include "bftengine/IStateTransfer.hpp"
#include "db_interfaces.h"
#include "kvbc_key_types.hpp"

namespace concord::kvbc::reconfiguration {
class StReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler {
 public:
  StReconfigurationHandler(bftEngine::IStateTransfer& st, IReader& ro_storage) : ro_storage_(ro_storage) {
    st.addOnTransferringCompleteCallback([&](uint64_t cp) { stCallBack(cp); });
  }

 private:
  template <typename T>
  void deserializeCmfMessage(T& msg, const std::string& strval);
  template <typename T>
  bool handlerStoredCommand(const std::string& key, concord::messages::ReconfigurationErrorMsg& error_msg);
  uint64_t getStoredBftSeqNum(BlockId bid);

  void stCallBack(uint64_t);
  bool handle(const concord::messages::WedgeCommand&, uint64_t, concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::WedgeStatusRequest&,
              concord::messages::WedgeStatusResponse&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::GetVersionCommand&,
              concord::messages::GetVersionResponse&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::DownloadCommand&,
              uint64_t,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::DownloadStatusCommand&,
              concord::messages::DownloadStatus&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::InstallCommand& cmd,
              uint64_t,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::InstallStatusCommand& cmd,
              concord::messages::InstallStatusResponse&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::KeyExchangeCommand&,
              concord::messages::ReconfigurationErrorMsg&,
              uint64_t) override {
    return true;
  }
  bool handle(const concord::messages::AddRemoveCommand&,
              concord::messages::ReconfigurationErrorMsg&,
              uint64_t) override {
    return true;
  }
  bool verifySignature(const concord::messages::ReconfigurationRequest&,
                       concord::messages::ReconfigurationErrorMsg&) const override {
    return true;
  }
  bool handle(const concord::messages::AddRemoveCommand& cmd,
              uint64_t bftSeqNum,
              concord::messages::ReconfigurationErrorMsg& error_msg) {
    return handle(cmd, error_msg, bftSeqNum);
  }

  bool handle(const concord::messages::KeyExchangeCommand& cmd,
              uint64_t bftSeqNum,
              concord::messages::ReconfigurationErrorMsg& error_msg) {
    return handle(cmd, error_msg, bftSeqNum);
  }
  kvbc::IReader& ro_storage_;
};
}  // namespace concord::kvbc::reconfiguration