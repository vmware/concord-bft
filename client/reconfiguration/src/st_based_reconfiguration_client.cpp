// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "concord.cmf.hpp"
#include "bftengine/ReconfigurationCmd.hpp"
#include "client/reconfiguration/st_based_reconfiguration_client.hpp"
namespace concord::client::reconfiguration {

STBasedReconfigurationClient::STBasedReconfigurationClient(std::function<void(uint64_t)> updateStateCb,
                                                           const uint64_t& blockId,
                                                           uint64_t interval_timeout_ms)
    : storeReconfigBlockToMdtCb_(std::move(updateStateCb)),
      lastKnownReconfigurationCmdBlockId_(blockId),
      interval_timeout_ms_(interval_timeout_ms) {}

State STBasedReconfigurationClient::getNextState(uint64_t lastKnownBlockId) const {
  std::unique_lock<std::mutex> lk(lock_);
  while (!stopped_ && updates_.empty()) {
    new_updates_.wait(lk, [this]() { return !updates_.empty(); });
  }
  if (stopped_) return {lastKnownBlockId, {}};
  auto ret = std::move(updates_.front());
  updates_.pop();
  return ret;
}
void STBasedReconfigurationClient::pushUpdate(std::vector<State>& states) {
  std::lock_guard<std::mutex> lg(lock_);
  bool notify = false;
  std::sort(states.begin(), states.end(), [](const State& a, const State& b) {
    return a.blockid < b.blockid;
  });  // sort the states with increating blockId
  for (auto& s : states) {
    if (s.blockid > lastKnownReconfigurationCmdBlockId_) {
      lastKnownReconfigurationCmdBlockId_ = s.blockid;
      updates_.push(std::move(s));
      notify = true;
    }
  }
  if (notify) new_updates_.notify_one();
}
State STBasedReconfigurationClient::getLatestClientUpdate(uint16_t clientId) const {
  return {lastKnownReconfigurationCmdBlockId_, {}};
}
bool STBasedReconfigurationClient::updateState(const WriteState& state) {
  bftEngine::ReconfigurationCmd::ReconfigurationCmdData::cmdBlock cmdData;
  std::istringstream inStream;
  std::string page(state.data.begin(), state.data.end());
  inStream.str(page);
  concord::serialize::Serializable::deserialize(inStream, cmdData);
  if (storeReconfigBlockToMdtCb_) storeReconfigBlockToMdtCb_(cmdData.blockId_);
  if (state.callBack) state.callBack();
  return true;
}
}  // namespace concord::client::reconfiguration