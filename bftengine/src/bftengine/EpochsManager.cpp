// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "EpochsManager.hpp"
#include "Crypto.hpp"
#include "InternalBFTClient.hpp"
#include "Replica.hpp"
#include "concord.cmf.hpp"
#include "messages/ClientRequestMsg.hpp"

namespace bftEngine {

std::string EpochManager::createEpochUpdateMsg(uint64_t epoch) {
  concord::messages::ReconfigurationRequest req;
  req.command = concord::messages::EpochUpdateMsg{replica_id_, epoch};
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, req);
  std::string sig(signer_->signatureLength(), '\0');
  std::size_t sig_length{0};
  signer_->sign(
      reinterpret_cast<char*>(data_vec.data()), data_vec.size(), sig.data(), signer_->signatureLength(), sig_length);
  req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  data_vec.clear();
  concord::messages::serialize(data_vec, req);
  return std::string(data_vec.begin(), data_vec.end());
}
EpochManager::EpochManager(EpochManager::InitData* id)
    : bft_client_{id->cl},
      signer_{id->signer},
      replica_id_{id->replica_id},
      epochs_data_{id->n},
      scratchPage_{std::string()},
      n_{id->n},
      f_{id->f},
      is_ro_{id->is_ro},
      aggregator_{std::make_shared<concordMetrics::Aggregator>()},
      metrics_{concordMetrics::Component("epoch_manager", aggregator_)},
      epoch_number{metrics_.RegisterGauge("epoch_number", 0)},
      num_of_sent_epoch_messages_{metrics_.RegisterCounter("num_sent_epoch_messages")} {
  is_ro_ = id->is_ro;
  scratchPage_.resize(sizeOfReservedPage());
  if (loadReservedPage(0, sizeOfReservedPage(), scratchPage_.data())) {
    std::istringstream inStream;
    inStream.str(scratchPage_);
    concord::serialize::Serializable::deserialize(inStream, epochs_data_);
  }
  epoch_number.Get().Set(epochs_data_.epochs_[replica_id_]);

  id->state_transfer.addOnTransferringCompleteCallback(
      [&](uint64_t) {
        if (loadReservedPage(0, sizeOfReservedPage(), scratchPage_.data())) {
          std::istringstream inStream;
          inStream.str(scratchPage_);
          concord::serialize::Serializable::deserialize(inStream, epochs_data_);
          if (n_ > epochs_data_.n_) {
            for (auto i = epochs_data_.n_; i < n_; i++) {
              epochs_data_.epochs_[i] = 0;
            }
            epochs_data_.n_ = n_;
          }
        }
        epoch_number.Get().Set(epochs_data_.epochs_[replica_id_]);
        metrics_.UpdateAggregator();
      },
      IStateTransfer::FIRST);
}

void EpochManager::updateEpochForReplica(uint32_t replica_id, uint64_t epoch_id) {
  if (is_ro_) return;
  epochs_data_.epochs_[replica_id] = epoch_id;
  if (replica_id == replica_id_) {
    epoch_number.Get().Set(epoch_id);
    metrics_.UpdateAggregator();
  }
}
uint64_t EpochManager::getEpochForReplica(uint32_t replica_id) { return epochs_data_.epochs_[replica_id]; }
const EpochManager::EpochsData& EpochManager::getEpochData() { return epochs_data_; }
void EpochManager::sendUpdateEpochMsg(uint64_t epoch) {
  if (is_ro_) return;
  LOG_INFO(GL, "sending an update for the replica epoch number");
  auto strMsg = createEpochUpdateMsg(epoch);
  bft_client_->sendRequest(RECONFIG_FLAG,
                           strMsg.size(),
                           strMsg.c_str(),
                           "EpochUpdateMsg-" + std::to_string(epoch) + "-" + std::to_string(replica_id_));
  num_of_sent_epoch_messages_.Get().Inc();
  metrics_.UpdateAggregator();
}
void EpochManager::save() {
  std::ostringstream outStream;
  concord::serialize::Serializable::serialize(outStream, epochs_data_);
  auto data = outStream.str();
  saveReservedPage(0, data.size(), data.data());
  metrics_.UpdateAggregator();
}

int64_t EpochManager::getHighestQuorumedEpoch() {
  std::map<uint64_t, uint64_t> epochs;
  for (uint32_t i = 0; i < n_; i++) {
    auto currEpoch = getEpochForReplica(i);
    epochs[currEpoch]++;
  }
  for (const auto& [epoch, size] : epochs) {
    if (size >= 2 * f_ + 1) return epoch;
  }
  return -1;
}
void EpochManager::reserveEpochNumberForLaterUse(uint64_t epoch) {
  reserved_epoch = epoch;
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
  auto sn = now_ms.count();
  auto strMsg = createEpochUpdateMsg(epoch);
  auto cid = "EpochUpdateMsg-" + std::to_string(epoch) + "-" + std::to_string(replica_id_);
  pending_request_.reset(
      new ClientRequestMsg(bft_client_->getClientId(), RECONFIG_FLAG, sn, strMsg.size(), strMsg.c_str(), 60000, cid));
}
void EpochManager::sendReservedEpochNumber() {
  if (reserved_epoch <= getEpochForReplica(replica_id_)) return;
  // We want to send exactly the same request (to avoid unnecessary view changes)
  LOG_INFO(GL, "sending an update for the replica epoch number");
  ClientRequestMsg* copy = new ClientRequestMsg(bft_client_->getClientId(),
                                                pending_request_->flags(),
                                                pending_request_->requestSeqNum(),
                                                pending_request_->requestLength(),
                                                pending_request_->requestBuf(),
                                                6000,
                                                pending_request_->getCid());
  bft_client_->sendRequest(copy);
}

void EpochManager::EpochsData::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, n_);
  for (uint32_t i = 0; i < n_; i++) {
    serialize(outStream, epochs_.at(i));
  }
}
void EpochManager::EpochsData::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, n_);
  for (uint32_t i = 0; i < n_; i++) {
    deserialize(inStream, epochs_[i]);
  }
}
}  // namespace bftEngine