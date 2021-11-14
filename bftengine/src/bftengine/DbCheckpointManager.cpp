// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "DbCheckpointManager.hpp"
#include "db_checkpoint.cmf.hpp"
#include "ReplicaConfig.hpp"
#include "Replica.hpp"
#include "SigManager.hpp"

namespace bftEngine::impl {

void DbCheckpointManager::sendInternalCreateDbCheckpointMsg(const SeqNum& seqNum, bool noop) {
  auto replica_id = bftEngine::ReplicaConfig::instance().getreplicaId();
  concord::messages::db_checkpoint::CreateDbCheckpoint req;
  req.sender = replica_id;
  req.seqNum = seqNum;
  req.noop = noop;
  std::vector<uint8_t> data_vec;
  concord::messages::db_checkpoint::serialize(data_vec, req);
  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  uint16_t sig_length{0};
  SigManager::instance()->sign(reinterpret_cast<char*>(data_vec.data()), data_vec.size(), sig.data(), sig_length);
  req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  data_vec.clear();
  concord::messages::db_checkpoint::serialize(data_vec, req);
  std::string strMsg(data_vec.begin(), data_vec.end());
  std::string cid = "replicaDbCheckpoint_" + std::to_string(seqNum) + "_" + std::to_string(replica_id);
  if (client_) client_->sendRequest(bftEngine::DB_CHECKPOINT_FLAG, strMsg.size(), strMsg.c_str(), cid);
}

}  // namespace bftEngine::impl
