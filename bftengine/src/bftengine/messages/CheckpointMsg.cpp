// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "CheckpointMsg.hpp"
#include "assertUtils.hpp"
#include "SigManager.hpp"
#include "EpochManager.hpp"

namespace bftEngine {
namespace impl {

CheckpointMsg::CheckpointMsg(ReplicaId genReplica,
                             SeqNum seqNum,
                             std::uint64_t state,
                             const Digest& stateDigest,
                             const Digest& reservedPagesDigest,
                             const Digest& rvbDataDigest,
                             bool stateIsStable,
                             const concordUtils::SpanContext& spanContext)
    : MessageBase(genReplica,
                  MsgCode::Checkpoint,
                  spanContext.data().size(),
                  sizeof(Header) + SigManager::instance()->getMySigLength()) {
  b()->seqNum = seqNum;
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  b()->state = state;
  b()->stateDigest = stateDigest;
  b()->reservedPagesDigest = reservedPagesDigest;
  b()->rvbDataDigest = rvbDataDigest;
  b()->flags = 0;
  b()->genReplicaId = genReplica;
  if (stateIsStable) b()->flags |= 0x1;
  std::memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

void CheckpointMsg::sign() {
  auto sigManager = SigManager::instance();
  sigManager->sign(body(), sizeof(Header), body() + sizeof(Header) + spanContextSize(), sigManager->getMySigLength());
}

void CheckpointMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(type() == MsgCode::Checkpoint);
  ConcordAssert(senderId() != repInfo.myId());

  auto sigManager = SigManager::instance();

  if (size() < sizeof(Header) + spanContextSize() || !repInfo.isIdOfReplica(senderId()) ||
      !repInfo.isIdOfReplica(idOfGeneratedReplica()) || (seqNumber() % checkpointWindowSize != 0) ||
      (stateDigest().isZero() && reservedPagesDigest().isZero() && rvbDataDigest().isZero())) {
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic validations"));
  }

  auto sigLen = sigManager->getSigLength(idOfGeneratedReplica());

  if (size() < sizeof(Header) + spanContextSize() + sigLen) {
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": size"));
  }

  if (!sigManager->verifySig(
          idOfGeneratedReplica(), body(), sizeof(Header), body() + sizeof(Header) + spanContextSize(), sigLen))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": verifySig"));
  // TODO(GG): consider to protect against messages that are larger than needed (here and in other messages)
}

bool CheckpointMsg::equivalent(const CheckpointMsg* a, const CheckpointMsg* b) {
  bool equal = (a->seqNumber() == b->seqNumber()) && (a->stateDigest() == b->stateDigest()) &&
               (a->reservedPagesDigest() == b->reservedPagesDigest()) && (a->rvbDataDigest() == b->rvbDataDigest()) &&
               (a->state() == b->state());
  if (!equal) {
    auto logger = logging::getLogger("concord.bft");
    std::ostringstream oss;
    const auto* ah = a->b();
    const auto* bh = b->b();

    oss << "Mismatched Checkpoints, cp1:"
        << KVLOG(ah->seqNum,
                 ah->epochNum,
                 ah->state,
                 ah->stateDigest,
                 ah->reservedPagesDigest,
                 ah->rvbDataDigest,
                 ah->genReplicaId,
                 ah->flags)
        << "cp2:"
        << KVLOG(bh->seqNum,
                 bh->epochNum,
                 bh->state,
                 bh->stateDigest,
                 bh->reservedPagesDigest,
                 bh->rvbDataDigest,
                 bh->genReplicaId,
                 bh->flags);
    LOG_WARN(logger, oss.str());
    metrics_.number_of_mismatches_++;
    UpdateAggregator();
  }
  return equal;
}

concordMetrics::Component CheckpointMsg::metrics_component_{
    concordMetrics::Component("checkpoint_msg", std::make_shared<concordMetrics::Aggregator>())};

CheckpointMsg::Metrics CheckpointMsg::metrics_{metrics_component_.RegisterCounter("number_of_checkpoint_mismatch")};

}  // namespace impl
}  // namespace bftEngine
