// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "Bitmap.hpp"
#include "ViewsManager.hpp"
#include "ReplicaConfig.hpp"
#include "PersistentStorageDescriptors.hpp"

#include <cstdint>
#include <optional>
#include <vector>

namespace bftEngine {
namespace impl {
class PrePrepareMsg;
class CheckpointMsg;
class ViewChangeMsg;
class NewViewMsg;
class FullCommitProofMsg;
class PrepareFullMsg;
class CommitFullMsg;

// The PersistentStorage interface is used to write/read the concord-bft state
// to/from a persistent storage. In case the replica's process is killed,
// crashed or restarted, this interface is used to restore the concord-bft
// state from the persistent storage.
// PersistentStorage is designed to only store data elements that are essential
// to reconstruct the internal in-memory data structures of concord-bft.
// For simplicity and efficiency, some of the internal data elements are not
// written to the persistent storage (for example, messages with partial
// threshold signatures are not stored in persistent storage; if needed such
// messages are re-created and retransmitted by the replicas).

class PersistentStorage {
 public:
  virtual ~PersistentStorage() = default;

  //////////////////////////////////////////////////////////////////////////
  // Transactions management
  //////////////////////////////////////////////////////////////////////////

  // begin re-entrant write-only transaction
  // returns the number of nested transactions
  virtual uint8_t beginWriteTran() = 0;

  // end re-entrant write-only transaction
  // returns the number of remaining nested transactions
  virtual uint8_t endWriteTran() = 0;

  // return true IFF write-only transactions are running now
  virtual bool isInWriteTran() const = 0;

  //////////////////////////////////////////////////////////////////////////
  // Update methods (should only be used in write-only transactions)
  //////////////////////////////////////////////////////////////////////////
  virtual void setLastExecutedSeqNum(SeqNum seqNum) = 0;
  virtual void setPrimaryLastUsedSeqNum(SeqNum seqNum) = 0;
  virtual void setStrictLowerBoundOfSeqNums(SeqNum seqNum) = 0;
  virtual void setLastViewThatTransferredSeqNumbersFullyExecuted(ViewNum view) = 0;

  // DescriptorOfLastExitFromView contains pointers to messages (the content of
  // the messages should be copied, the caller is the owner of these messages).
  virtual void setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &prevViewDesc) = 0;

  // DescriptorOfLastNewView contains pointers to messages (the content of the
  // messages should be copied, the caller is the owner of these messages).
  virtual void setDescriptorOfLastNewView(const DescriptorOfLastNewView &prevViewDesc) = 0;

  virtual void setDescriptorOfLastExecution(const DescriptorOfLastExecution &prevViewDesc) = 0;

  // DescriptorOfLastStableCheckpoint contains pointers to Checkpoint messages representing
  // the proof for our latest stable Checkpoint.
  virtual void setDescriptorOfLastStableCheckpoint(const DescriptorOfLastStableCheckpoint &stableCheckDesc) = 0;

  // We have two windows "SeqNumWindow" and "CheckWindow"
  // TODO(GG): explain the windows.

  virtual void setLastStableSeqNum(SeqNum seqNum) = 0;

  // The window of sequence numbers is:
  // { i | LS + 1 <= i <= LS + kWorkWindowSize }
  // where LS=lastStableSeqNum
  //
  // The window of checkpoints is:
  // { LS, LS + CWS, LS + 2 * CWS }
  // where LS=lastStableSeqNum and CWS=checkpointWindowSize

  virtual void clearSeqNumWindow() = 0;

  virtual void setPrePrepareMsgInSeqNumWindow(SeqNum seqNum, PrePrepareMsg *msg) = 0;
  virtual void setSlowStartedInSeqNumWindow(SeqNum seqNum, bool slowStarted) = 0;
  virtual void setFullCommitProofMsgInSeqNumWindow(SeqNum seqNum, FullCommitProofMsg *msg) = 0;
  virtual void setForceCompletedInSeqNumWindow(SeqNum seqNum, bool forceCompleted) = 0;
  virtual void setPrepareFullMsgInSeqNumWindow(SeqNum seqNum, PrepareFullMsg *msg) = 0;
  virtual void setCommitFullMsgInSeqNumWindow(SeqNum seqNum, CommitFullMsg *msg) = 0;

  virtual void setCheckpointMsgInCheckWindow(SeqNum seqNum, CheckpointMsg *msg) = 0;
  virtual void setCompletedMarkInCheckWindow(SeqNum seqNum, bool mark) = 0;

  // User data to be persisted, e.g. application-specific replica-local data, scratchpad data, etc.
  // Provide two methods - one that does it atomically and one that does it in an already created transaction.
  // setUserDataInTransaction() has a precondition that a transaction has already been created.
  virtual void setUserDataAtomically(const void *data, std::size_t numberOfBytes) = 0;
  virtual void setUserDataInTransaction(const void *data, std::size_t numberOfBytes) = 0;

  virtual void setEraseMetadataStorageFlag() = 0;
  virtual bool getEraseMetadataStorageFlag() = 0;
  virtual void eraseMetadata() = 0;

  virtual void setNewEpochFlag(bool flag) = 0;
  virtual bool getNewEpochFlag() = 0;

  virtual bool setReplicaSpecificInfo(uint32_t clientId, uint64_t requestSeqNum, char *rsiData, size_t rsiSize) = 0;

  //////////////////////////////////////////////////////////////////////////
  // Read methods (should only be used before using write-only transactions)
  //////////////////////////////////////////////////////////////////////////
  virtual void getReplicaSpecificInfo(uint32_t clientId, uint64_t requestSeqNum, char *rsiData, size_t &rsiSize) = 0;
  virtual SeqNum getLastExecutedSeqNum() = 0;
  virtual SeqNum getPrimaryLastUsedSeqNum() = 0;
  virtual SeqNum getStrictLowerBoundOfSeqNums() = 0;
  virtual ViewNum getLastViewThatTransferredSeqNumbersFullyExecuted() = 0;

  virtual bool hasDescriptorOfLastExitFromView() = 0;
  virtual DescriptorOfLastExitFromView getAndAllocateDescriptorOfLastExitFromView() = 0;

  virtual bool hasDescriptorOfLastNewView() = 0;
  virtual DescriptorOfLastNewView getAndAllocateDescriptorOfLastNewView() = 0;

  virtual bool hasDescriptorOfLastExecution() = 0;
  virtual DescriptorOfLastExecution getDescriptorOfLastExecution() = 0;

  virtual DescriptorOfLastStableCheckpoint getDescriptorOfLastStableCheckpoint() = 0;

  virtual SeqNum getLastStableSeqNum() = 0;

  virtual PrePrepareMsg *getAndAllocatePrePrepareMsgInSeqNumWindow(SeqNum seqNum) = 0;
  virtual bool getSlowStartedInSeqNumWindow(SeqNum seqNum) = 0;
  virtual FullCommitProofMsg *getAndAllocateFullCommitProofMsgInSeqNumWindow(SeqNum seqNum) = 0;
  virtual bool getForceCompletedInSeqNumWindow(SeqNum seqNum) = 0;
  virtual PrepareFullMsg *getAndAllocatePrepareFullMsgInSeqNumWindow(SeqNum seqNum) = 0;
  virtual CommitFullMsg *getAndAllocateCommitFullMsgInSeqNumWindow(SeqNum seqNum) = 0;
  virtual CheckpointMsg *getAndAllocateCheckpointMsgInCheckWindow(SeqNum seqNum) = 0;
  virtual bool getCompletedMarkInCheckWindow(SeqNum seqNum) = 0;

  virtual std::optional<std::vector<std::uint8_t>> getUserData() const = 0;
  virtual void setDbCheckpointMetadata(const std::vector<std::uint8_t> &) = 0;
  virtual std::optional<std::vector<std::uint8_t>> getDbCheckpointMetadata(const uint32_t &) = 0;
};

}  // namespace impl
}  // namespace bftEngine
