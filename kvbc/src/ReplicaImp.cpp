// Copyright 2018-2019 VMware, all rights reserved
//
// KV Blockchain replica implementation.

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <unistd.h>

#include "ReplicaImp.h"
#include <inttypes.h>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include "CommDefs.hpp"
#include "kv_types.hpp"
#include "hex_tools.h"
#include "replica_state_sync.h"
#include "sliver.hpp"
#include "blockchain/db_interfaces.h"
#include "blockchain/block.h"
#include "storage/db_metadata_storage.h"

using bftEngine::ICommunication;
using bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;
using bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest;

using concord::storage::blockchain::Key;
using concord::storage::IDBClient;
using concord::storage::DBMetadataStorage;

using concord::storage::blockchain::DBAdapter;
using concord::storage::blockchain::BlockId;
using concord::storage::blockchain::ILocalKeyValueStorageReadOnly;
using concord::storage::blockchain::ILocalKeyValueStorageReadOnlyIterator;
using concord::storage::blockchain::DBKeyManipulator;
namespace block = concord::storage::blockchain::block;

using concordUtils::SetOfKeyValuePairs;
using concordUtils::Value;
using concordUtils::KeyValuePair;

namespace concord {
namespace kvbc {

/**
 * Opens the database and creates the replica thread. Replica state moves to
 * Starting.
 */
Status ReplicaImp::start() {
  LOG_INFO(logger, "ReplicaImp::Start() id = " << m_replicaConfig.replicaId);

  if (m_currentRepStatus != RepStatus::Idle) {
    return Status::IllegalOperation("todo");
  }

  m_currentRepStatus = RepStatus::Starting;
  m_metadataStorage = new DBMetadataStorage(m_bcDbAdapter->getDb().get(), DBKeyManipulator::generateMetadataKey);

  if (m_replicaConfig.isReadOnly) {
    LOG_INFO(logger, "ReadOnly mode");
    m_replicaPtr =
        bftEngine::IReplica::createNewRoReplica(&m_replicaConfig, m_stateTransfer, m_ptrComm, m_metadataStorage);
  } else {
    createReplicaAndSyncState();
  }
  m_replicaPtr->SetAggregator(aggregator_);
  m_replicaPtr->start();
  m_currentRepStatus = RepStatus::Running;

  /// TODO(IG, GG)
  /// add return value to start/stop

  return Status::OK();
}

void ReplicaImp::createReplicaAndSyncState() {
  bool isNewStorage = m_metadataStorage->isNewStorage();
  LOG_INFO(logger, "createReplicaAndSyncState: isNewStorage= " << isNewStorage);
  m_replicaPtr = bftEngine::IReplica::createNewReplica(
      &m_replicaConfig, m_cmdHandler, m_stateTransfer, m_ptrComm, m_metadataStorage);
  if (!isNewStorage && !m_stateTransfer->isCollectingState()) {
    uint64_t removedBlocksNum = replicaStateSync_->execute(
        logger, *m_bcDbAdapter, m_appState->m_lastReachableBlock, m_replicaPtr->getLastExecutedSequenceNum());
    m_lastBlock -= removedBlocksNum;
    m_appState->m_lastReachableBlock -= removedBlocksNum;
    LOG_INFO(logger,
             "createReplicaAndSyncState: removedBlocksNum = "
                 << removedBlocksNum << ", new m_lastBlock = " << m_lastBlock
                 << ", new m_lastReachableBlock = " << m_appState->m_lastReachableBlock);
  }
}

/**
 * Closes the database. Call `wait()` after this to wait for thread to stop.
 */
Status ReplicaImp::stop() {
  m_currentRepStatus = RepStatus::Stopping;
  m_replicaPtr->stop();
  m_currentRepStatus = RepStatus::Idle;
  return Status::OK();
}

ReplicaImp::RepStatus ReplicaImp::getReplicaStatus() const { return m_currentRepStatus; }

const ILocalKeyValueStorageReadOnly &ReplicaImp::getReadOnlyStorage() { return m_InternalStorageWrapperForIdleMode; }

Status ReplicaImp::addBlockToIdleReplica(const SetOfKeyValuePairs &updates) {
  if (getReplicaStatus() != IReplica::RepStatus::Idle) {
    return Status::IllegalOperation("");
  }

  BlockId d;
  return addBlockInternal(updates, d);
}

Status ReplicaImp::get(const Sliver &key, Sliver &outValue) const {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  BlockId dummy;
  return getInternal(m_lastBlock, key, outValue, dummy);
}

Status ReplicaImp::get(BlockId readVersion, const Sliver &key, Sliver &outValue, BlockId &outBlock) const {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  return getInternal(readVersion, key, outValue, outBlock);
}

BlockId ReplicaImp::getLastBlock() const { return m_lastBlock; }

Status ReplicaImp::getBlockData(BlockId blockId, SetOfKeyValuePairs &outBlockData) const {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  Sliver block = getBlockInternal(blockId);

  if (block.length() == 0) {
    return Status::NotFound("todo");
  }

  outBlockData = block::getData(block);

  return Status::OK();
}

Status ReplicaImp::mayHaveConflictBetween(const Sliver &key, BlockId fromBlock, BlockId toBlock, bool &outRes) const {
  // TODO(GG): add assert or print warning if fromBlock==0 (all keys have a
  // conflict in block 0)

  // we conservatively assume that we have a conflict
  outRes = true;

  Sliver dummy;
  BlockId block = 0;
  Status s = getInternal(toBlock, key, dummy, block);
  if (s.isOK() && block < fromBlock) {
    outRes = false;
  }

  return s;
}

ILocalKeyValueStorageReadOnlyIterator *ReplicaImp::getSnapIterator() const {
  return m_InternalStorageWrapperForIdleMode.getSnapIterator();
}

Status ReplicaImp::freeSnapIterator(ILocalKeyValueStorageReadOnlyIterator *iter) const {
  return m_InternalStorageWrapperForIdleMode.freeSnapIterator(iter);
}

void ReplicaImp::monitor() const { m_InternalStorageWrapperForIdleMode.monitor(); }

Status ReplicaImp::addBlock(const SetOfKeyValuePairs &updates, BlockId &outBlockId) {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  // TODO(GG): what do we want to do with several identical keys in the same
  // block?

  return addBlockInternal(updates, outBlockId);
}

void ReplicaImp::set_command_handler(ICommandsHandler *handler) { m_cmdHandler = handler; }

ReplicaImp::ReplicaImp(ICommunication *comm,
                       bftEngine::ReplicaConfig &replicaConfig,
                       DBAdapter *dbAdapter,
                       std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : logger(concordlogger::Log::getLogger("skvbc.replicaImp")),
      m_currentRepStatus(RepStatus::Idle),
      m_InternalStorageWrapperForIdleMode(this),
      m_bcDbAdapter(dbAdapter),
      m_lastBlock(dbAdapter->getLatestBlock()),
      m_ptrComm(comm),
      m_replicaConfig(replicaConfig),
      m_appState(new BlockchainAppState(this)),
      aggregator_(aggregator) {
  bftEngine::SimpleBlockchainStateTransfer::Config state_transfer_config;

  state_transfer_config.myReplicaId = m_replicaConfig.replicaId;
  state_transfer_config.cVal = m_replicaConfig.cVal;
  state_transfer_config.fVal = m_replicaConfig.fVal;
  state_transfer_config.numReplicas = m_replicaConfig.numReplicas + m_replicaConfig.numRoReplicas;
  if (replicaConfig.maxNumOfReservedPages > 0)
    state_transfer_config.maxNumOfReservedPages = replicaConfig.maxNumOfReservedPages;
  if (replicaConfig.sizeOfReservedPage > 0) state_transfer_config.sizeOfReservedPage = replicaConfig.sizeOfReservedPage;

  m_stateTransfer = bftEngine::SimpleBlockchainStateTransfer::create(
      state_transfer_config, m_appState.get(), m_bcDbAdapter->getDb(), aggregator);
}

ReplicaImp::~ReplicaImp() {
  if (m_replicaPtr) {
    if (m_replicaPtr->isRunning()) {
      m_replicaPtr->stop();
    }
    delete m_replicaPtr;
  }

  if (m_stateTransfer) {
    if (m_stateTransfer->isRunning()) {
      m_stateTransfer->stopRunning();
    }
    delete m_stateTransfer;
  }
}

Status ReplicaImp::addBlockInternal(const SetOfKeyValuePairs &updates, BlockId &outBlockId) {
  m_lastBlock++;
  m_appState->m_lastReachableBlock++;

  BlockId block = m_lastBlock;
  SetOfKeyValuePairs updatesInNewBlock;

  LOG_DEBUG(logger, "block:" << block << " updates: " << updates.size());

  StateTransferDigest stDigest;
  if (block > 1) {
    Sliver parentBlockData;
    bool found;
    m_bcDbAdapter->getBlockById(block - 1, parentBlockData, found);
    if (!found || parentBlockData.length() == 0) {
      //(IG): panic, data corrupted
      LOG_FATAL(logger, "addBlockInternal: no block or block data for id " << block - 1);
      exit(1);
    }

    bftEngine::SimpleBlockchainStateTransfer::computeBlockDigest(
        block - 1, reinterpret_cast<const char *>(parentBlockData.data()), parentBlockData.length(), &stDigest);
  } else {
    memset(stDigest.content, 0, BLOCK_DIGEST_SIZE);
  }

  Sliver blockRaw = block::create(updates, updatesInNewBlock, stDigest.content);

  Status s = m_bcDbAdapter->addBlockAndUpdateMultiKey(updatesInNewBlock, block, blockRaw);
  if (!s.isOK()) {
    LOG_ERROR(logger, "Failed to add block or update keys for block " << block);
    return s;
  }

  outBlockId = block;
  return Status::OK();
}

Status ReplicaImp::getInternal(BlockId readVersion, Sliver key, Sliver &outValue, BlockId &outBlock) const {
  Status s = m_bcDbAdapter->getKeyByReadVersion(readVersion, key, outValue, outBlock);
  if (!s.isOK()) {
    LOG_ERROR(logger, "Failed to get key " << key << " by read version " << readVersion);
    return s;
  }

  return Status::OK();
}

void ReplicaImp::insertBlockInternal(BlockId blockId, Sliver block) {
  if (blockId > m_lastBlock) {
    m_lastBlock = blockId;
  }
  // when ST runs, blocks arrive in batches in reverse order. we need to keep
  // track on the "Gap" and to close it. Only when it is closed, the last
  // reachable block becomes the same as the last block
  if (blockId == m_appState->m_lastReachableBlock + 1) {
    m_appState->m_lastReachableBlock = m_lastBlock;
  }

  bool found = false;
  Sliver existingBlock;
  Status s = m_bcDbAdapter->getBlockById(blockId, existingBlock, found);
  if (!s.isOK()) {
    // the replica is corrupted!
    // TODO(GG): what do we want to do now?
    LOG_FATAL(logger, "replica may be corrupted\n\n");
    // TODO(GG): how do we want to handle this - restart replica?
    exit(1);
  }

  // if we already have a block with the same ID
  if (found && existingBlock.length() > 0) {
    if (existingBlock.length() != block.length() || memcmp(existingBlock.data(), block.data(), block.length())) {
      // the replica is corrupted !
      // TODO(GG): what do we want to do now ?
      LOG_ERROR(logger,
                "found block " << blockId << ", size in db is " << existingBlock.length() << ", inserted is "
                               << block.length() << ", data in db " << existingBlock << ", data inserted " << block);
      LOG_ERROR(logger,
                "Block size test " << (existingBlock.length() != block.length()) << ", block data test "
                                   << (memcmp(existingBlock.data(), block.data(), block.length())));

      m_bcDbAdapter->deleteBlockAndItsKeys(blockId);

      // TODO(GG): how do we want to handle this - restart replica?
      // exit(1);
      return;
    }
  } else {
    SetOfKeyValuePairs keys;
    if (block.length() > 0) {
      keys = block::getData(block);
    }
    s = m_bcDbAdapter->addBlockAndUpdateMultiKey(keys, blockId, block);
    if (!s.isOK()) {
      // TODO(SG): What to do?
      printf("Failed to add block");
      exit(1);
    }
  }
}

Sliver ReplicaImp::getBlockInternal(BlockId blockId) const {
  assert(blockId <= m_lastBlock);
  Sliver retVal;

  bool found;
  Status s = m_bcDbAdapter->getBlockById(blockId, retVal, found);
  if (!s.isOK()) {
    // TODO(SG): To do something smarter
    LOG_ERROR(logger, "An error occurred in get block");
    return Sliver();
  }

  if (!found) {
    return Sliver();
  } else {
    return retVal;
  }
}

ReplicaImp::StorageWrapperForIdleMode::StorageWrapperForIdleMode(const ReplicaImp *r) : rep(r) {}

Status ReplicaImp::StorageWrapperForIdleMode::get(const Sliver &key, Sliver &outValue) const {
  if (rep->getReplicaStatus() != IReplica::RepStatus::Idle) {
    return Status::IllegalOperation("");
  }

  return rep->get(key, outValue);
}

Status ReplicaImp::StorageWrapperForIdleMode::get(BlockId readVersion,
                                                  const Sliver &key,
                                                  Sliver &outValue,
                                                  BlockId &outBlock) const {
  if (rep->getReplicaStatus() != IReplica::RepStatus::Idle) {
    return Status::IllegalOperation("");
  }

  return rep->get(readVersion, key, outValue, outBlock);
}

BlockId ReplicaImp::StorageWrapperForIdleMode::getLastBlock() const { return rep->getLastBlock(); }

Status ReplicaImp::StorageWrapperForIdleMode::getBlockData(BlockId blockId, SetOfKeyValuePairs &outBlockData) const {
  if (rep->getReplicaStatus() != IReplica::RepStatus::Idle) {
    return Status::IllegalOperation("");
  }

  Sliver block = rep->getBlockInternal(blockId);

  if (block.length() == 0) {
    return Status::NotFound("todo");
  }

  outBlockData = block::getData(block);

  return Status::OK();
}

Status ReplicaImp::StorageWrapperForIdleMode::mayHaveConflictBetween(const Sliver &key,
                                                                     BlockId fromBlock,
                                                                     BlockId toBlock,
                                                                     bool &outRes) const {
  outRes = true;

  Sliver dummy;
  BlockId block = 0;
  Status s = rep->getInternal(toBlock, key, dummy, block);

  if (s.isOK() && block < fromBlock) {
    outRes = false;
  }

  return s;
}

ILocalKeyValueStorageReadOnlyIterator *ReplicaImp::StorageWrapperForIdleMode::getSnapIterator() const {
  return new StorageIterator(this->rep);
}

Status ReplicaImp::StorageWrapperForIdleMode::freeSnapIterator(ILocalKeyValueStorageReadOnlyIterator *iter) const {
  if (iter == NULL) {
    return Status::InvalidArgument("Invalid iterator");
  }

  StorageIterator *storageIter = (StorageIterator *)iter;
  Status s = storageIter->freeInternalIterator();
  delete storageIter;
  return s;
}

void ReplicaImp::StorageWrapperForIdleMode::monitor() const { this->rep->m_bcDbAdapter->monitor(); }

ReplicaImp::StorageIterator::StorageIterator(const ReplicaImp *r)
    : logger(concordlogger::Log::getLogger("skvbc.ReplicaImp")), rep(r) {
  m_iter = r->getBcDbAdapter()->getIterator();
  m_currentBlock = r->getLastBlock();
}

KeyValuePair ReplicaImp::StorageIterator::first(BlockId readVersion, BlockId &actualVersion, bool &isEnd) {
  Key key;
  Value value;
  Status s = rep->getBcDbAdapter()->first(m_iter, readVersion, actualVersion, isEnd, key, value);

  if (s.isNotFound()) {
    isEnd = true;
    m_current = KeyValuePair();
    return m_current;
  }

  if (!s.isOK()) {
    LOG_ERROR(logger, "Failed to get first");
    exit(1);
  }

  m_isEnd = isEnd;
  m_current = KeyValuePair(key, value);

  return m_current;
}

KeyValuePair ReplicaImp::StorageIterator::seekAtLeast(BlockId readVersion,
                                                      const Key &key,
                                                      BlockId &actualVersion,
                                                      bool &isEnd) {
  Key actualKey;
  Value value;
  Status s = rep->getBcDbAdapter()->seekAtLeast(m_iter, key, readVersion, actualVersion, actualKey, value, isEnd);

  if (s.isNotFound()) {
    isEnd = true;
    m_current = KeyValuePair();
    return m_current;
  }

  if (!s.isOK()) {
    LOG_FATAL(logger, "Failed to seek at least");
    exit(1);
  }

  m_isEnd = isEnd;
  m_current = KeyValuePair(actualKey, value);
  return m_current;
}

/**
 * TODO(SG): There is a question mark regarding on these APIs. Suppose I have
 * (k0,2), (k1,7), (k2,4) and I request next(k0,5). Do we return end() (because
 * k1 cannot be returned), or do we return k2?  I implemented the second choice,
 * as it makes better sense. The world in Block 5 did not include k1, that's
 * perfectly OK.
 */
// Note: key,readVersion must exist in map already
KeyValuePair ReplicaImp::StorageIterator::next(BlockId readVersion,
                                               const Key &key,
                                               BlockId &actualVersion,
                                               bool &isEnd) {
  Key nextKey;
  Value nextValue;
  Status s = rep->getBcDbAdapter()->next(m_iter, readVersion, nextKey, nextValue, actualVersion, isEnd);

  if (s.isNotFound()) {
    isEnd = true;
    m_current = KeyValuePair();
    return m_current;
  }

  if (!s.isOK()) {
    LOG_FATAL(logger, "Failed to get next");
    exit(1);
  }

  m_isEnd = isEnd;
  m_current = KeyValuePair(nextKey, nextValue);
  return m_current;
}

KeyValuePair ReplicaImp::StorageIterator::getCurrent() {
  Key key;
  Value value;
  Status s = rep->getBcDbAdapter()->getCurrent(m_iter, key, value);

  if (!s.isOK()) {
    LOG_FATAL(logger, "Failed to get current");
    exit(1);
  }

  m_current = KeyValuePair(key, value);
  return m_current;
}

bool ReplicaImp::StorageIterator::isEnd() {
  bool isEnd;
  Status s = rep->getBcDbAdapter()->isEnd(m_iter, isEnd);

  if (!s.isOK()) {
    LOG_FATAL(logger, "Failed to get current");
    exit(1);
  }

  return isEnd;
}

Status ReplicaImp::StorageIterator::freeInternalIterator() { return rep->getBcDbAdapter()->freeIterator(m_iter); }

/*
 * These functions are used by the ST module to interact with the KVB
 */
ReplicaImp::BlockchainAppState::BlockchainAppState(ReplicaImp *const parent)
    : m_ptrReplicaImpl{parent},
      m_logger{concordlogger::Log::getLogger("blockchainappstate")},
      m_lastReachableBlock{parent->getBcDbAdapter()->getLastReachableBlock()} {}

/*
 * This method assumes that *outBlock is big enough to hold block content
 * The caller is the owner of the memory
 */
bool ReplicaImp::BlockchainAppState::getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) {
  Sliver res = m_ptrReplicaImpl->getBlockInternal(blockId);
  if (0 == res.length()) {
    // in normal state it should not happen. If it happened - the data is
    // corrupted
    LOG_FATAL(m_logger, "Block not found, ID: " << blockId);
    exit(1);
  }

  *outBlockSize = res.length();
  memcpy(outBlock, res.data(), res.length());
  return true;
}

bool ReplicaImp::BlockchainAppState::hasBlock(uint64_t blockId) {
  Sliver res = m_ptrReplicaImpl->getBlockInternal(blockId);
  return res.length() > 0;
}

bool ReplicaImp::BlockchainAppState::getPrevDigestFromBlock(uint64_t blockId, StateTransferDigest *outPrevBlockDigest) {
  assert(blockId > 0);
  Sliver result;
  bool found;
  m_ptrReplicaImpl->m_bcDbAdapter->getBlockById(blockId, result, found);
  if (!found) {
    // in normal state it should not happen. If it happened - the data is
    // corrupted
    LOG_FATAL(m_logger, "Block not found for parent digest, ID: " << blockId);
    exit(1);
  }

  auto parentDigest = block::getParentDigest(result);
  assert(outPrevBlockDigest);
  memcpy(outPrevBlockDigest, parentDigest, BLOCK_DIGEST_SIZE);
  return true;
}

/*
 * This method cant return false by current insertBlockInternal impl.
 * It is used only by State Transfer to synchronize state between replicas.
 */
bool ReplicaImp::BlockchainAppState::putBlock(uint64_t blockId, char *block, uint32_t blockSize) {
  char *tmpBlockPtr = new char[blockSize];
  memcpy(tmpBlockPtr, block, blockSize);
  Sliver s(tmpBlockPtr, blockSize);

  m_ptrReplicaImpl->insertBlockInternal(blockId, s);
  return true;
}

uint64_t ReplicaImp::BlockchainAppState::getLastReachableBlockNum() {
  LOG_INFO(m_logger, "m_lastReachableBlock=" << m_lastReachableBlock);
  return m_lastReachableBlock;
}

uint64_t ReplicaImp::BlockchainAppState::getLastBlockNum() { return m_ptrReplicaImpl->m_lastBlock; }

}  // namespace kvbc
}  // namespace concord
