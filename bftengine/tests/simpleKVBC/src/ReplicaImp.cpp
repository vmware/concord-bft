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

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <inttypes.h>
#include <chrono>

#ifndef _WIN32
#include <sys/param.h>
#include <unistd.h>
#endif

#include "Comparators.h"
#include "CommFactory.hpp"
#include "ReplicaImp.h"
#include "ReplicaConfig.hpp"
#include "Replica.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "InMemoryDBClient.h"
#include "KeyfileIOUtils.hpp"

using namespace bftEngine;
using namespace bftEngine::SimpleBlockchainStateTransfer;

namespace SimpleKVBC {

struct blockEntry {
  int16_t keyOffset;
  int16_t keySize;
  int16_t valOffset;
  int16_t valSize;
};

struct blockHeader {
  int16_t numberOfElements;
  StateTransferDigest prevBlockDigest;
  blockEntry entries[1];  // n>0 entries
};

Status ReplicaImp::start() {
  if (m_currentRepStatus != RepStatus::Ready)
    return Status::IllegalOperation("todo");

  m_currentRepStatus = RepStatus::Starting;

  Status s = m_bcDbAdapter->getDb()->init();

  if (!s.ok()) {
    return s;
  }

  m_currentRepStatus = RepStatus::Running;

  m_replica->start();

  return Status::OK();
}

Status ReplicaImp::stop() {
  return Status::IllegalOperation("NotImplemented");

  // m_bcDbAdapter->getDb()->close();
  // return Status::OK();
}

ReplicaImp::RepStatus ReplicaImp::getReplicaStatus() const {
  return m_currentRepStatus;
}

Status ReplicaImp::get(Slice key, Slice& outValue) const {
  // TODO(GG): check legality of operation (the method should be invocked from
  // the replica's internal thread)
  BlockId dummy;
  return getInternal(lastBlock, key, outValue, dummy);
}

Status ReplicaImp::get(BlockId readVersion,
                       Slice key,
                       Slice& outValue,
                       BlockId& outBlock) const {
  // TODO(GG): check legality of operation (the method should be invocked from
  // the replica's internal thread)
  return getInternal(readVersion, key, outValue, outBlock);
}

BlockId ReplicaImp::getLastBlock() const { return lastBlock; }

Status ReplicaImp::getBlockData(BlockId blockId,
                                SetOfKeyValuePairs& outBlockData) const {
  // TODO(GG): check legality of operation (the method should be invocked from
  // the replica's internal thread)

  Slice block = getBlockInternal(blockId);

  if (block.size == 0) return Status::NotFound("");

  outBlockData = fetchBlockData(block);

  return Status::OK();
}

Status ReplicaImp::mayHaveConflictBetween(Slice key,
                                          BlockId fromBlock,
                                          BlockId toBlock,
                                          bool& outRes) const {
  // TODO(GG): add assert or print warning if fromBlock==0 (all keys have a
  // conflict in block 0)

  outRes = true;  // we conservatively assume that we have a conflict

  Slice dummy;
  BlockId block = 0;
  Status s = getInternal(toBlock, key, dummy, block);
  if (s.ok() && block < fromBlock) outRes = false;

  return s;
}

ILocalKeyValueStorageReadOnlyIterator* ReplicaImp::getSnapIterator() const {
  return m_InternalStorageWrapperForIdleMode.getSnapIterator();
}

Status ReplicaImp::freeSnapIterator(
    ILocalKeyValueStorageReadOnlyIterator* iter) const {
  return m_InternalStorageWrapperForIdleMode.freeSnapIterator(iter);
}

Status ReplicaImp::addBlock(const SetOfKeyValuePairs& updates,
                            BlockId& outBlockId) {
  // TODO(GG): check legality of operation (the method should be invocked from
  // the replica's internal thread)

  return addBlockInternal(updates, outBlockId);
}

uint64_t ReplicaImp::getLastReachableBlockNum() {
  return m_bcDbAdapter->getLastReachableBlock();
}

uint64_t ReplicaImp::getLastBlockNum() { return m_bcDbAdapter->getLastBlock(); }

bool ReplicaImp::hasBlock(uint64_t blockId) {
  return m_bcDbAdapter->hasBlockId(blockId);
}

bool ReplicaImp::getBlock(uint64_t blockId,
                          char* outBlock,
                          uint32_t* outBlockSize) {
  bool found = false;
  Slice blockRaw;
  Status s = m_bcDbAdapter->getBlockById(blockId, blockRaw, found);

  if (s.ok()) {
    memcpy(outBlock, blockRaw.data, blockRaw.size);
    *outBlockSize = blockRaw.size;
    return true;
  } else {
    *outBlockSize = 0;
    return false;
  }
}

bool ReplicaImp::getPrevDigestFromBlock(
    uint64_t blockId, StateTransferDigest* outPrevBlockDigest) {
  bool found = false;
  Slice blockRaw;
  Status s = m_bcDbAdapter->getBlockById(blockId, blockRaw, found);

  if (s.ok()) {
    blockHeader* header = (blockHeader*)blockRaw.data;
    memcpy(outPrevBlockDigest,
           &header->prevBlockDigest,
           sizeof(StateTransferDigest));
    return true;
  } else {
    return false;
  }
}

bool ReplicaImp::putBlock(uint64_t blockId, char* block, uint32_t blockSize) {
  Slice b(block, blockSize);
  insertBlockInternal(blockId, b);
  return true;
}

ReplicaImp::ReplicaImp()
    : m_running(false), m_InternalStorageWrapperForIdleMode(this) {
  m_currentRepStatus = RepStatus::Ready;
  lastBlock = 0;
}

ReplicaImp::~ReplicaImp() {}

Status ReplicaImp::addBlockInternal(const SetOfKeyValuePairs& updates,
                                    BlockId& outBlockId) {
  StateTransferDigest digestOfPrev;

  if (lastBlock == 0) {
    memset((char*)&digestOfPrev, 0, sizeof(digestOfPrev));
  } else {
    bool found = false;
    Slice prevBlock;
    Status s = m_bcDbAdapter->getBlockById(lastBlock, prevBlock, found);
    assert(s.ok() && found);

    computeBlockDigest(
        lastBlock, prevBlock.data, prevBlock.size, &digestOfPrev);
  }

  BlockId block = lastBlock + 1;
  SetOfKeyValuePairs updatesInNewBlock;

  Slice blockRaw =
      createBlockFromUpdates(updates, updatesInNewBlock, digestOfPrev);

  // TODO(GG): free memory if we can't add the block

  if (blockRaw.size == 0) {
    return Status::IllegalBlockSize("Block is empty");
  }

  if (blockRaw.size > maxBlockSize) {
    return Status::IllegalBlockSize("Block is too big");
  }

  Status s = m_bcDbAdapter->addBlock(block, blockRaw);

  if (!s.ok()) {
    assert(false);  // in this version we assume that addBlock will never fail
    return s;
  }
  // blocks[block] = blockRaw;
  lastBlock++;

  for (SetOfKeyValuePairs::iterator it = updatesInNewBlock.begin();
       it != updatesInNewBlock.end();
       ++it) {
    const KeyValuePair& kvPair = *it;
    Status s = m_bcDbAdapter->updateKey(kvPair.first, block, kvPair.second);
    if (!s.ok()) {
      assert(
          false);  // in this version we assume that updateKey will never fail
      return s;
    }
    // map[kp] = kvPair.second;
  }

  outBlockId = block;

  return Status::OK();
}

Status ReplicaImp::getInternal(BlockId readVersion,
                               Slice key,
                               Slice& outValue,
                               BlockId& outBlock) const {
  Status s =
      m_bcDbAdapter->getKeyByReadVersion(readVersion, key, outValue, outBlock);
  if (!s.ok()) {
    // assert(false && "Failed to get key " << sliceToString(key) << " by read
    // version " << readVersion);
    return s;
  }

  return Status::OK();
}

void ReplicaImp::insertBlockInternal(BlockId blockId, Slice block) {
  if (blockId > lastBlock) {
    lastBlock = blockId;
  }

  bool found = false;
  Slice blockRaw;
  Status s = m_bcDbAdapter->getBlockById(blockId, blockRaw, found);
  if (!s.ok()) {
    // the replica is corrupted !
    // TODO(GG): what do we want to do now ?
    printf("replica may be corrupted\n\n");
    exit(1);  // TODO(GG)
  }

  if (found &&
      blockRaw.size > 0)  // if we already have a block with the same ID
  {
    if (blockRaw.size != block.size ||
        memcmp(blockRaw.data, block.data, block.size)) {
      fprintf(stderr, "replica is corrupted !");
      // the replica is corrupted !
      // TODO(GG)
      return;
    }
  } else {
    if (block.size > 0) {
      char* b = new char[block.size];
      memcpy(b, block.data, block.size);

      const char* begin = b;
      blockHeader* header = (blockHeader*)begin;

      for (int16_t i = 0; i < header->numberOfElements; i++) {
        const char* key = begin + header->entries[i].keyOffset;
        const int16_t keyLen = header->entries[i].keySize;
        const char* val = begin + header->entries[i].valOffset;
        const int16_t valLen = header->entries[i].valSize;

        const Slice keySlice(key, keyLen);
        const Slice valSlice(val, valLen);

        const KeyIDPair pk(keySlice, blockId);

        Status s = m_bcDbAdapter->updateKey(pk.key, pk.blockId, valSlice);
        if (!s.ok()) {
          printf("Failed to update key");  // in this version we assume that
                                           // addBlock will never fail
          exit(1);
        }
        // map[pk] = valSlice;
      }
      Slice newBlock(b, block.size);
      Status s = m_bcDbAdapter->addBlock(blockId, newBlock);
      if (!s.ok()) {
        printf("Failed to add block");  // in this version we assume that
                                        // addBlock will never fail
        exit(1);
      }
    } else {
      assert(0);  // illegal block size
    }
  }
}

Slice ReplicaImp::getBlockInternal(BlockId blockId) const {
  assert(blockId <= lastBlock);
  Slice retVal;

  bool found;
  Status s = m_bcDbAdapter->getBlockById(blockId, retVal, found);
  if (!s.ok()) {
    printf(
        "An error occurred in get block");  // TODO(SG): To do something smarter
    return Slice();
  }

  // std::map<BlockId, Slice>::const_iterator it = blocks.find(blockId);
  if (!found) {
    return Slice();
  } else {
    return retVal;
  }
}

bool ReplicaImp::executeCommand(uint16_t clientId,
                                bool readOnly,
                                uint32_t requestSize,
                                const char* request,
                                uint32_t maxReplySize,
                                char* outReply,
                                uint32_t& outActualReplySize) {
  // TODO(GG): verify command .....

  Slice cmdContent(request, requestSize);
  if (readOnly) {
    size_t replySize = 0;
    bool ret =
        m_cmdHandler->executeReadOnlyCommand(cmdContent,
                                             *this,
                                             maxReplySize,
                                             outReply,
                                             replySize);  // TODO(GG): ret vals
    outActualReplySize = replySize;
    return ret;
  } else {
    size_t replySize = 0;
    bool ret = m_cmdHandler->executeCommand(cmdContent,
                                            *this,
                                            *this,
                                            maxReplySize,
                                            outReply,
                                            replySize);  // TODO(GG): ret vals
    outActualReplySize = replySize;
    return ret;
  }
}

ReplicaImp::StorageWrapperForIdleMode::StorageWrapperForIdleMode(
    const ReplicaImp* r)
    : rep(r) {}

Status ReplicaImp::StorageWrapperForIdleMode::get(Slice key,
                                                  Slice& outValue) const {
  if (rep->getReplicaStatus() != IReplica::RepStatus::Ready)
    return Status::IllegalOperation("");

  return rep->get(key, outValue);
}

Status ReplicaImp::StorageWrapperForIdleMode::get(BlockId readVersion,
                                                  Slice key,
                                                  Slice& outValue,
                                                  BlockId& outBlock) const {
  if (rep->getReplicaStatus() != IReplica::RepStatus::Ready)
    return Status::IllegalOperation("");

  return rep->get(readVersion, key, outValue, outBlock);
}

BlockId ReplicaImp::StorageWrapperForIdleMode::getLastBlock() const {
  return rep->getLastBlock();
}

Status ReplicaImp::StorageWrapperForIdleMode::getBlockData(
    BlockId blockId, SetOfKeyValuePairs& outBlockData) const {
  if (rep->getReplicaStatus() != IReplica::RepStatus::Ready)
    return Status::IllegalOperation("");

  Slice block = rep->getBlockInternal(blockId);

  if (block.size == 0) return Status::NotFound("todo");

  outBlockData = ReplicaImp::fetchBlockData(block);

  return Status::OK();
}

Status ReplicaImp::StorageWrapperForIdleMode::mayHaveConflictBetween(
    Slice key, BlockId fromBlock, BlockId toBlock, bool& outRes) const {
  outRes = true;

  Slice dummy;
  BlockId block = 0;
  Status s = rep->getInternal(toBlock, key, dummy, block);
  if (s.ok() && block < fromBlock) outRes = false;

  return s;
}

ILocalKeyValueStorageReadOnlyIterator*
ReplicaImp::StorageWrapperForIdleMode::getSnapIterator() const {
  return new StorageIterator(this->rep);
}

Status ReplicaImp::StorageWrapperForIdleMode::freeSnapIterator(
    ILocalKeyValueStorageReadOnlyIterator* iter) const {
  if (iter == NULL) {
    return Status::InvalidArgument("Invalid iterator");
  }

  StorageIterator* storageIter = (StorageIterator*)iter;
  Status s = storageIter->freeInternalIterator();
  delete storageIter;
  return s;
}

ReplicaImp::StorageIterator::StorageIterator(const ReplicaImp* r) : rep(r) {
  m_iter = r->getBcDbAdapter()->getIterator();
  m_currentBlock = r->getLastBlock();
}

KeyValuePair ReplicaImp::StorageIterator::first(BlockId readVersion,
                                                BlockId& actualVersion,
                                                bool& isEnd) {
  Key key;
  Value value;
  Status s = rep->getBcDbAdapter()->first(
      m_iter, readVersion, actualVersion, isEnd, key, value);
  if (s.IsNotFound()) {
    isEnd = true;
    m_current = KeyValuePair();
    return m_current;
  }

  if (!s.ok()) {
    // assert(false && "Failed to get first");
    exit(1);
  }

  m_isEnd = isEnd;
  m_current = KeyValuePair(key, value);

  return m_current;
}

KeyValuePair ReplicaImp::StorageIterator::seekAtLeast(BlockId readVersion,
                                                      Key key,
                                                      BlockId& actualVersion,
                                                      bool& isEnd) {
  Key actualKey;
  Value value;
  Status s = rep->getBcDbAdapter()->seekAtLeast(
      m_iter, key, readVersion, actualVersion, actualKey, value, isEnd);
  if (s.IsNotFound()) {
    isEnd = true;
    m_current = KeyValuePair();
    return m_current;
  }

  if (!s.ok()) {
    // assert(false && "Failed to seek at least");
    exit(1);
  }

  m_isEnd = isEnd;
  m_current = KeyValuePair(actualKey, value);
  return m_current;
}

/* TODO(SG): There is a question mark regarding on these APIs. Suppose I have
(k0,2), (k1,7), (k2,4) and I request next(k0,5). Do we return end() (because k1
cannot be returned), or do we return k2? I implemented the second choice, as it
makes better sense. The world in Block 5 did not include k1, that's perfectly
OK.
*/
// Note: key,readVersion must exist in map already
KeyValuePair ReplicaImp::StorageIterator::next(BlockId readVersion,
                                               Key key,
                                               BlockId& actualVersion,
                                               bool& isEnd) {
  Key nextKey;
  Value nextValue;
  Status s = rep->getBcDbAdapter()->next(
      m_iter, readVersion, nextKey, nextValue, actualVersion, isEnd);
  if (s.IsNotFound()) {
    isEnd = true;
    m_current = KeyValuePair();
    return m_current;
  }

  if (!s.ok()) {
    // assert(false && "Failed to get next");
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
  if (!s.ok()) {
    // assert(false && "Failed to get current");
    exit(1);
  }

  m_current = KeyValuePair(key, value);
  return m_current;
}

bool ReplicaImp::StorageIterator::isEnd() {
  bool isEnd;
  Status s = rep->getBcDbAdapter()->isEnd(m_iter, isEnd);
  if (!s.ok()) {
    // assert(false && "Failed to get current");
    exit(1);
  }

  return isEnd;
}

Status ReplicaImp::StorageIterator::freeInternalIterator() {
  return rep->getBcDbAdapter()->freeIterator(m_iter);
}

Slice ReplicaImp::createBlockFromUpdates(
    const SetOfKeyValuePairs& updates,
    SetOfKeyValuePairs& outUpdatesInNewBlock,
    StateTransferDigest digestOfPrev) {
  // TODO(GG): overflow handling ....

  assert(outUpdatesInNewBlock.size() == 0);

  int16_t blockBodySize = 0;
  int16_t numOfElemens = 0;
  for (auto it = updates.begin(); it != updates.end(); ++it) {
    const KeyValuePair& kvPair = KeyValuePair(it->first, it->second);
    numOfElemens++;
    blockBodySize += (int16_t)(kvPair.first.size + kvPair.second.size);
  }

  const int16_t headerSize = sizeof(blockHeader) - sizeof(blockEntry) +
                             sizeof(blockEntry) * (numOfElemens);
  const int16_t blockSize = headerSize + blockBodySize;

  try {
    char* blockBuffer = new char[blockSize];
    memset(blockBuffer, 0, blockSize);

    blockHeader* header = (blockHeader*)blockBuffer;

    int16_t idx = 0;
    header->numberOfElements = numOfElemens;
    header->prevBlockDigest = digestOfPrev;

    int16_t currentOffset = headerSize;
    for (auto it = updates.begin(); it != updates.end(); ++it) {
      const KeyValuePair& kvPair = *it;
      // key
      header->entries[idx].keyOffset = currentOffset;
      header->entries[idx].keySize = (int16_t)kvPair.first.size;
      memcpy(blockBuffer + currentOffset, kvPair.first.data, kvPair.first.size);
      Slice newKey(blockBuffer + currentOffset, kvPair.first.size);

      currentOffset += (int16_t)kvPair.first.size;

      // value
      header->entries[idx].valOffset = currentOffset;
      header->entries[idx].valSize = (int16_t)kvPair.second.size;
      memcpy(
          blockBuffer + currentOffset, kvPair.second.data, kvPair.second.size);
      Slice newVal(blockBuffer + currentOffset, kvPair.second.size);

      currentOffset += (int16_t)kvPair.second.size;

      // add to outUpdatesInNewBlock
      KeyValuePair newKVPair(newKey, newVal);
      outUpdatesInNewBlock.insert(newKVPair);

      idx++;
    }
    assert(idx == numOfElemens);
    assert(currentOffset == blockSize);

    return Slice(blockBuffer, blockSize);
  } catch (std::bad_alloc&) {
    // assert(false && "Failed to alloc size " << blockSize << ", error: " <<
    // ba.what());
    char* emptyBlockBuffer = new char[1];
    memset(emptyBlockBuffer, 0, 1);
    return Slice(emptyBlockBuffer, 1);
  }
}

SetOfKeyValuePairs ReplicaImp::fetchBlockData(Slice block) {
  SetOfKeyValuePairs retVal;

  if (block.size > 0) {
    const char* begin = block.data;
    blockHeader* header = (blockHeader*)begin;

    for (int16_t i = 0; i < header->numberOfElements; i++) {
      const char* key = begin + header->entries[i].keyOffset;
      const int16_t keyLen = header->entries[i].keySize;
      const char* val = begin + header->entries[i].valOffset;
      const int16_t valLen = header->entries[i].valSize;

      Slice keySlice(key, keyLen);
      Slice valSlice(val, valLen);

      KeyValuePair kv(key, val);

      retVal.insert(kv);
    }
  }

  return retVal;
}

int RequestsHandlerImp::execute(uint16_t clientId,
                                bool readOnly,
                                uint32_t requestSize,
                                const char* request,
                                uint32_t maxReplySize,
                                char* outReply,
                                uint32_t& outActualReplySize) {
  ReplicaImp* r = m_Executor;

  bool ret = r->executeCommand(clientId,
                               readOnly,
                               requestSize,
                               request,
                               maxReplySize,
                               outReply,
                               outActualReplySize);
  return ret ? 0 : 1;
}

IReplica* createReplica(const ReplicaConfig& c,
                        bftEngine::ICommunication* comm,
                        ICommandsHandler* _cmdHandler) {
  IDBClient* _db = new InMemoryDBClient();

  ReplicaImp* r = new ReplicaImp();

  bftEngine::ReplicaConfig replicaConfig;

  std::ifstream keyfile(c.pathOfKeysfile);
  if (!keyfile.is_open()) {
    throw std::runtime_error("Unable to read replica keyfile.");
  }

  bool succ = inputReplicaKeyfile(keyfile, c.pathOfKeysfile, replicaConfig);
  if (!succ) throw std::runtime_error("Unable to parse replica keyfile.");

  if (replicaConfig.replicaId != c.replicaId)
    throw std::runtime_error("Unexpected replica ID in security keys file");

  if (replicaConfig.fVal != c.fVal)
    throw std::runtime_error("Unexpected F value in security keys file");

  if (replicaConfig.cVal != c.cVal)
    throw std::runtime_error("Unexpected C value in security keys file");

  replicaConfig.numOfClientProxies = c.numOfClientProxies;
  replicaConfig.statusReportTimerMillisec = c.statusReportTimerMillisec;
  ;
  replicaConfig.concurrencyLevel = c.concurrencyLevel;
  ;
  replicaConfig.autoViewChangeEnabled = c.autoViewChangeEnabled;
  ;
  replicaConfig.viewChangeTimerMillisec = c.viewChangeTimerMillisec;

  bftEngine::SimpleBlockchainStateTransfer::Config stConfig;

  stConfig.myReplicaId = replicaConfig.replicaId;
  stConfig.pedanticChecks = true;
  stConfig.fVal = replicaConfig.fVal;
  stConfig.cVal = replicaConfig.cVal;
  stConfig.maxBlockSize = c.maxBlockSize;

  bftEngine::IStateTransfer* stateTransfer =
      bftEngine::SimpleBlockchainStateTransfer::create(stConfig, r, false);

  RequestsHandlerImp* reqHandler = new RequestsHandlerImp();
  reqHandler->m_Executor = r;

  Replica* replica = Replica::createNewReplica(
      &replicaConfig, reqHandler, stateTransfer, comm, nullptr);

  r->m_replica = replica;
  r->maxBlockSize = c.maxBlockSize;

  BlockchainDBAdapter* dbAdapter = new BlockchainDBAdapter(_db);
  r->m_bcDbAdapter = dbAdapter;
  r->m_cmdHandler = _cmdHandler;

  return r;
}

}  // namespace SimpleKVBC
