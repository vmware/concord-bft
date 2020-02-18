#include "ror_app_state.hpp"
#include <cassert>
#include <cstring>
#include <unordered_map>
#include <vector>
#include "hash_defs.h"

using namespace concordUtils;
using namespace bftEngine::SimpleBlockchainStateTransfer;
using namespace concord::storage;

class RoRAppStateImpl : public IAppState {
 public:
  RoRAppStateImpl(std::shared_ptr<concord::storage::blockchain::DBKeyManipulator> keyManipulator,
                  std::shared_ptr<concord::storage::IDBClient> localClient,
                  std::shared_ptr<concord::storage::IDBClient> objectStoreClient)
      : keyManipulator_{keyManipulator}, localClient_{localClient}, objectStoreClient_{objectStoreClient} {
    std::vector<Sliver> keys{lastBlockIdKey, lastReachableBlockIdKey};
    KeysVector outValues;
    Status s = localClient_->multiGet(keys, outValues);
    if (s.isOK()) {
      lastBlockId_ = *reinterpret_cast<const uint64_t *>(outValues[0].data());
      lastReachableBlockId_ = *reinterpret_cast<const uint64_t *>(outValues[1].data());
    } else if (s.isNotFound()) {
      LOG_INFO(logger_, "RoRAppStateImpl, first time start");
    } else {
      LOG_ERROR(logger_, "RoRAppStateImpl, can't read local metadata, status: " << s);
      exit(-1);
    }
  }

  virtual bool hasBlock(uint64_t blockId) override {
    Sliver key = keyManipulator_->genBlockDbKey(blockId);
    Status res = Status::OK();
    // for using in mem db for tests, we must first ty to cast to ObjectStoreClient
    auto ptr = std::dynamic_pointer_cast<concord::storage::ObjectStoreClient>(objectStoreClient_);
    if (ptr)
      res = ptr->object_exists(key);
    else {  // and if we are in test, just use the interface
      Sliver v;
      res = objectStoreClient_->get(key, v);
    }

    LOG_DEBUG(logger_, "hasBlock, blockId: " << blockId << ", result: " << res.toString());
    return res.isOK();
  }

  virtual bool getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) override {
    throw std::logic_error("this method is not implemented in RoRAppStateImpl");
  }

  virtual bool getPrevDigestFromBlock(uint64_t blockId, StateTransferDigest *outPrevBlockDigest) override {
    using namespace concord::storage::blockchain;
    assert(blockId > 0);

    Sliver blockData;
    Sliver key = keyManipulator_->genBlockDbKey(blockId);
    auto s = objectStoreClient_->get(key, blockData);
    if (!s.isOK()) {
      LOG_ERROR(logger_, "objectStoreClient_->get returned " << s);
      return false;
    }

    LOG_DEBUG(logger_, "getPrevDigestFromBlock, blockId: " << blockId);

    assert(outPrevBlockDigest);
    auto parentDigest = concord::storage::blockchain::block::getParentDigest(blockData);
    assert(outPrevBlockDigest);
    memcpy(outPrevBlockDigest, parentDigest, BLOCK_DIGEST_SIZE);
    return true;
  }

  virtual bool putBlock(uint64_t blockId, char *block, uint32_t blockSize) override {
    Sliver key = keyManipulator_->genBlockDbKey(blockId);
    Sliver value = Sliver::copy(block, blockSize);
    Status s = objectStoreClient_->put(key, value);
    if (s.isOK()) {
      if (blockId > lastBlockId_) {
        lastBlockId_ = blockId;
      }
      // when ST runs, blocks arrive in batches in reverse order. we need to
      // keep track on the "Gap" and to close it. Only when it is closed, the
      // last reachable block becomes the same as the last block
      if (blockId == lastReachableBlockId_ + 1) {
        lastReachableBlockId_ = lastBlockId_;
      }
    } else {
      LOG_ERROR(logger_, "putBlock, blockId: " << blockId << ", status: " << s);
      return false;
    }

    // if the local write fails but the Object store write was OK, we dont worry
    // because next time ST will write the same block again and in the worst
    // case the block in the Object store will be overwritten.
    using namespace concord::storage;
    using namespace concordUtils;

    SetOfKeyValuePairs meta;
    Sliver v1 = Sliver::copy((const char *)&lastBlockId_, sizeof(lastBlockId_));
    Sliver v2 = Sliver::copy((const char *)&lastReachableBlockId_, sizeof(lastReachableBlockId_));
    meta.insert(std::make_pair(lastBlockIdKey, v1));
    meta.insert(std::make_pair(lastReachableBlockIdKey, v2));

    s = localClient_->multiPut(meta);
    if (!s.isOK()) {
      LOG_FATAL(logger_, "putBlock, couldn't write local metadata, status: " << s);
      exit(-1);
    }

    LOG_DEBUG(logger_,
              "putBlock, blockId: " << blockId << ", status: " << s << ", lastBlockId: " << lastBlockId_
                                    << ", lastReachableBlockId: " << lastReachableBlockId_);
    return true;
  }

  virtual uint64_t getLastReachableBlockNum() override { return lastReachableBlockId_; }

  virtual uint64_t getLastBlockNum() override { return lastBlockId_; }

  virtual void wait() override {
    std::static_pointer_cast<concord::storage::ObjectStoreClient>(objectStoreClient_)->wait_for_storage();
  }

 private:
  uint64_t lastBlockId_ = 0;
  uint64_t lastReachableBlockId_ = 0;

  Sliver lastBlockIdKey = Sliver::copy(kLastBlockIdKey.c_str(), kLastBlockIdKey.size());
  Sliver lastReachableBlockIdKey = Sliver::copy(kLastReachableBlockIdKey.c_str(), kLastReachableBlockIdKey.size());

  std::shared_ptr<concord::storage::blockchain::DBKeyManipulator> keyManipulator_ = nullptr;
  std::shared_ptr<concord::storage::IDBClient> localClient_ = nullptr;
  std::shared_ptr<concord::storage::IDBClient> objectStoreClient_ = nullptr;
  concordlogger::Logger logger_ = concordlogger::Log::getLogger("RoRAppStateImpl");
};

concord::consensus::RoRAppState::RoRAppState(
    std::shared_ptr<concord::storage::blockchain::DBKeyManipulator> keyManipulator,
    std::shared_ptr<concord::storage::IDBClient> localClient,
    std::shared_ptr<concord::storage::IDBClient> objectStoreClient) {
  pImpl_ = std::make_shared<RoRAppStateImpl>(keyManipulator, localClient, objectStoreClient);
}

concord::consensus::RoRAppState::~RoRAppState() {}

bool concord::consensus::RoRAppState::hasBlock(uint64_t blockId) { return pImpl_->hasBlock(blockId); }

bool concord::consensus::RoRAppState::getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) {
  return pImpl_->getBlock(blockId, outBlock, outBlockSize);
}

bool concord::consensus::RoRAppState::getPrevDigestFromBlock(
    uint64_t blockId, bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest *outPrevBlockDigest) {
  return pImpl_->getPrevDigestFromBlock(blockId, outPrevBlockDigest);
}

bool concord::consensus::RoRAppState::putBlock(uint64_t blockId, char *block, uint32_t blockSize) {
  return pImpl_->putBlock(blockId, block, blockSize);
}

uint64_t concord::consensus::RoRAppState::getLastReachableBlockNum() { return pImpl_->getLastReachableBlockNum(); }

uint64_t concord::consensus::RoRAppState::getLastBlockNum() { return pImpl_->getLastBlockNum(); }

void concord::consensus::RoRAppState::wait() { return pImpl_->wait(); }