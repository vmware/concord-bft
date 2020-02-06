#pragma once

#include <memory>
#include <string>
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "blockchain/block.h"
#include "blockchain/db_adapter.h"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "status.hpp"
#include "storage/db_interface.h"
#include "object_store/object_store_client.hpp"

const std::string kLastBlockIdKey = "1";
const std::string kLastReachableBlockIdKey = "2";

namespace concord {
namespace consensus {

class RoRAppState
    : public bftEngine::SimpleBlockchainStateTransfer::IAppState {
 public:
  virtual ~RoRAppState();

  RoRAppState(
      std::shared_ptr<concord::storage::blockchain::DBKeyManipulator>
          keyManipulator,
      std::shared_ptr<concord::storage::IDBClient> localClient,
      std::shared_ptr<concord::storage::IDBClient> objectStoreClient);

  // returns true IFF block blockId exists
  // (i.e. the block is stored in the application/storage layer).
  virtual bool hasBlock(uint64_t blockId) override;

  // If block blockId exists, then its content is returned via the arguments
  // outBlock and outBlockSize. Returns true IFF block blockId exists.
  virtual bool getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) override;

  // If block blockId exists, then the digest of block blockId-1 is returned via
  // the argument outPrevBlockDigest. Returns true IFF block blockId exists.
  virtual bool getPrevDigestFromBlock(
    uint64_t blockId, bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest *outPrevBlockDigest) override;
    
  // adds block
  // blockId   - the block number
  // block     - pointer to a buffer that contains the new block
  // blockSize - the size of the new block
  virtual bool putBlock(uint64_t blockId, char *block, uint32_t blockSize) override;
  
  // returns the maximal block number n such that all blocks 1 <= i <= n exist.
  // if block 1 does not exist, returns 0.
  virtual uint64_t getLastReachableBlockNum() override;
  
  // returns the maximum block number that is currently stored in
  // the application/storage layer.
  virtual uint64_t getLastBlockNum() override;

  virtual void wait() override;

  // When the state is updated by the application, getLastReachableBlockNum()
  // and getLastBlockNum() should always return the same block number.
  // When that state transfer module is updating the state, then these methods
  // may return different block numbers.State

 private:
  std::shared_ptr<bftEngine::SimpleBlockchainStateTransfer::IAppState> pImpl_ =
      nullptr;
};
}  // namespace consensus
}  // namespace concord