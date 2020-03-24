// Copyright 2018 VMware, all rights reserved
//
// Contains functionality for working with composite database keys and
// using them to perform basic blockchain operations.

#pragma once

#include "base_db_adapter.h"
#include "db_types.h"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "sliver.hpp"
#include "storage/db_interface.h"

#include <memory>

namespace concord {
namespace storage {
namespace blockchain {
inline namespace v1DirectKeyValue {
class DBKeyComparator : public IDBClient::IKeyComparator {
 public:
  virtual int composedKeyComparison(const char *_a_data,
                                    size_t _a_length,
                                    const char *_b_data,
                                    size_t _b_length) override;

 private:
  concordlogger::Logger &logger() const {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.storage.blockchain.DBKeyComparator");
    return logger_;
  }
};

class DBKeyManipulator : public DBKeyManipulatorBase {
 public:
  static concordUtils::Sliver genBlockDbKey(BlockId _blockId);
  static concordUtils::Sliver genDataDbKey(const concord::kvbc::Key &_key, BlockId _blockId);
  static BlockId extractBlockIdFromKey(const concord::kvbc::Key &_key);
  static BlockId extractBlockIdFromKey(const char *_key_data, size_t _key_length);
  static detail::EDBKeyType extractTypeFromKey(const concord::kvbc::Key &_key);
  static detail::EDBKeyType extractTypeFromKey(const char *_key_data);
  static ObjectId extractObjectIdFromKey(const concord::kvbc::Key &_key);
  static ObjectId extractObjectIdFromKey(const char *_key_data, size_t _key_length);
  static concordUtils::Sliver extractKeyFromKeyComposedWithBlockId(const concord::kvbc::Key &_composedKey);
  static int compareKeyPartOfComposedKey(const char *a_data, size_t a_length, const char *b_data, size_t b_length);
  static concordUtils::Sliver extractKeyFromMetadataKey(const concord::kvbc::Key &_composedKey);
  static bool isKeyContainBlockId(const concord::kvbc::Key &_composedKey);
  static concord::kvbc::KeyValuePair composedToSimple(concord::kvbc::KeyValuePair _p);
  static concordUtils::Sliver generateMetadataKey(ObjectId objectId);
  static concordUtils::Sliver generateStateTransferKey(ObjectId objectId);
  static concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid);
  static concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageStaticKey(uint32_t pageid, uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageid, uint64_t chkpt);
  static uint64_t extractCheckPointFromKey(const char *_key_data, size_t _key_length);
  static std::pair<uint32_t, uint64_t> extractPageIdAndCheckpointFromKey(const char *_key_data, size_t _key_length);

 private:
  static concordUtils::Sliver genDbKey(detail::EDBKeyType _type, const concord::kvbc::Key &_key, BlockId _blockId);
  static Sliver generateReservedPageKey(detail::EDBKeyType, uint32_t pageid, uint64_t chkpt);
  static bool copyToAndAdvance(char *_buf, size_t *_offset, size_t _maxOffset, char *_src, size_t _srcSize);
};

class DBAdapter : public DBAdapterBase {
 public:
  DBAdapter(const std::shared_ptr<IDBClient> &db, bool readOnly = false);

  // Adds a block from a set of key/value pairs and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key
  // - calculating and filling in the parent digest.
  // Typically called by the application when adding a new block.
  Status addBlock(const concord::kvbc::SetOfKeyValuePairs &kv, BlockId blockId);

  // Adds a block from its raw representation and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key.
  // Typically called by state transfer when a block is received and needs to be added.
  Status addBlock(const concordUtils::Sliver &block, BlockId blockId);

  Status getKeyByReadVersion(BlockId readVersion,
                             const concordUtils::Sliver &key,
                             concordUtils::Sliver &outValue,
                             BlockId &outBlock) const;

  Status getBlockById(BlockId _blockId, concordUtils::Sliver &_blockRaw, bool &_found) const;

  Status first(IDBClient::IDBClientIterator *iter,
               BlockId readVersion,
               OUT BlockId &actualVersion,
               OUT bool &isEnd,
               OUT concordUtils::Sliver &_key,
               OUT concordUtils::Sliver &_value);
  Status seekAtLeast(IDBClient::IDBClientIterator *iter,
                     const concordUtils::Sliver &_searchKey,
                     BlockId _readVersion,
                     OUT BlockId &_actualVersion,
                     OUT Sliver &_key,
                     OUT Sliver &_value,
                     OUT bool &_isEnd);
  Status next(IDBClient::IDBClientIterator *iter,
              BlockId _readVersion,
              OUT concordUtils::Sliver &_key,
              OUT concordUtils::Sliver &_value,
              OUT BlockId &_actualVersion,
              OUT bool &_isEnd);

  Status getCurrent(IDBClient::IDBClientIterator *iter,
                    OUT concordUtils::Sliver &_key,
                    OUT concordUtils::Sliver &_value);
  Status isEnd(IDBClient::IDBClientIterator *iter, OUT bool &_isEnd);

  Status delKey(const concordUtils::Sliver &_key, BlockId _blockID);
  Status delBlock(BlockId _blockId);
  void deleteBlockAndItsKeys(BlockId blockId);

  BlockId getLatestBlock() const;
  BlockId getLastReachableBlock() const;

 private:
  Status addBlockAndUpdateMultiKey(const concord::kvbc::SetOfKeyValuePairs &_kvMap,
                                   BlockId _block,
                                   const concordUtils::Sliver &_blockRaw);
};

}  // namespace v1DirectKeyValue
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
