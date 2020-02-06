// Copyright 2018 VMware, all rights reserved
//
// Translation between BlockAppender/ILocalkeyValueStorage* to the underlying
// database.

#pragma once

#include "Logger.hpp"

#include "kv_types.hpp"
#include "sliver.hpp"
#include "blockchain/db_types.h"
#include "storage/db_interface.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <utility>

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

class DBKeyManipulator {
 public:
  static concordUtils::Sliver genBlockDbKey(BlockId _blockId);
  static concordUtils::Sliver genDataDbKey(const concordUtils::Key &_key, BlockId _blockId);
  static detail::EDBKeyType extractTypeFromKey(const concordUtils::Key &_key);
  static detail::EDBKeyType extractTypeFromKey(const char *_key_data);
  static BlockId extractBlockIdFromKey(const concordUtils::Key &_key);
  static BlockId extractBlockIdFromKey(const char *_key_data, size_t _key_length);
  static ObjectId extractObjectIdFromKey(const concordUtils::Key &_key);
  static ObjectId extractObjectIdFromKey(const char *_key_data, size_t _key_length);
  static concordUtils::Sliver extractKeyFromKeyComposedWithBlockId(const concordUtils::Key &_composedKey);
  static int compareKeyPartOfComposedKey(const char *a_data, size_t a_length, const char *b_data, size_t b_length);
  static concordUtils::Sliver extractKeyFromMetadataKey(const concordUtils::Key &_composedKey);
  static bool isKeyContainBlockId(const concordUtils::Key &_composedKey);
  static concordUtils::KeyValuePair composedToSimple(concordUtils::KeyValuePair _p);
  static concordUtils::Sliver generateMetadataKey(ObjectId objectId);
  static concordUtils::Sliver generateStateTransferKey(ObjectId objectId);
  static concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid);
  static concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageStaticKey(uint32_t pageid, uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageid, uint64_t chkpt);
  static uint64_t extractCheckPointFromKey(const char *_key_data, size_t _key_length);
  static std::pair<uint32_t, uint64_t> extractPageIdAndCheckpointFromKey(const char *_key_data, size_t _key_length);

 private:
  static concordUtils::Sliver genDbKey(detail::EDBKeyType _type, const concordUtils::Key &_key, BlockId _blockId);
  static Sliver generateReservedPageKey(detail::EDBKeyType, uint32_t pageid, uint64_t chkpt);
  static bool copyToAndAdvance(char *_buf, size_t *_offset, size_t _maxOffset, char *_src, size_t _srcSize);

  static concordlogger::Logger &logger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.storage.blockchain.DBKeyManipulator");
    return logger_;
  }
};

class DBAdapter {
 public:
  explicit DBAdapter(IDBClient *db, bool readOnly = false);

  std::shared_ptr<IDBClient> getDb() { return db_; }

  Status addBlock(BlockId _blockId, const concordUtils::Sliver &_blockRaw);
  Status updateKey(const concordUtils::Key &_key, BlockId _block, concordUtils::Value _value);
  Status addBlockAndUpdateMultiKey(const SetOfKeyValuePairs &_kvMap,
                                   BlockId _block,
                                   const concordUtils::Sliver &_blockRaw);
  Status getKeyByReadVersion(BlockId readVersion,
                             const concordUtils::Sliver &key,
                             concordUtils::Sliver &outValue,
                             BlockId &outBlock) const;
  Status getBlockById(BlockId _blockId, concordUtils::Sliver &_blockRaw, bool &_found) const;

  IDBClient::IDBClientIterator *getIterator() { return db_->getIterator(); }

  Status freeIterator(IDBClient::IDBClientIterator *_iter) { return db_->freeIterator(_iter); }

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
  void monitor() const;

  BlockId getLatestBlock();
  BlockId getLastReachableBlock();

 private:
  concordlogger::Logger logger_;
  std::shared_ptr<IDBClient> db_;
  KeyValuePair m_current;
  bool m_isEnd;
};

}  // namespace v1DirectKeyValue
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
