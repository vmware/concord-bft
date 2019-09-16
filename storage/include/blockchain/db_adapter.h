// Copyright 2018 VMware, all rights reserved
//
// Translation between BlockAppender/ILocalkeyValueStorage* to the underlying
// database.

#pragma once

#include "Logger.hpp"

#include "sliver.hpp"
#include "blockchain/db_types.h"
#include "blockchain/db_interfaces.h"
#include "storage/db_interface.h"
#include <memory>
namespace concord {
namespace storage {
namespace blockchain {


class KeyManipulator: public IDBClient::IKeyManipulator{
 public:
  KeyManipulator():logger_(concordlogger::Log::getLogger("concord.storage.blockchain.KeyManipulator")){}
  virtual int   composedKeyComparison(const uint8_t* _a_data, size_t _a_length, const uint8_t* _b_data, size_t _b_length) override;

  Sliver        genDbKey(EDBKeyType _type, const Key& _key, BlockId _blockId);
  Sliver        genBlockDbKey(BlockId _blockId);
  Sliver        genDataDbKey(const Key& _key, BlockId _blockId);
  char          extractTypeFromKey(const Key& _key);
  char          extractTypeFromKey(const uint8_t* _key_data);
  BlockId       extractBlockIdFromKey(const Key& _key);
  BlockId       extractBlockIdFromKey(const uint8_t* _key_data, size_t _key_length);
  ObjectId      extractObjectIdFromKey(const Key& _key);
  ObjectId      extractObjectIdFromKey(const uint8_t* _key_data, size_t _key_length);
  Sliver        extractKeyFromKeyComposedWithBlockId(const Key& _composedKey);
  int           compareKeyPartOfComposedKey(const uint8_t *a_data, size_t a_length, const uint8_t *b_data, size_t b_length);
  Sliver        extractKeyFromMetadataKey(const Key& _composedKey);
  bool          isKeyContainBlockId(const Key& _composedKey);
  KeyValuePair  composedToSimple(KeyValuePair _p);
  static Sliver generateMetadataKey(ObjectId objectId);
 protected:

  static bool   copyToAndAdvance(uint8_t *_buf, size_t *_offset, size_t _maxOffset, uint8_t *_src, size_t _srcSize);


  concordlogger::Logger logger_;
};

class DBAdapter {
 public:
  explicit DBAdapter(IDBClient *db, bool readOnly = false);

  std::shared_ptr<IDBClient> getDb() { return db_; }

  Status addBlock(BlockId _blockId, Sliver _blockRaw);
  Status updateKey(const Key& _key, BlockId _block, Value _value);
  Status addBlockAndUpdateMultiKey(const SetOfKeyValuePairs &_kvMap, BlockId _block, Sliver _blockRaw);
  Status getKeyByReadVersion(BlockId readVersion, const Sliver& key, Sliver &outValue, BlockId &outBlock) const;
  Status getBlockById(BlockId _blockId, Sliver &_blockRaw, bool &_found) const;

  IDBClient::IDBClientIterator* getIterator() { return db_->getIterator(); }

  Status freeIterator(IDBClient::IDBClientIterator *_iter) { return db_->freeIterator(_iter); }

  Status first(IDBClient::IDBClientIterator *iter, BlockId readVersion, OUT BlockId &actualVersion, OUT bool &isEnd,
               OUT Sliver &_key, OUT Sliver &_value);
  Status seekAtLeast(IDBClient::IDBClientIterator *iter, const Sliver& _searchKey, BlockId _readVersion,
                     OUT BlockId &_actualVersion, OUT Sliver &_key, OUT Sliver &_value, OUT bool &_isEnd);
  Status next(IDBClient::IDBClientIterator *iter, BlockId _readVersion, OUT Sliver &_key, OUT Sliver &_value,
              OUT BlockId &_actualVersion, OUT bool &_isEnd);

  Status getCurrent(IDBClient::IDBClientIterator *iter, OUT Sliver &_key, OUT Sliver &_value);
  Status isEnd(IDBClient::IDBClientIterator *iter, OUT bool &_isEnd);

  Status delKey(const Sliver& _key, BlockId _blockID);
  Status delBlock(BlockId _blockId);
  void   deleteBlockAndItsKeys(BlockId blockId);
  void   monitor() const;

  BlockId getLatestBlock();
  BlockId getLastReachableBlock();

 private:
  concordlogger::Logger      logger_;
  std::shared_ptr<IDBClient> db_;
  std::shared_ptr<KeyManipulator> key_manipulator_;
  KeyValuePair m_current;
  bool m_isEnd;
};


}
}
}
