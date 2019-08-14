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
  virtual int   composedKeyComparison(const Sliver&, const Sliver& ) override;

  Sliver        genDbKey(EDBKeyType _type, Key _key, BlockId _blockId);
  Sliver        genBlockDbKey(BlockId _blockId);
  Sliver        genDataDbKey(Key _key, BlockId _blockId);
  char          extractTypeFromKey(Key _key);
  BlockId       extractBlockIdFromKey(Key _key);
  ObjectId      extractObjectIdFromKey(Key _key);
  Sliver        extractKeyFromKeyComposedWithBlockId(Key _composedKey);
  Sliver        extractKeyFromMetadataKey(Key _composedKey);
  bool          isKeyContainBlockId(Key _composedKey);
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
  Status updateKey(Key _key, BlockId _block, Value _value);
  Status addBlockAndUpdateMultiKey(const SetOfKeyValuePairs &_kvMap, BlockId _block, Sliver _blockRaw);
  Status getKeyByReadVersion(BlockId readVersion, Sliver key, Sliver &outValue, BlockId &outBlock) const;
  Status getBlockById(BlockId _blockId, Sliver &_blockRaw, bool &_found) const;

  IDBClient::IDBClientIterator* getIterator() { return db_->getIterator(); }

  Status freeIterator(IDBClient::IDBClientIterator *_iter) { return db_->freeIterator(_iter); }

  Status first(IDBClient::IDBClientIterator *iter, BlockId readVersion, OUT BlockId &actualVersion, OUT bool &isEnd,
               OUT Sliver &_key, OUT Sliver &_value);
  Status seekAtLeast(IDBClient::IDBClientIterator *iter, Sliver _searchKey, BlockId _readVersion,
                     OUT BlockId &_actualVersion, OUT Sliver &_key, OUT Sliver &_value, OUT bool &_isEnd);
  Status next(IDBClient::IDBClientIterator *iter, BlockId _readVersion, OUT Sliver &_key, OUT Sliver &_value,
              OUT BlockId &_actualVersion, OUT bool &_isEnd);

  Status getCurrent(IDBClient::IDBClientIterator *iter, OUT Sliver &_key, OUT Sliver &_value);
  Status isEnd(IDBClient::IDBClientIterator *iter, OUT bool &_isEnd);

  Status delKey(Sliver _key, BlockId _blockID);
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
