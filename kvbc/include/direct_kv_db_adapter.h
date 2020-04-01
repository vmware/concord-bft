// Copyright 2018 VMware, all rights reserved
//
// Contains functionality for working with composite database keys and
// using them to perform basic blockchain operations.

#pragma once

#include "base_db_adapter.h"
#include "kv_types.hpp"
#include "db_interfaces.h"
#include "Logger.hpp"

#include <memory>
#include "storage/key_manipulator.hpp"

namespace concord::kvbc {
inline namespace v1DirectKeyValue {
using concord::storage::detail::EDBKeyType;

class DBKeyComparator : public concord::storage::IDBClient::IKeyComparator {
 public:
  int composedKeyComparison(const char *_a_data, size_t _a_length, const char *_b_data, size_t _b_length) override;
  static concordlogger::Logger &logger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.kvbc.DBKeyComparator");
    return logger_;
  }
};

class DBKeyManipulatorBase : public storage::DBKeyManipulatorBase {
 protected:
  static concordUtils::Sliver genDbKey(EDBKeyType, const Key &, BlockId);
  static concordlogger::Logger &logger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.kvbc.DBKeyManipulator");
    return logger_;
  }
};

class KeyGenerator : public IDataKeyGenerator, public DBKeyManipulatorBase {
 public:
  concordUtils::Sliver blockKey(const BlockId &) const override;
  concordUtils::Sliver dataKey(const Key &, const BlockId &) const override;
};

class DBKeyManipulator : public DBKeyManipulatorBase {
 public:
  static BlockId extractBlockIdFromKey(const Key &_key);
  static BlockId extractBlockIdFromKey(const char *_key_data, size_t _key_length);
  static EDBKeyType extractTypeFromKey(const Key &_key);
  static EDBKeyType extractTypeFromKey(const char *_key_data);
  static storage::ObjectId extractObjectIdFromKey(const Key &_key);
  static storage::ObjectId extractObjectIdFromKey(const char *_key_data, size_t _key_length);
  static concordUtils::Sliver extractKeyFromKeyComposedWithBlockId(const Key &_composedKey);
  static int compareKeyPartOfComposedKey(const char *a_data, size_t a_length, const char *b_data, size_t b_length);
  static concordUtils::Sliver extractKeyFromMetadataKey(const Key &_composedKey);
  static bool isKeyContainBlockId(const Key &_composedKey);
  static KeyValuePair composedToSimple(KeyValuePair _p);
};

class DBAdapter : public IDbAdapter, private DBAdapterBase {
 public:
  DBAdapter(std::shared_ptr<storage::IDBClient>,
            std::unique_ptr<IDataKeyGenerator> keyGen = std::make_unique<KeyGenerator>());

  // Adds a block from a set of key/value pairs and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key
  // - calculating and filling in the parent digest.
  // Typically called by the application when adding a new block.
  BlockId addBlock(const SetOfKeyValuePairs &updates) override;
  // Adds a block from its raw representation and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key.
  // Typically called by state transfer when a block is received and needs to be added.
  void addRawBlock(const RawBlock &, const BlockId &) override;

  std::pair<Value, BlockId> getValue(const Key &, const BlockId &blockVersion) const override;

  RawBlock getRawBlock(const BlockId &blockId) const override;

  void deleteBlock(const BlockId &) override;

  BlockId getLatestBlockId() const override;
  BlockId getLastReachableBlockId() const override;

  std::shared_ptr<storage::IDBClient> getDb() const override { return db_; }

 private:
  concordUtils::Status addBlockAndUpdateMultiKey(const SetOfKeyValuePairs &_kvMap,
                                                 const BlockId &_block,
                                                 const concordUtils::Sliver &_blockRaw);

  std::unique_ptr<IDataKeyGenerator> keyGen_;
};

}  // namespace v1DirectKeyValue
}  // namespace concord::kvbc
