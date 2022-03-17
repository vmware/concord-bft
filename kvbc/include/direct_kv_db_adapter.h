// Copyright 2018 VMware, all rights reserved
//
// Contains functionality for working with composite database keys and
// using them to perform basic blockchain operations.

#pragma once

#include "db_adapter_interface.h"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "storage/db_interface.h"
#include "storage/direct_kv_key_manipulator.h"
#include "PerformanceManager.hpp"

#include <memory>

namespace concord::kvbc::v1DirectKeyValue {

/** Key comparator for sorting keys in database.
 *  Used with rocksdb when there's no natural lexicographical key comparison.
 *
 *  @deprecated should be removed after switching to merkle-style keys serialization.
 */
class DBKeyComparator : public concord::storage::IDBClient::IKeyComparator {
 public:
  int composedKeyComparison(const char *_a_data, size_t _a_length, const char *_b_data, size_t _b_length) override;
  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.kvbc.DBKeyComparator");
    return logger_;
  }
};

/** Defines interface for blockchain data keys generation.
 *
 */
class IDataKeyGenerator {
 public:
  virtual Key blockKey(const BlockId &) const = 0;
  virtual Key dataKey(const Key &, const BlockId &) const = 0;
  virtual Key mdtKey(const Key &) const = 0;

  virtual ~IDataKeyGenerator() = default;
};

/** Default Key Generator
 *  Used with rocksdb
 */
class RocksKeyGenerator : public IDataKeyGenerator, storage::v1DirectKeyValue::DBKeyGeneratorBase {
 public:
  Key blockKey(const BlockId &) const override;
  Key dataKey(const Key &, const BlockId &) const override;
  Key mdtKey(const Key &key) const override { return key; }

 protected:
  static concordUtils::Sliver genDbKey(storage::v1DirectKeyValue::detail::EDBKeyType, const Key &, BlockId);
  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.kvbc.RocksKeyGenerator");
    return logger_;
  }
};

/** Key generator for S3 storage
 *  As S3 has textual hierarchical URI like structure the generated keys are as follows:
 *  prefix/block_id/raw_block
 *  prefix/block_id/key1
 *  ......................
 *  prefix/block_id/keyN
 *
 *  metadata have the following form:
 *  prefix/metadata/key
 *
 *  keys are transformed to hexadecimal strings.
 *
 *  Where prefix is some arbitrary string.
 *  Typically it should be a blockchain id.
 *  For tests it is usually a timestamp.
 */
class S3KeyGenerator : public IDataKeyGenerator {
 public:
  S3KeyGenerator(const std::string &prefix = "") : prefix_(prefix.size() ? prefix + std::string("/") : "") {}
  Key blockKey(const BlockId &) const override;
  Key dataKey(const Key &, const BlockId &) const override;
  Key mdtKey(const Key &key) const override;

 protected:
  static std::string string2hex(const std::string &s);
  static std::string hex2string(const std::string &);
  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.kvbc.S3KeyGenerator");
    return logger_;
  }

  std::string prefix_;
};

/** Key Manipulator for extracting info from database keys
 *  Used for rocksdb when there's no natural lexicographical key comparison.
 *
 *  @deprecated should be removed after switching to merkle-style keys serialization.
 */
class DBKeyManipulator {
 public:
  static BlockId extractBlockIdFromKey(const Key &_key);
  static BlockId extractBlockIdFromKey(const char *_key_data, size_t _key_length);
  static storage::v1DirectKeyValue::detail::EDBKeyType extractTypeFromKey(const Key &_key);
  static storage::v1DirectKeyValue::detail::EDBKeyType extractTypeFromKey(const char *_key_data);
  static storage::ObjectId extractObjectIdFromKey(const Key &_key);
  static storage::ObjectId extractObjectIdFromKey(const char *_key_data, size_t _key_length);
  static Key extractKeyFromKeyComposedWithBlockId(const Key &_composedKey);
  static int compareKeyPartOfComposedKey(const char *a_data, size_t a_length, const char *b_data, size_t b_length);
  static Key extractKeyFromMetadataKey(const Key &_composedKey);
  static bool isKeyContainBlockId(const Key &_composedKey);
  static KeyValuePair composedToSimple(KeyValuePair _p);
  static std::string extractFreeKey(const char *_key_data, size_t _key_length);
  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.kvbc.DBKeyManipulator");
    return logger_;
  }
};

/** DBadapter for managing key/value blockchain atop key/value store in form of simple blocks
 *
 */
class DBAdapter : public IDbAdapter {
 public:
  DBAdapter(std::shared_ptr<storage::IDBClient> dataStore,
            std::unique_ptr<IDataKeyGenerator> keyGen = std::make_unique<RocksKeyGenerator>(),
            bool use_mdt = false,
            bool save_kv_pairs_separately = true,
            const std::shared_ptr<concord::performance::PerformanceManager> &pm_ =
                std::make_shared<concord::performance::PerformanceManager>());

  // Adds a block from a set of key/value pairs and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key
  // - calculating and filling in the parent digest.
  // Typically called by the application when adding a new block.
  BlockId addBlock(const SetOfKeyValuePairs &updates) override;

  // Direct adapter links during State Transfer in AddRawBlock only, since no post-processing is needed.
  void linkUntilBlockId(BlockId until_block_id) override { return; }

  // Adds a block from its raw representation and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key.
  // Typically called by state transfer when a block is received and needs to be added.
  void addRawBlock(const RawBlock &, const BlockId &, bool lastBlock = false) override;

  std::pair<Value, BlockId> getValue(const Key &, const BlockId &blockVersion) const override;

  RawBlock getRawBlock(const BlockId &blockId) const override;

  void deleteBlock(const BlockId &) override;

  void deleteLastReachableBlock() override;

  bool hasBlock(const BlockId &blockId) const override;

  BlockId getGenesisBlockId() const override { return (getLastReachableBlockId() ? INITIAL_GENESIS_BLOCK_ID : 0); }
  BlockId getLatestBlockId() const override { return lastBlockId_; }
  BlockId getLastReachableBlockId() const override { return lastReachableBlockId_; }

  // Returns the block data in the form of a set of key/value pairs.
  SetOfKeyValuePairs getBlockData(const RawBlock &rawBlock) const override;

  // Returns the parent digest of the passed block.
  BlockDigest getParentDigest(const RawBlock &rawBlock) const override;

  std::shared_ptr<storage::IDBClient> getDb() const override { return db_; }
  virtual void getLastKnownReconfigurationCmdBlock(std::string &outBlockData) const override;
  virtual void setLastKnownReconfigurationCmdBlock(std::string &blockData) override;

 protected:
  void setLastReachableBlockNum(const BlockId &blockId);
  void setLatestBlock(const BlockId &blockId);

  BlockId fetchLatestBlockId() const;
  BlockId fetchLastReachableBlockId() const;

  // The following methods are used to store metadata parameters
  BlockId mdtGetLatestBlockId() const;
  BlockId mdtGetLastReachableBlockId() const;
  concordUtils::Status mdtGet(const concordUtils::Sliver &key, concordUtils::Sliver &val) const;
  concordUtils::Status mdtPut(const concordUtils::Sliver &key, const concordUtils::Sliver &val);

  concordUtils::Status addBlockAndUpdateMultiKey(const SetOfKeyValuePairs &_kvMap,
                                                 const BlockId &_block,
                                                 const concordUtils::Sliver &_blockRaw);
  logging::Logger logger_;
  std::shared_ptr<storage::IDBClient> db_;
  std::unique_ptr<IDataKeyGenerator> keyGen_;
  bool mdt_ = false;  // whether we explicitly store blockchain metadata
  BlockId lastBlockId_ = 0;
  BlockId lastReachableBlockId_ = 0;
  BlockId lastKnownReconfigurationCmdBlock_ = 0;
  bool saveKvPairsSeparately_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
  std::mutex mutex_;
};

}  // namespace concord::kvbc::v1DirectKeyValue
