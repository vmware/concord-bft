// Copyright 2018 VMware, all rights reserved
//
// Storage key comparators implementation.

#include "comparators.h"

#include "Logger.hpp"

#include "hex_tools.h"
#include "sliver.hpp"
#include "blockchain_db_adapter.h"
#include "blockchain_db_types.h"
#include "blockchain_db_interfaces.h"
#include "rocksdb_client.h"

#include <chrono>

using concordUtils::Sliver;
using concordlogger::Logger;
using concord::storage::rocksdb::copyRocksdbSlice;

namespace concord {
namespace storage {
namespace blockchain {

/*
 * If key a is earlier than key b, return a negative number; if larger, return a
 * positive number; if equal, return zero.
 *
 * Comparison is done by decomposed parts. Types are compared first, followed by
 * the application key, and finally the block id. Types and keys are sorted in
 * ascending order, and block IDs are sorted in descending order.
 */
int RocksKeyComparator::ComposedKeyComparison(const Logger& logger,
                                              const Sliver& _a,
                                              const Sliver& _b) {
  // TODO(BWF): see note about multiple bytes in extractTypeFromKey
  char aType = KeyManipulator::extractTypeFromKey(_a);
  char bType = KeyManipulator::extractTypeFromKey(_b);
  if (aType != bType) {
    int ret = aType - bType;
    return ret;
  }

  // In case it is E_DB_KEY_TYPE_METADATA_KEY, compare object IDs.
  if (aType == (char)EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY) {
    BlockId aObjId = KeyManipulator::extractObjectIdFromKey(logger, _a);
    BlockId bObjId = KeyManipulator::extractObjectIdFromKey(logger, _b);

    if (aObjId < bObjId) return -1;
    if (bObjId < aObjId) return 1;
    return 0;
  }

  // if this is a block, we stop with key comparison - it doesn't have a block
  // id component (that would be redundant)
  if (aType == ((char)EDBKeyType::E_DB_KEY_TYPE_BLOCK)) {
    // Extract the block ids to compare so that endianness of environment
    // does not matter.
    BlockId aId = KeyManipulator::extractBlockIdFromKey(logger, _a);
    BlockId bId = KeyManipulator::extractBlockIdFromKey(logger, _b);

    if (aId < bId) {
      return -1;
    } else if (bId < aId) {
      return 1;
    } else {
      return 0;
    }
  }

  Sliver aKey =
      KeyManipulator::extractKeyFromKeyComposedWithBlockId(logger, _a);
  Sliver bKey =
      KeyManipulator::extractKeyFromKeyComposedWithBlockId(logger, _b);

  int keyComp = aKey.compare(bKey);

  if (keyComp == 0) {
    BlockId aId = KeyManipulator::extractBlockIdFromKey(logger, _a);
    BlockId bId = KeyManipulator::extractBlockIdFromKey(logger, _b);

    // within a type+key, block ids are sorted in reverse order
    if (aId < bId) {
      return 1;
    } else if (bId < aId) {
      return -1;
    } else {
      return 0;
    }
  }

  return keyComp;
}

/* RocksDB */
#ifdef USE_ROCKSDB
int RocksKeyComparator::Compare(const ::rocksdb::Slice& _a,
                                const ::rocksdb::Slice& _b) const {
  Sliver a = copyRocksdbSlice(_a);
  Sliver b = copyRocksdbSlice(_b);
  int ret = ComposedKeyComparison(logger, a, b);

  LOG_DEBUG(logger,
                  "Compared " << a << " with " << b << ", returning " << ret);

  return ret;
}
#endif

/* In memory */
bool RocksKeyComparator::InMemKeyComp(const Sliver& _a, const Sliver& _b) {
  Logger logger(
      concordlogger::Log::getLogger("concord.storage.RocksKeyComparator"));
  int comp = ComposedKeyComparison(logger, _a, _b);

  LOG_DEBUG(logger, "Compared " << _a << " with " << _b
                                      << ", a < b == " << (comp < 0));

  // Check: comp < 0 ==> _a < _b
  return comp < 0;
}

}
}
}
