// Copyright 2018 VMware, all rights reserved

// @file direct_kv_db_adapter.cpp
//
// Data is stored in the form of key value pairs. However, the key used is a
// composite database key. Its composition is : Key Type | Key | Block Id
//
// Block Id is functionally equivalent to the version of the block.
//
// For the purposes of iteration, the keys appear in the following order:
//    Ascending order of Key Type
//    -> Ascending order of Key
//       -> Descending order of Block Id

#include "Logger.hpp"
#include "sliver.hpp"
#include "status.hpp"
#include "storage/direct_kv_key_manipulator.h"
#include "hex_tools.h"
#include "assertUtils.hpp"
#include <cstring>

using logging::Logger;
using concordUtils::Sliver;
using concordUtils::HexPrintBuffer;

namespace concord::storage::v1DirectKeyValue {

using detail::EDBKeyType;

/**
 * @brief Generates a Composite Database Key from objectId.
 *
 * Merges the key type and the block id to generate a composite
 * database key for E_DB_KEY_TYPE_METADATA_KEY.
 *
 * Format : Key Type | Object Id
 *
 * @return Sliver object of the generated composite database key.
 */

Sliver MetadataKeyManipulator::generateMetadataKey(ObjectId objectId) const {
  size_t keySize = sizeof(EDBKeyType) + sizeof(objectId);
  auto keyBuf = new char[keySize];
  size_t offset = 0;
  EDBKeyType keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&objectId, sizeof(objectId));
  return Sliver(keyBuf, keySize);
}
Sliver S3MetadataKeyManipulator::generateMetadataKey(const concordUtils::Sliver &key) const {
  return prefix_ + std::string("metadata/") + key.toString();
}

/*
 * Format : Key Type | Object Id
 */
Sliver STKeyManipulator::generateStateTransferKey(ObjectId objectId) const {
  size_t keySize = sizeof(EDBKeyType) + sizeof(objectId);
  auto keyBuf = new char[keySize];
  size_t offset = 0;
  EDBKeyType keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_ST_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&objectId, sizeof(objectId));
  return Sliver(keyBuf, keySize);
}
/**
 * Format : Key Type | Page Id
 */
Sliver STKeyManipulator::generateSTPendingPageKey(uint32_t pageid) const {
  size_t keySize = sizeof(EDBKeyType) + sizeof(pageid);
  auto keyBuf = new char[keySize];
  size_t offset = 0;
  auto keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_ST_PENDING_PAGE_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&pageid, sizeof(pageid));
  return Sliver(keyBuf, keySize);
}
/**
 * Format : Key Type | Checkpoint
 */
Sliver STKeyManipulator::generateSTCheckpointDescriptorKey(uint64_t chkpt) const {
  size_t keySize = sizeof(EDBKeyType) + sizeof(chkpt);
  auto keyBuf = new char[keySize];
  size_t offset = 0;
  auto keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_ST_CHECKPOINT_DESCRIPTOR_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&chkpt, sizeof(chkpt));
  return Sliver(keyBuf, keySize);
}

/**
 * Format : Key Type | Page Id | Checkpoint
 */
Sliver STKeyManipulator::generateSTReservedPageDynamicKey(uint32_t pageid, uint64_t chkpt) const {
  return generateReservedPageKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY, pageid, chkpt);
}

Sliver STKeyManipulator::getReservedPageKeyPrefix() const {
  static Sliver s(std::string(1, static_cast<char>(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY)));
  return s;
}
/**
 * Format : Key Type | Page Id | Checkpoint
 */
Sliver STKeyManipulator::generateReservedPageKey(EDBKeyType keyType, uint32_t pageid, uint64_t chkpt) {
  size_t keySize = sizeof(EDBKeyType) + sizeof(pageid) + sizeof(chkpt);
  auto keyBuf = new char[keySize];
  size_t offset = 0;
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&pageid, sizeof(pageid));
  copyToAndAdvance(keyBuf, &offset, keySize, (char *)&chkpt, sizeof(chkpt));
  return Sliver(keyBuf, keySize);
}

bool DBKeyGeneratorBase::copyToAndAdvance(
    char *_buf, size_t *_offset, size_t _maxOffset, const char *_src, const size_t &_srcSize) {
  if (!_buf && !_offset && !_src) ConcordAssert(false);

  if (*_offset >= _maxOffset && _srcSize > 0) ConcordAssert(false);

  memcpy(_buf + *_offset, _src, _srcSize);
  *_offset += _srcSize;

  return true;
}

uint64_t STKeyManipulator::extractCheckPointFromKey(const char *_key_data, size_t _key_length) {
  ConcordAssert(_key_length >= sizeof(uint64_t));
  uint64_t chkp = *(uint64_t *)(_key_data + 1);

  LOG_TRACE(logger(), "checkpoint " << chkp << " from key " << (HexPrintBuffer{_key_data, _key_length}));
  return chkp;
}

std::pair<uint32_t, uint64_t> STKeyManipulator::extractPageIdAndCheckpointFromKey(const char *_key_data,
                                                                                  size_t _key_length) {
  ConcordAssert(_key_length >= sizeof(uint32_t) + sizeof(uint64_t));

  uint32_t pageId = *(uint32_t *)(_key_data + 1);
  uint64_t chkp = *(uint64_t *)(_key_data + sizeof(uint32_t) + 1);
  LOG_TRACE(logger(),
            "pageId " << pageId << " checkpoint " << chkp << " from key " << (HexPrintBuffer{_key_data, _key_length}));
  return std::make_pair(pageId, chkp);
}

}  // namespace concord::storage::v1DirectKeyValue
