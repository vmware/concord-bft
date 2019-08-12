// Copyright (c) 2019 VMware, Inc. All Rights Reserved.

#ifndef CONCORD_STORAGE_BLOCKCHAIN_DB_HELPERS_H_
#define CONCORD_STORAGE_BLOCKCHAIN_DB_HELPERS_H_

#include <cstdint>
#include <cstring>

namespace concordStorage {
namespace blockchain {

bool copyToAndAdvance(uint8_t *_buf, size_t *_offset, size_t _maxOffset,
                      uint8_t *_src, size_t _srcSize);

}  // namespace blockchain 
}  // namespace concordStorage

#endif  // CONCORD_STORAGE_BLOCKCHAIN_DB_HELPERS_H_
