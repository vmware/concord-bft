// Copyright (c) 2018 VMware, Inc. All Rights Reserved.

#include "blockchain_db_helpers.h"

namespace concord {
namespace storage {
namespace blockchain {

bool copyToAndAdvance(uint8_t *_buf, size_t *_offset, size_t _maxOffset,
                      uint8_t *_src, size_t _srcSize) {
  if (!_buf) {
    return false;
  }

  if (!_offset) {
    return false;
  }

  if (!_src) {
    return false;
  }

  if (*_offset >= _maxOffset && _srcSize > 0) {
    return false;
  }

  memcpy(_buf + *_offset, _src, _srcSize);
  *_offset += _srcSize;

  return true;
}

}
}
}
