// Copyright (c) 2019 VMware, Inc. All Rights Reserved.

#pragma once

#include <cstdint>
#include <cstring>

namespace concord {
namespace storage {
namespace blockchain {

bool copyToAndAdvance(uint8_t *_buf, size_t *_offset, size_t _maxOffset,
                      uint8_t *_src, size_t _srcSize);

}
}
}

