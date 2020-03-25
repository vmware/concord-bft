// Copyright 2018 VMware, all rights reserved

#ifndef CONCORD_BFT_UTIL_HEX_TOOLS_H_
#define CONCORD_BFT_UTIL_HEX_TOOLS_H_

#include <stdio.h>
#include <string>

namespace concordUtils {

std::ostream &hexPrint(std::ostream &s, const char *data, size_t size);

struct HexPrintBuffer {
  const char *bytes;
  const size_t size;
};

// Print a char* of bytes as its 0x<hex> representation.
inline std::ostream &operator<<(std::ostream &s, const HexPrintBuffer p) {
  concordUtils::hexPrint(s, p.bytes, p.size);
  return s;
}

}  // namespace concordUtils

#endif  // CONCORD_BFT_UTIL_HEX_TOOLS_H_
