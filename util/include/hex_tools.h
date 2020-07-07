// Copyright 2018 VMware, all rights reserved

#ifndef CONCORD_BFT_UTIL_HEX_TOOLS_H_
#define CONCORD_BFT_UTIL_HEX_TOOLS_H_

#include <stdio.h>

#include <cstdint>
#include <string>

#include "sliver.hpp"

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

// Converts a hex string into a byte buffer. Handles leading 0x characters (if present).
Sliver hexToSliver(const std::string &hex);

// Converts a buffer into a hex string.
std::string bufferToHex(const char *data, size_t size);
std::string bufferToHex(const std::uint8_t *data, size_t size);

// Converts a sliver into a hex string.
std::string sliverToHex(const Sliver &sliver);

}  // namespace concordUtils

#endif  // CONCORD_BFT_UTIL_HEX_TOOLS_H_
