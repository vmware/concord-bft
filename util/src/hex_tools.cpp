// Copyright 2018 VMware, all rights reserved

#include "hex_tools.h"

#include <cstdint>
#include <iomanip>
#include <ios>
#include <ostream>
#include <stdexcept>

namespace concordUtils {

// Print <size> bytes from <data> to <s> as their 0x<hex> representation.
std::ostream &hexPrint(std::ostream &s, const char *data, size_t size) {
  // Store current state of ostream flags
  std::ios::fmtflags f(s.flags());
  s << "0x";
  for (size_t i = 0; i < size; i++) {
    // Convert from signed char to std::uint8_t and then to an unsigned non-char type so that it prints as an integer.
    const auto u = static_cast<std::uint8_t>(data[i]);
    s << std::hex << std::setw(2) << std::setfill('0') << static_cast<std::uint16_t>(u);
  }
  // restore current state
  s.flags(f);
  return s;
}

}  // namespace concordUtils
