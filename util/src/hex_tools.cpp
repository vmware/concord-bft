// Copyright 2018 VMware, all rights reserved

#include "hex_tools.h"

#include <iomanip>
#include <ios>
#include <ostream>
#include <stdexcept>

namespace concordUtils {

// Print <size> bytes from <data> to <s> as their 0x<hex> representation.
std::ostream &hexPrint(std::ostream &s, const char* data, size_t size) {
  // Store current state of ostream flags
  std::ios::fmtflags f(s.flags());
  s << "0x";
  for (size_t i = 0; i < size; i++) {
    s << std::hex << std::setw(2) << std::setfill('0') << (uint)data[i];
  }
  // restore current state
  s.flags(f);
  return s;
}

}  // namespace concordUtils
