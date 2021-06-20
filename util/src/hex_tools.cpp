// Copyright 2018 VMware, all rights reserved

#include "hex_tools.h"

#include <cstdint>
#include <iomanip>
#include <ios>
#include <ostream>
#include <sstream>
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

Sliver hexToSliver(const std::string &hex) {
  if (hex.empty()) {
    return Sliver{};
  } else if (hex.size() % 2) {
    throw std::invalid_argument{"Invalid hex string: " + hex};
  }
  const auto valid_chars = "0123456789abcdefABCDEF";
  auto start = std::string::size_type{0};
  if (hex.find("0x") == 0 || hex.find("0X") == 0) {
    start += 2;
    if (hex.size() > 2 && hex.find_first_not_of(valid_chars, 2) != std::string::npos) {
      throw std::invalid_argument{"Invalid hex string: " + hex};
    }
  } else if (hex.find_first_not_of(valid_chars) != std::string::npos) {
    throw std::invalid_argument{"Invalid hex string: " + hex};
  }

  auto result = std::string{};
  result.reserve(hex.size() / 2);
  for (auto i = start; i < hex.size(); i += 2) {
    const auto byte = hex.substr(i, 2);
    result.push_back(std::stoi(byte, nullptr, 16));
  }
  return result;
}

std::string bufferToHex(const char *data, size_t size) {
  auto ss = std::stringstream{};
  hexPrint(ss, data, size);
  return ss.str();
}

std::string bufferToHex(const std::uint8_t *data, size_t size) {
  return bufferToHex(reinterpret_cast<const char *>(data), size);
}

std::string sliverToHex(const Sliver &sliver) { return bufferToHex(sliver.data(), sliver.length()); }

std::string vectorToHex(const std::vector<std::uint8_t> &data) { return bufferToHex(data.data(), data.size()); }

}  // namespace concordUtils
