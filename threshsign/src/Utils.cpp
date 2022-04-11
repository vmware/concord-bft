// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "Utils.h"
#include "AutoBuf.h"
#include "Logger.hpp"

#include <string>
#include <stdexcept>
#include <climits>
#include <memory>
#include <set>
#include <fstream>

#include "XAssert.h"

using std::endl;

bool Utils::fileExists(const std::string& file) {
  std::ifstream fin(file);
  return fin.good();
}

void Utils::bin2hex(const void* bin, int binLen, char* hexBuf, int hexBufCapacity) {
  int needed = binLen * 2 + 1;
  static const char hex[] = "0123456789abcdef";

  if (hexBufCapacity < needed) {
    LOG_ERROR(
        THRESHSIGN_LOG,
        "You have not supplied a large enough buffer: got " << hexBufCapacity << " but need " << needed << " bytes");
    throw std::runtime_error("bin2hex not enough capacity for hexbuf");
  }

  const unsigned char* bytes = reinterpret_cast<const unsigned char*>(bin);
  for (int i = 0; i < binLen; i++) {
    unsigned char c = bytes[i];
    hexBuf[2 * i] = hex[c >> 4];       // translate the upper 4 bits
    hexBuf[2 * i + 1] = hex[c & 0xf];  // translate the lower 4 bits
  }
  hexBuf[2 * binLen] = '\0';
}

std::string Utils::bin2hex(const void* bin, int binLen) {
  assertStrictlyPositive(binLen);

  size_t hexBufSize = 2 * static_cast<size_t>(binLen) + 1;
  AutoCharBuf hexBuf(static_cast<int>(hexBufSize));
  std::string hexStr(hexBufSize, '\0');  // pre-allocate string object

  assertEqual(hexBufSize, hexStr.size());

  Utils::bin2hex(bin, binLen, hexBuf, static_cast<int>(hexBufSize));
  hexStr.assign(hexBuf.getBuf());

  return hexStr;
}

void Utils::hex2bin(const std::string& hexStr, unsigned char* bin, int binCapacity) {
  hex2bin(hexStr.c_str(), static_cast<int>(hexStr.size()), bin, binCapacity);
}

void Utils::hex2bin(const char* hexBuf, int hexBufLen, unsigned char* bin, int binCapacity) {
  assertNotNull(hexBuf);
  assertNotNull(bin);

  // If we get a string such as F, we convert it to 0F, so that sscanf
  // can handle it properly.
  if (hexBufLen % 2) {
    throw std::runtime_error("Invalid hexadecimal string: odd size");
  }

  int binLen = hexBufLen / 2;

  if (binLen > binCapacity) {
    throw std::runtime_error("hexBuf size is larger than binary buffer size");
  }

  for (int count = 0; count < binLen; count++) {
#if defined(__STDC_LIB_EXT1__) || defined(_WIN32)
    if (sscanf_s(hexBuf, "%2hhx", bin + count) != 1)
      throw std::runtime_error("Invalid hexadecimal string: bad character");
#else
    if (sscanf(hexBuf, "%2hhx", bin + count) != 1)
      throw std::runtime_error("Invalid hexadecimal string: bad character");
#endif

    hexBuf += 2;
  }
}
