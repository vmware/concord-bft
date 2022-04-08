/*
 * Utils.cpp
 *
 *  Created on: Oct 10, 2014
 *      Author: Alin Tomescu <alinush@mit.edu>
 */
#include <xutils/Utils.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>

#include <string>
#include <stdexcept>
#include <climits>
#include <memory>
#include <set>
#include <fstream>

#include <xassert/XAssert.h>

using std::endl;

bool Utils::fileExists(const std::string& file) {
    std::ifstream fin(file);
    return fin.good();
}

void Utils::bin2hex(const void * bin, size_t binLen, char * hexBuf, size_t hexBufCapacity) {
    size_t needed = binLen * 2 + 1;
    static const char hex[] = "0123456789abcdef";

    if(hexBufCapacity < needed) {
        logerror << "You have not supplied a large enough buffer: got "
                <<  hexBufCapacity << " but need " << needed << " bytes" << endl;
        throw std::runtime_error("bin2hex not enough capacity for hexbuf");
    }

    const unsigned char * bytes = reinterpret_cast<const unsigned char *>(bin);
    for(size_t i = 0; i < binLen; i++)
    {
        unsigned char c = bytes[i];
        hexBuf[2*i]   = hex[c >> 4];    // translate the upper 4 bits
        hexBuf[2*i+1] = hex[c & 0xf];   // translate the lower 4 bits
    }
    hexBuf[2*binLen] = '\0';
}

std::string Utils::bin2hex(const void * bin, size_t binLen) {
    assertStrictlyPositive(binLen);

    size_t hexBufSize = 2 * binLen + 1;
    AutoCharBuf hexBuf(hexBufSize);
    std::string hexStr(hexBufSize, '\0');  // pre-allocate string object

    assertEqual(hexBufSize, hexStr.size());

    Utils::bin2hex(bin, binLen, hexBuf, hexBufSize);
    hexStr.assign(hexBuf.getBuf());

    return hexStr;
}

void Utils::hex2bin(const std::string& hexStr, unsigned char * bin, size_t binCapacity) {
    hex2bin(hexStr.c_str(), hexStr.size(), bin, binCapacity);
}

void Utils::hex2bin(const char * hexBuf, size_t hexBufLen, unsigned char * bin, size_t binCapacity)
{
    assertNotNull(hexBuf);
    assertNotNull(bin);

    // If we get a string such as F, we convert it to 0F, so that sscanf
    // can handle it properly.
    if (hexBufLen % 2) {
        throw std::runtime_error("Invalid hexadecimal string: odd size");
    }

    size_t binLen = hexBufLen / 2;

    if(binLen > binCapacity) {
        throw std::runtime_error("hexBuf size is larger than binary buffer size");
    }

    for (size_t count = 0; count < binLen; count++) {

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
