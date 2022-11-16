// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
//
//
// Thin wrapper over selected cryptographic function utilities from OpenSSL's
// crypto library, which we consider a reasonably trusted source of
// cryptographic implementations. This wrapper is intended to provide a cleaner
// and more convenient interface to the OpenSSL crypto library to fit better
// with the rest of the Concord codebase, as the OpenSSL crypto library itself
// has a C interface.

#pragma once

#include <openssl/bn.h>
#include <openssl/err.h>
#include "scope_exit.hpp"
#include "crypto.hpp"

#define THROW_OPENSSL_ERROR throw std::runtime_error(ERR_reason_error_string(ERR_get_error()))
namespace concord::crypto::openssl {

class Integer {
 public:
  Integer() : num_{BN_new()} { assertResultValid(num_.get()); }
  explicit Integer(const long n) : Integer() {
    long val = n;
    if (val < 0) {
      BN_set_negative(num_.get(), 1);
      val *= -1;
    }
    assertResultValid(BN_set_word(num_.get(), val));
  }
  Integer(const std::string& s) : Integer{reinterpret_cast<unsigned const char*>(s.data()), s.size()} {}
  // BN_bin2bn() converts the positive integer in big-endian form of length len at s into a BIGNUM
  Integer(const unsigned char* val_ptr, size_t size) : num_{BN_bin2bn(val_ptr, size, nullptr)} {
    assertResultValid(num_.get());
  }
  ~Integer() = default;
  Integer(const Integer& i) : num_{BN_dup(i.num_.get())} { assertResultValid(num_.get()); }
  Integer(Integer&& other) { num_ = std::move(other.num_); }

  template <typename T>
  static void assertResultValid(T result) {
    if (!result) {
      THROW_OPENSSL_ERROR;
    }
  }

 private:
  Integer(BIGNUM* num) : num_{num} {}

 public:
  static Integer fromHexString(const std::string& hex_str) {
    BIGNUM* result_ptr = nullptr;
    assertResultValid(BN_hex2bn(&result_ptr, hex_str.c_str()));
    return Integer(result_ptr);
  }
  static Integer fromDecString(const std::string& dec_str) {
    BIGNUM* result_ptr = nullptr;
    assertResultValid(BN_dec2bn(&result_ptr, dec_str.c_str()));
    return Integer(result_ptr);
  }
  void setNegative() { BN_set_negative(num_.get(), 1); }
  bool isNegative() { return BN_is_negative(num_.get()); }

  Integer operator+(const Integer& i) {
    BIGNUM* result_ptr = BN_new();
    assertResultValid(BN_add(result_ptr, num_.get(), i.num_.get()));
    return Integer(result_ptr);
  }
  Integer& operator+=(const Integer& i) {
    *this = (*this + i);
    return *this;
  }
  Integer operator-(const Integer& i) {
    BIGNUM* result_ptr = BN_new();
    assertResultValid(BN_sub(result_ptr, num_.get(), i.num_.get()));
    return Integer(result_ptr);
  }
  Integer& operator-=(const Integer& i) {
    *this = (*this - i);
    return *this;
  }
  Integer operator*(const Integer& i) const {
    UniqueBNCTX ctx(BN_CTX_new());
    assertResultValid(ctx.get());
    BIGNUM* result_ptr = BN_new();
    assertResultValid(BN_mul(result_ptr, num_.get(), i.num_.get(), ctx.get()));
    return Integer(result_ptr);
  }
  Integer operator/(const Integer& i) const {
    UniqueBNCTX ctx(BN_CTX_new());
    assertResultValid(ctx.get());
    BIGNUM* result_ptr = BN_new();
    assertResultValid(BN_div(result_ptr, nullptr, num_.get(), i.num_.get(), ctx.get()));
    return Integer(result_ptr);
  }
  Integer operator%(const Integer& i) const {
    UniqueBNCTX ctx(BN_CTX_new());
    assertResultValid(ctx.get());
    BIGNUM* result_ptr = BN_new();
    assertResultValid(BN_mod(result_ptr, num_.get(), i.num_.get(), ctx.get()));
    return Integer(result_ptr);
  }
  Integer operator=(const Integer& i) {
    num_.reset(BN_dup(i.num_.get()));
    assertResultValid(num_.get());
    return *this;
  }

  bool operator!=(const Integer& i) const { return BN_cmp(num_.get(), i.num_.get()) != 0; }
  bool operator==(const Integer& i) const { return BN_cmp(num_.get(), i.num_.get()) == 0; }

  Integer operator<<(size_t n) const {
    BIGNUM* result_ptr = BN_new();
    assertResultValid(BN_lshift(result_ptr, num_.get(), n));
    return Integer(result_ptr);
  }

  size_t size() const { return BN_num_bytes(num_.get()); }
  std::string toHexString(bool add_prefix = false) const {
    auto strNum = BN_bn2hex(num_.get());
    assertResultValid(strNum);
    std::string result{UniqueOpenSSLString{strNum}.get()};
    if (add_prefix) {
      result = std::string("0x") + result;
    }
    return result;
  }
  std::string toDecString() const {
    auto strNum = BN_bn2dec(num_.get());
    assertResultValid(strNum);
    std::string result{UniqueOpenSSLString{strNum}.get()};
    return result;
  }

 private:
  UniqueBIGNUM num_;
};

inline std::ostream& operator<<(std::ostream& os, const Integer& i) {
  os << i.toHexString();
  return os;
}

}  // namespace concord::crypto::openssl
