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

#define THROW_OPENSSL_ERROR throw std::runtime_error(ERR_reason_error_string(ERR_get_error()))
namespace concord::crypto::openssl {

class Integer {
 public:
  Integer() : num_{BN_new()} {
    if (!num_) THROW_OPENSSL_ERROR;
  }
  explicit Integer(const long n) : Integer() {
    long val = n;
    if (val < 0) {
      BN_set_negative(num_, 1);
      val *= -1;
    }
    if (!BN_set_word(num_, val)) THROW_OPENSSL_ERROR;
  }
  Integer(const std::string& s) : Integer{reinterpret_cast<unsigned const char*>(s.data()), s.size()} {}
  // BN_bin2bn() converts the positive integer in big-endian form of length len at s into a BIGNUM
  Integer(const unsigned char* val_ptr, size_t size) : num_{BN_bin2bn(val_ptr, size, nullptr)} {
    if (!num_) THROW_OPENSSL_ERROR;
  }
  ~Integer() { BN_free(num_); }
  Integer(const Integer& i) : num_{BN_dup(i.num_)} {
    if (!num_) THROW_OPENSSL_ERROR;
  }

 private:
  Integer(BIGNUM* num) : num_{num} {}

 public:
  static Integer fromHexString(const std::string& hex_str) {
    BIGNUM* num = BN_new();
    if (!num) THROW_OPENSSL_ERROR;
    if (!BN_hex2bn(&num, hex_str.c_str())) THROW_OPENSSL_ERROR;
    return num;
  }
  static Integer fromDecString(const std::string& dec_str) {
    BIGNUM* num = BN_new();
    if (!num) THROW_OPENSSL_ERROR;
    if (!BN_dec2bn(&num, dec_str.c_str())) THROW_OPENSSL_ERROR;
    return num;
  }
  void setNegative() { BN_set_negative(num_, 1); }

  Integer operator+(const Integer& i) {
    BIGNUM* res = BN_new();
    BN_add(res, num_, i.num_);
    return res;
  }
  Integer& operator+=(const Integer& i) {
    BN_add(num_, num_, i.num_);
    return *this;
  }
  Integer operator-(const Integer& i) {
    BIGNUM* res = BN_new();
    BN_sub(res, num_, i.num_);
    return res;
  }
  Integer& operator-=(const Integer& i) {
    BN_sub(num_, num_, i.num_);
    return *this;
  }
  Integer operator*(const Integer& i) const {
    BN_CTX* ctx = BN_CTX_new();
    if (!ctx) THROW_OPENSSL_ERROR;
    concord::util::ScopeExit s{[&]() { BN_CTX_free(ctx); }};
    BIGNUM* res = BN_new();
    if (!res) THROW_OPENSSL_ERROR;
    if (!BN_mul(res, num_, i.num_, ctx)) THROW_OPENSSL_ERROR;
    return res;
  }
  Integer operator/(const Integer& i) const {
    BN_CTX* ctx = BN_CTX_new();
    if (!ctx) THROW_OPENSSL_ERROR;
    concord::util::ScopeExit s{[&]() { BN_CTX_free(ctx); }};
    BIGNUM* res = BN_new();
    if (!res) THROW_OPENSSL_ERROR;
    if (!BN_div(res, nullptr, num_, i.num_, ctx)) THROW_OPENSSL_ERROR;
    return res;
  }
  Integer operator%(const Integer& i) const {
    BN_CTX* ctx = BN_CTX_new();
    if (!ctx) THROW_OPENSSL_ERROR;
    concord::util::ScopeExit s{[&]() { BN_CTX_free(ctx); }};
    BIGNUM* res = BN_new();
    if (!res) THROW_OPENSSL_ERROR;
    if (!BN_mod(res, num_, i.num_, ctx)) THROW_OPENSSL_ERROR;
    return res;
  }
  Integer operator=(const Integer& i) {
    num_ = BN_dup(i.num_);
    if (!num_) THROW_OPENSSL_ERROR;
    return *this;
  }

  bool operator!=(const Integer& i) const { return BN_cmp(num_, i.num_) != 0; }
  bool operator==(const Integer& i) const { return BN_cmp(num_, i.num_) == 0; }

  Integer operator<<(size_t n) const {
    BIGNUM* res = BN_new();
    if (!res) THROW_OPENSSL_ERROR;
    if (!BN_lshift(res, num_, n)) THROW_OPENSSL_ERROR;
    return res;
  }
  size_t size() const { return BN_num_bytes(num_); }
  std::string toHexString() const { return BN_bn2hex(num_); }
  std::string toDecString() const { return BN_bn2dec(num_); }

 private:
  BIGNUM* num_;
};

inline std::ostream& operator<<(std::ostream& os, const Integer& i) {
  os << i.toHexString();
  return os;
}
}  // namespace concord::crypto::openssl
