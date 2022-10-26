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

#pragma once

#include <stdexcept>

#include "Relic.h"

namespace BLS {
namespace Relic {

/**
 * Wrapper around RELIC's bn_t. (i.e., big number type)
 *
 * TODO: Implement C++11 move semantics?
 */
class BNT {
 public:
  bn_t n;  // the big number itself

 public:
  BNT() {
    bn_null(n);
    bn_new(n);
    bn_zero(n);
  }

  BNT(const dig_t d) : BNT() { bn_set_dig(n, d); }

  BNT(int d);

  BNT(const bn_t &o) : BNT() { bn_copy(n, o); }

  BNT(const unsigned char *buf, int len) : BNT() { fromBytes(buf, len); }

  BNT(const std::string &str, int base = 10) : BNT() { fromString(str, base); }

  BNT(const BNT &c) : BNT() { bn_copy(n, c.n); }

  ~BNT() { bn_free(n); }

 public:
  bool getBit(int i) const { return bn_get_bit(n, i) == 1; }

  int getBits() const { return bn_bits(n); }

  int getByteCount() const { return bn_size_bin(n); }

  void toBytes(unsigned char *buf, int capacity) const;

  void fromBytes(const unsigned char *buf, int size) { bn_read_bin(n, buf, size); }

  std::string toString(int base = 10) const;
  void fromString(const std::string &str, int base = 10);

  BNT &Random(int numBits) {
    bn_rand(n, BN_POS, numBits);
    return *this;
  }

  BNT &RandomMod(const BNT &modulus) {
    // FIXME: RELIC: bn_rand_mod should take const bn_t
    bn_rand_mod(n, const_cast<BNT &>(modulus));
    return *this;
  }

  BNT &Negate() {
    // NOTE: Checked RELIC library and this does not allocate extra memory!
    bn_neg(n, n);
    return *this;
  }

  BNT &Subtract(const BNT &b) {
    bn_sub(n, n, b);
    return *this;
  }

  BNT &Times(const BNT &b) {
    // NOTE: Checked RELIC library and it's safe (and fast) to use same output as input.
    bn_mul(n, n, b);
    return *this;
  }
  BNT &Times(const dig_t d) {
    // WARNING: bn_mul_dig only takes an unsigned type as an arg!
    // NOTE: Checked in RELIC library, and using same output as input is okay! (for all implementations)
    bn_mul_dig(n, n, d);
    return *this;
  }

  BNT &DivideBy(const BNT &b) {
    bn_div(n, n, b);
    return *this;
  }

  BNT &DivideBy(const dig_t b) {
    bn_div_dig(n, n, b);
    return *this;
  }

  /**
   * Precomputes u to speed up modular reduction on n % m.
   * Returns u, and caller can now call n.FastModulo(m, u).
   */
  static BNT FastModuloPre(const BNT &m) {
    BNT u;
    bn_mod_pre(u, m);
    return u;
  }
  static BNT FastModuloPreMonty(const BNT &m) {
    BNT u;
    bn_mod_pre_monty(u, m);
    return u;
  }
  static BNT FastModuloPreBarrett(const BNT &m) {
    BNT u;
    bn_mod_pre_barrt(u, m);
    return u;
  }
  static BNT FastModuloPrePmers(const BNT &m) {
    BNT u;
    bn_mod_pre_pmers(u, m);
    return u;
  }

  /**
   * Faster modular reduction.
   *
   * WARNING: u = BNT::FastModuloPre(m) must be called first.
   */
  BNT &FastModulo(const BNT &m, const BNT &u);
  BNT &FastModuloMonty(const BNT &m, const BNT &u);
  BNT &FastModuloBarrett(const BNT &m, const BNT &u);
  BNT &FastModuloPmers(const BNT &m, const BNT &u);

  BNT &SlowModulo(const BNT &m) {
    // TODO: PERF: For MONTGOMERY will need to call more things to convert input to Montgomery form
    // NOTE: Checked RELIC library and it's safe to use same output as input.
    bn_mod_basic(n, n, m);
    return *this;
  }

  static BNT invertModPrime(const dig_t &a, const BNT &p);
  BNT invertModPrime(const BNT &p) const;

  dig_t toDigit() const {
    dig_t d;
    bn_get_dig(&d, n);
    return d;
  }

 public:
  static BNT One() {
    static BNT one(1);
    return one;
  }

  static BNT Zero() {
    static BNT zero(0);
    return zero;
  }

 public:
  // Implicitly cast BNT objects to bn_t types so that we can keep the same syntax when calling RELIC functions
  operator bn_t &() { return n; }
  operator const bn_t &() const { return n; }

  bool operator==(const BNT &rhs) const { return bn_cmp(n, rhs.n) == CMP_EQ; }

  bool operator!=(const BNT &rhs) const { return bn_cmp(n, rhs.n) != CMP_EQ; }

  bool operator!=(const dig_t &rhs) const { return bn_cmp_dig(n, rhs) != CMP_EQ; }

  bool operator<(const BNT &rhs) const { return bn_cmp(n, rhs.n) == CMP_LT; }

  bool operator<(const dig_t d) const { return bn_cmp_dig(n, d) == CMP_LT; }

  bool operator>(const BNT &rhs) const { return bn_cmp(n, rhs.n) == CMP_GT; }

  bool operator>=(const BNT &rhs) const { return bn_cmp(n, rhs.n) != CMP_LT; }

  bool operator<=(const BNT &rhs) const { return bn_cmp(n, rhs.n) != CMP_GT; }

  BNT &operator=(const dig_t d) {
    bn_set_dig(n, d);
    return *this;
  }

  BNT &operator=(const BNT &b) {
    bn_copy(n, b);
    return *this;
  }

  /**
   * WARNING: Looked at RELIC and it seems that bn_add_imp expects different output than input.
   * We might as well use this inefficient but convenient operator implementation then.
   */
  BNT operator+(const BNT &rhs) const {
    BNT newN;
    bn_add(newN, n, rhs);
    return newN;
  }

  /**
   * We just use these for convenience when we have to. We don't want to sacrifice performance by copying bn_t
   * objects around too often!
   */
 public:
  BNT operator*(const BNT &rhs) const {
    BNT mult;
    bn_mul(mult, n, rhs);
    return mult;
  }

  BNT operator%(const BNT &m) const {
    // TODO: PERF: For MONTGOMERY will need to call more things to convert input to Montgomery form
    // NOTE: Checked RELIC library and it's safe to use same output as input.
    BNT r;
    bn_mod_basic(r, n, m);
    return r;
  }

  // Unary negation operator overload
  BNT operator-() const {
    BNT r(n);
    bn_neg(r, r);
    return r;
  }

  BNT operator-(const BNT &b) const {
    BNT r;
    bn_sub(r, n, b);
    return r;
  }
};

/**
 * Wrapper around RELIC's g1_t.
 * (Note the similarity between all of these. Couldn't figure out a nice way to work around code duplication.)
 */
class G1T {
 public:
  g1_t n;

 public:
  G1T() {
    g1_null(n);
    g1_new(n);
    // WARNING: G1T::Identity() assumes the default constructor sets this to infinity!
    g1_set_infty(n);
  }

  G1T(const unsigned char *buf, int len) : G1T() { fromBytes(buf, len); }

  G1T(const G1T &c) : G1T() { g1_copy(n, c.n); }

  G1T(const std::string &str) : G1T() { fromString(str); }

  G1T &operator=(const G1T &c) {
    g1_copy(n, c.n);
    return *this;
  }

  ~G1T() { g1_free(n); }

 public:
  int getByteCount() const { return g1_size_bin(n, 1); }

  size_t toBytes(unsigned char *buf, int size) const;
  void fromBytes(const unsigned char *buf, int size);

  std::string toString() const;
  void fromString(const std::string &str);

  G1T &Random() {
    g1_rand(n);
    return *this;
  }

  G1T &Add(const G1T &b) {
    g1_add(n, n, b);
    return *this;
  }

  G1T &Times(const BNT &b) {
    g1_mul(n, n, b);
    return *this;
  }

  G1T &Double() {
    g1_dbl(n, n);
    return *this;
  }

  static G1T Double(const G1T &n) {
    G1T r;
    g1_dbl(r, n);
    return r;
  }

  static G1T Add(const G1T &a, const G1T &b) {
    G1T r;
    g1_add(r, a, b);
    return r;
  }

  static G1T Times(const G1T &a, const BNT &e) {
    G1T r;
    g1_mul(r, a, e);
    return r;
  }

  static G1T TimesTwice(const G1T &a1, const BNT &e1, const G1T &a2, const BNT &e2) {
    G1T r;
    g1_mul_sim(r, a1, e1, a2, e2);
    return r;
  }

  static G1T Identity() {
    static G1T id;
    return id;
  }

 public:
  // Implicitly cast G1T objects to g1_t types so that we can keep the same syntax when calling RELIC functions
  operator g1_t &() { return n; }
  operator const g1_t &() const { return n; }

  bool operator==(const G1T &rhs) const { return g1_cmp(n, rhs.n) == CMP_EQ; }

  bool operator!=(const G1T &rhs) const { return g1_cmp(n, rhs.n) != CMP_EQ; }
};

/**
 * Wrapper around RELIC's g2_t.
 * (Note the similarity between all of these. Couldn't figure out a nice way to work around code duplication.)
 */
class G2T {
 public:
  g2_t n;

 public:
  G2T() {
    g2_null(n);
    g2_new(n);
    // WARNING: G2T::Identity() assumes the default constructor sets this to infinity!
    g2_set_infty(n);
  }

  G2T(const G2T &c) : G2T() {
    // WARNING: RELIC asks for non-const, even though it does not modify the args. Thus, we remove the const using a
    // cast.
    g2_copy(n, const_cast<g2_t &>(c.n));
  }

  G2T(const std::string &str) : G2T() { fromString(str); }

  G2T(const unsigned char *buf, int size) : G2T() { fromBytes(buf, size); }

  G2T &operator=(const G2T &c) {
    g2_copy(n, const_cast<g2_t &>(c.n));
    return *this;
  }

  ~G2T() { g2_free(n); }

 public:
  int getByteCount() const {
    // FIXME: RELIC: g2_size_bin should take const g2_t
    return g2_size_bin(const_cast<g2_t &>(n), 1);
  }
  void toBytes(unsigned char *buf, int size) const;
  void fromBytes(const unsigned char *buf, int size);

  std::string toString() const;
  void fromString(const std::string &str);

  G2T &Random() {
    g2_rand(n);
    return *this;
  }

  G2T &Times(const BNT &b) {
    g2_mul(n, n, b);
    return *this;
  }

  G2T &Add(const G2T &b) {
    // FIXME: RELIC: g2_add should take const g2_t
    g2_add(n, n, const_cast<G2T &>(b));
    return *this;
  }

  G2T &Double() {
    g2_dbl(n, n);
    return *this;
  }

  static G2T Double(const G2T &n) {
    G2T r;
    // FIXME: RELIC: g2_dbl should take const g2_t
    g2_dbl(r, const_cast<G2T &>(n));
    return r;
  }

  static G2T Add(const G2T &a, const G2T &b) {
    G2T r;
    // FIXME: RELIC: g2_add should take const g2_t
    g2_add(r, const_cast<G2T &>(a), const_cast<G2T &>(b));
    return r;
  }

  static G2T Times(const G2T &a, const BNT &e) {
    G2T r;
    // FIXME: RELIC: g2_mul should take const g2_t
    g2_mul(r, const_cast<G2T &>(a), e);
    return r;
  }

  static G2T TimesTwice(const G2T &a1, const BNT &e1, const G2T &a2, const BNT &e2) {
    G2T r;
    // FIXME: RELIC: should take const's
    g2_mul_sim(r, const_cast<G2T &>(a1), const_cast<BNT &>(e1), const_cast<G2T &>(a2), const_cast<BNT &>(e2));
    return r;
  }

  static G2T Identity() {
    static G2T id;
    return id;
  }

 public:
  // Implicitly cast G2T objects to g2_t types so that we can keep the same syntax when calling RELIC functions
  operator g2_t &() { return n; }
  operator const g2_t &() const { return n; }

  bool operator==(const G2T &rhs) const {
    // WARNING: RELIC asks for non-const, even though it does not modify the args. Thus, we remove the const using a
    // cast.
    return g2_cmp(const_cast<g2_t &>(n), const_cast<g2_t &>(rhs.n)) == CMP_EQ;
  }

  bool operator!=(const G2T &rhs) const {
    // WARNING: RELIC asks for non-const, even though it does not modify the args. Thus, we remove the const using a
    // cast.
    return g2_cmp(const_cast<g2_t &>(n), const_cast<g2_t &>(rhs.n)) != CMP_EQ;
  }
};

/**
 * Wrapper around RELIC's gt_t.
 * (Note the similarity between all of these. Couldn't figure out a nice way to work around code duplication.)
 */
class GTT {
 public:
  gt_t n;

 public:
  GTT() {
    gt_null(n);
    gt_new(n);
    // WARNING: Zero() relies on GTT()-created objects to be zero.
    gt_zero(n);
  }

  GTT(const GTT &c) : GTT() {
    // WARNING: RELIC asks for non-const, even though it does not modify the args. Thus, we remove the const using a
    // cast.
    gt_copy(n, const_cast<gt_t &>(c.n));
  }

  ~GTT() { gt_free(n); }

 public:
  static const GTT &Zero();

  bool isZero() const {
    // FIXME: RELIC: fp12_cmp_dig takes non-const args
    return gt_cmp_dig(const_cast<gt_t &>(n), 0) == CMP_EQ;
  }

  bool isUnity() const {
    // FIXME: RELIC: fp12_cmp_dig takes non-const args
    return gt_is_unity(const_cast<gt_t &>(n));
  }

 public:
  // Implicitly cast GTT objects to gt_t types so that we can keep the same syntax when calling RELIC functions
  operator gt_t &() { return n; }
  operator const gt_t &() const { return n; }

  bool operator==(const GTT &rhs) const {
    // WARNING: RELIC asks for non-const, even though it does not modify the args. Thus, we remove the const using a
    // cast.
    return gt_cmp(const_cast<gt_t &>(n), const_cast<gt_t &>(rhs.n)) == CMP_EQ;
  }

  bool operator!=(const GTT &rhs) const {
    // WARNING: RELIC asks for non-const, even though it does not modify the args. Thus, we remove the const using a
    // cast.
    return gt_cmp(const_cast<gt_t &>(n), const_cast<gt_t &>(rhs.n)) != CMP_EQ;
  }
};

/**
 * Easily print RELIC types using std::ostream objects like std::cout
 */
std::ostream &operator<<(std::ostream &o, const BNT &num);
std::ostream &operator<<(std::ostream &o, const G1T &num);
std::ostream &operator<<(std::ostream &o, const G2T &num);
std::ostream &operator<<(std::ostream &o, const GTT &num);

} /* namespace Relic */
} /* namespace BLS */
