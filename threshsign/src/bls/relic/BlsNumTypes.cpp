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

#include "threshsign/bls/relic/BlsNumTypes.h"

#include "Timer.h"
#include "Utils.h"
#include "AutoBuf.h"
#include "XAssert.h"
#include "Logger.hpp"

using std::endl;

namespace BLS {
namespace Relic {

template <class T>
static std::string toStringHelper(const T& num) {
  int size = num.getByteCount();
  AutoByteBuf buf(size);

  num.toBytes(buf, size);
  return Utils::bin2hex(buf, size);
}

template <class T>
static void fromStringHelper(T& num, const std::string& str) {
  if (str.size() % 2) {
    throw std::runtime_error("Cannot deserialize G1 elliptic curve point from odd-sized hexadecimal string");
  }

  AutoByteBuf buf(static_cast<int>(str.size() / 2));
  int bufSize = static_cast<int>(str.size() / 2);
  Utils::hex2bin(str, buf, bufSize);
  num.fromBytes(buf, bufSize);
}

/**
 * BNT
 */

BNT::BNT(int d) : BNT() {
  assertGreaterThanOrEqual(d, 0);
  assertGreaterThanOrEqual(sizeof(dig_t), sizeof(d));
  bn_set_dig(n, static_cast<dig_t>(d));
}

void BNT::toBytes(unsigned char* buf, int capacity) const {
  int size = getByteCount();
  if (capacity < size) {
    LOG_ERROR(BLS_LOG,
              "Expected buffer of size " << size << " bytes for serializing BNT object. You provided " << capacity
                                         << " bytes");
    throw std::logic_error("Buffer not large enough for serializing BNT");
  }

  memset(buf, 0, static_cast<size_t>(capacity));
  bn_write_bin(buf, capacity, n);
}

std::string BNT::toString(int base) const {
  int size = bn_size_str(n, base);
  AutoCharBuf buf(size);
  // bn_write_str(reinterpret_cast<unsigned char*>(buf.getBuf()), size, n, base);
  bn_write_str(buf.getBuf(), size, n, base);
  std::string str(buf);
  return str;
}

void BNT::fromString(const std::string& str, int base) {
  bn_read_str(n, str.c_str(), static_cast<int>(str.size()), base);
}

BNT& BNT::FastModuloMonty(const BNT& m, const BNT& u) {
  // RELIC API quirk: Need to convert 'n' to Montgomery form
  BNT montyN;
  bn_mod_monty_conv(montyN, n, m);
  bn_mod(n, montyN, m, u);
  return *this;
}

BNT& BNT::FastModuloBarrett(const BNT& m, const BNT& u) {
  bn_mod_barrt(n, n, m, u);
  return *this;
}

BNT& BNT::FastModuloPmers(const BNT& m, const BNT& u) {
  bn_mod_pmers(n, n, m, u);
  return *this;
}

BNT& BNT::FastModulo(const BNT& m, const BNT& u) {
  /**
   * NOTE: BNT::SlowModulo always uses RELIC's bn_mod_basic, no matter what RELIC compilation flags are used.
   *
   * Montgomery modular reduction functions: FastModulo much slower than SlowModulo
   * bn_mod_monty
   * bn_mod_monty_back
   * bn_mod_monty_basic
   * bn_mod_monty_comba
   * bn_mod_monty_conv
   * bn_mod_pre_monty
   *
   * Barrett modular reduction function: slower than Montgomery, but SlowModulo almost as fast as FastModulo
   * bn_mod_barrt
   * bn_mod_pre_barrt
   *
   *
   * PMERS modular reduction: much much slower than Barrett (20x), FastModulo is insanely slower than SlowModulo
   * bn_mod_pmers
   * bn_mod_pre_pmers
   */

#if BN_MOD == MONTY
  FastModuloMonty(m, u);
#else
  bn_mod(n, n, m, u);
#endif

  return *this;
}

// WARNING: Microbenchmarking this must be done on a single thread, so be careful!
//#define INVERT_MICROBENCH

#ifdef INVERT_MICROBENCH
AveragingTimer __inv_microbench_timer_dig("bn_gcd_ext_dig time:   ");
AveragingTimer __inv_microbench_timer_leh("bn_gcd_ext_lehme time: ");
#endif

BNT BNT::invertModPrime(const dig_t& a, const BNT& p) {
  assertTrue(bn_is_prime(p));

  BNT gcd, inv, ign;
  // NOTE: This is faster than Lehme method
  // FIXME: RELIC: Setting 'ing' to nullptr seems to segfault for no reason.
#ifdef INVERT_MICROBENCH
  {
    __inv_microbench_timer_dig.startLap();
#endif

    // NOTE: This is faster than bn_gcd_ext_lehme (but only works on small dig_t numbers)
    bn_gcd_ext_dig(gcd, ign, inv, p, a);

#ifdef INVERT_MICROBENCH
    __inv_microbench_timer_dig.endLap();
    if (__inv_microbench_timer_dig.numIterations() % 1000 == 0) {
      logperf << __inv_microbench_timer_dig << endl;
    }
  }
#endif
  assertEqual((BNT(a) * inv).SlowModulo(p), BNT::One());

  if (inv >= p || inv < 0) {
    inv.SlowModulo(p);
  } else {
    // logperf << "Avoided reducing mod p (dig_t)" << endl;
  }

  return inv;
}

BNT BNT::invertModPrime(const BNT& p) const {
  // n * inv + [?] * p == 1 <=> n * inv (mod p) + [?] * p (mod p) == 1 (mod p) <=> n * inv (mod p) == 1 (mod p)

  assertTrue(bn_is_prime(p));
  // WARNING: 'bn_gcd_ext_basic' does not seem to work when a < 0, |a| > m (e.g., a = -9, m = 7) but works when |a| < m
  assertFalse(*this < 0 && -(*this) > p);

  // Picked fastest GCD (implemented in various ways in RELIC: basic, Lehmer and Stein)
  BNT gcd, inv;
  // bn_gcd_ext_basic(gcd, inv, nullptr, n, p);
  // bn_gcd_ext_stein(gcd, inv, nullptr, n, p);  // faster than basic
#ifdef INVERT_MICROBENCH
  {
    __inv_microbench_timer_leh.startLap();
#endif

    bn_gcd_ext_lehme(gcd, inv, nullptr, n, p);  // faster than stein

#ifdef INVERT_MICROBENCH
    __inv_microbench_timer_leh.endLap();
    if (__inv_microbench_timer_leh.numIterations() % 1000 == 0) {
      logperf << __inv_microbench_timer_leh << endl;
    }
  }
#endif
  assertEqual(((*this) * inv).SlowModulo(p), BNT::One());

  // Q: Is it faster to compute n^{fieldOrder-1}? A: Tested and nope!
  //    BNT inv;
  //    BNT pm2;
  //    bn_sub_dig(pm2, p, static_cast<dig_t>(2));
  //    bn_mxp(inv, n, pm2, p);
  if (inv >= p || inv < 0) {
    inv.SlowModulo(p);
  } else {
    // logperf << "Avoided reducing mod p (Lehme)" << endl;
  }
  return inv;
}

/**
 * G1T
 */

size_t G1T::toBytes(unsigned char* buf, int size) const {
  assertGreaterThanOrEqual(size, getByteCount());
  g1_write_bin(buf, size, n, 1);
  return static_cast<size_t>(getByteCount());
  // LOG_TRACE(BLS_LOG, "Wrote G1T of " << size << " bytes");
}

void G1T::fromBytes(const unsigned char* buf, int size) {
  // LOG_TRACE(BLS_LOG, "Reading G1T of " << size << " bytes");
  g1_read_bin(n, buf, size);
}

std::string G1T::toString() const { return toStringHelper(*this); }

void G1T::fromString(const std::string& str) { fromStringHelper(*this, str); }

/**
 * G2T
 */

void G2T::toBytes(unsigned char* buf, int size) const {
  assertGreaterThanOrEqual(size, getByteCount());
  // FIXME: RELIC: g2_write_bin should take const g2_t
  g2_write_bin(buf, size, const_cast<g2_t&>(n), 1);
}
void G2T::fromBytes(const unsigned char* buf, int size) { g2_read_bin(n, buf, size); }

std::string G2T::toString() const { return toStringHelper(*this); }
void G2T::fromString(const std::string& str) { fromStringHelper(*this, str); }

/**
 * GTT
 */

const GTT& GTT::Zero() {
  // FIXME: RELIC: Missing gt_is_zero define (as fp2_is_zero or fp12_is_zero)
  static GTT zero;
  assertTrue(gt_cmp_dig(zero, 0) == CMP_EQ);
  return zero;
}

/**
 * std::ostream overload
 */

std::ostream& operator<<(std::ostream& o, const BNT& num) {
  o << num.toString();
  return o;
}

std::ostream& operator<<(std::ostream& o, const G1T& num) {
  // Print as hexadecimal string
  o << num.toString();
  return o;
}

std::ostream& operator<<(std::ostream& o, const G2T& num) {
  o << num.toString();
  return o;
}

std::ostream& operator<<(std::ostream& o, const GTT& num) {
  // Print as hexadecimal string
  // FIXME: RELIC: For some reason gt_size_bin fails on zero
  if (num.isZero()) {
    o << "00";
    return o;
  }

  // FIXME: RELIC: For some reason gt_size_bin fails on one
  if (num.isUnity()) {
    o << "01";
    return o;
  }

  // FIXME: RELIC: gt_size_bin should take const gt_t
  int size = gt_size_bin(const_cast<GTT&>(num), 1);
  AutoByteBuf buf(size);

  // FIXME: RELIC: gt_size_bin should take const gt_t
  gt_write_bin(buf, size, const_cast<GTT&>(num), 1);
  o << Utils::bin2hex(buf, size);
  return o;
}

} /* namespace Relic */
} /* namespace BLS */
