#pragma once

#include <iostream>
#include <ctime>
#include <vector>
#include <sstream>

#include <NTL/ZZ.h>
#include <NTL/ZZ_p.h>
#include <NTL/ZZ_pX.h>
#include <NTL/vec_ZZ_p.h>
#include <NTL/vector.h>
#include <NTL/BasicThreadPool.h>

#include <xutils/AutoBuf.h>
#include <xutils/NotImplementedException.h>

#include <utt/PolyCrypto.h>

using NTL::ZZ_p;
using NTL::ZZ_pX;
using NTL::ZZ;
using NTL::vec_ZZ_p;
using namespace std;

namespace libutt {

/**
 * Some useful NTL functions to know (see https://www.shoup.net/ntl/doc/ZZ_pX.cpp.html)
 * NOTE: Unfortunately, as far as I can tell, there's no way to compute f(w_i) fast using FFT in libntl.
 * It seems that NTL uses a complicated representation for the f(w_i) values and doesn't expose them as ZZ_p's.
 *
 * ZZ_pX(INIT_SIZE, n) -> initializes to zero, but space is pre-allocated
 *
 * const ZZ_pX& ZZ_pX::zero() -> returns const reference to 0 polynomial
 *
 * void ZZ_pX::SetMaxLength(long n) -> f.SetMaxLength(n) pre-allocate spaces for n coefficients. The polynomial that f represents is unchanged.
 *
 * clear(ZZ_pX& x) -> x = 0
 * set(ZZ_pX& x) -> x = 1
 * random(ZZ_pX& x, long degBound) -> picks random poly of degree = degBound - 1 (documentation is confusing; it says < degBound, which is not precise; but code generates exactly degBound - 1)
 *
 * rem(r, a, b) -> r = a%b
 * DivRem(q, r, a, b) -> q = a/b, r = a%b
 * div(q, a, b) -> q = a/b
 * diff(d, f) -> d = derivative of f
 */


template<typename FieldT>
void conv_single_fr_zp(const FieldT& a, ZZ_p& b, mpz_t& rop, ZZ& mid) {
    (a.as_bigint()).to_mpz(rop);
    char *s = mpz_get_str(NULL, 10, rop);

    mid = NTL::conv<ZZ>(s);
    b = NTL::conv<ZZ_p>(mid);

    void (*freefunc)(void *, size_t);
    mp_get_memory_functions(NULL, NULL, &freefunc);
    freefunc(s, strlen(s) + 1);
}

template<typename FieldT>
void convLibffToNtl(const vector<FieldT> &a, ZZ_pX &pa)
{
    (void)a;
    (void)pa;
    throw libxutils::NotImplementedException();
}

/*
 * DEPRECATED: This uses strings and is slower than the implementation above.
 */
template<typename FieldT>
void convLibffToNtl_slow(const vector<FieldT> &a, ZZ_pX &pa)
{
    pa = ZZ_pX(NTL::INIT_MONO, (long)(a.size() - 1));

    mpz_t rop;
    mpz_init(rop);
    ZZ mid;
    
    for (size_t i = 0; i < a.size(); i++) {
        conv_single_fr_zp(a[i], pa[static_cast<long>(i)], rop, mid);
    }

    mpz_clear(rop);
}

/**
 * Converts an NTL vec_ZZ_p vector of field elements to a vector of libff field elements.
 */
template<typename FieldT>
void convNtlToLibff(const ZZ_p& zzp, FieldT& ff) {
    long numBytes = NumBytes(ZZ_p::modulus());
    AutoBuf<unsigned char> buf(static_cast<int>(numBytes));
    buf.zeroize();

    mpz_t interm;
    mpz_init(interm);

    const ZZ& rep = NTL::rep(zzp);

    // NOTE: Puts the least significant byte in buf[0]
    NTL::BytesFromZZ(buf, rep, numBytes);

    // convert byte array to mpz_t
    mpz_import(interm,
        /*count =*/static_cast<size_t>(numBytes),
        /*order =*/-1,
        /*size  =*/1,
        /*endian=*/0,
        /*nails =*/0,
        buf);

    ff = libff::bigint<FieldT::num_limbs>(interm);
    
    mpz_clear(interm);
}

template<typename FieldT>
void convNtlToLibff(const vec_ZZ_p& pn, vector<FieldT>& pf) {
    // allocate enough space for libff polynomial
    pf.resize(static_cast<size_t>(pn.length()));

    mpz_t interm;
    mpz_init(interm);

    long numBytes = NumBytes(ZZ_p::modulus());
    AutoBuf<unsigned char> buf(static_cast<int>(numBytes));
    buf.zeroize();

    for (size_t i = 0; i < pf.size(); i++) {
        const ZZ_p& coeff = pn[static_cast<long>(i)];
        const ZZ& coeffRep = NTL::rep(coeff);

        // NOTE: Puts the least significant byte in buf[0]
        NTL::BytesFromZZ(buf, coeffRep, numBytes);

        // convert byte array to mpz_t
        mpz_import(interm,
            /*count =*/static_cast<size_t>(numBytes),
            /*order =*/-1,
            /*size  =*/1,
            /*endian=*/0,
            /*nails =*/0,
            buf);

        pf[i] = libff::bigint<FieldT::num_limbs>(interm);
    }

    mpz_clear(interm);
}

/**
 * Converts an NTL ZZ_pX polynomial to libff polynomial.
 */
template<typename FieldT>
void convNtlToLibff(const ZZ_pX& pn, vector<FieldT>& pf) {
    convNtlToLibff(pn.rep, pf);
}

/**
 * DEPRECATED: Converts an NTL ZZ_pX polynomial to libff polynomial.
 * This uses string streams and is slower than the implementation above.
 */
template<typename FieldT>
void convNtlToLibff_slow(const ZZ_pX &pa, vector<FieldT> &a) {
    a.resize((size_t)(NTL::deg(pa)+1));

    stringstream ss;
    for (size_t i = 0; i < a.size(); i++) {
        ss << pa[(long)i];

        a[i] = libff::bigint<FieldT::num_limbs>(ss.str().c_str());
        ss.str("");
    }
}

template<class Group, class FieldT>
Group multiExp(
    const std::vector<Group>& bases,
    const vec_ZZ_p& exps)
{
    std::vector<FieldT> expsff;
    convNtlToLibff(exps, expsff);

    long minSize = static_cast<long>(bases.size());
    if(exps.length() < minSize)
        minSize = exps.length();

    return multiExp<Group>(bases.cbegin(), bases.cbegin() + minSize,
        expsff.cbegin(), expsff.cbegin() + minSize);
}

template<class Group, class FieldT>
Group multiExp(
    const std::vector<Group>& bases,
    const ZZ_pX& exps)
{
    return multiExp<Group, FieldT>(bases, exps.rep);
}

} // end of namespace libutt
