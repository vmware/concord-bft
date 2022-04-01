#pragma once

#include <vector>
#include <algorithm>

#ifdef MULTICORE
#include <omp.h>
#endif

#include <libff/algebra/field_utils/field_utils.hpp>
#include <libfqfft/tools/exceptions.hpp>

/**
 * Let F_p denote a field of prime order p.
 * The Discrete Fourier Transform (DFT) of a vector 'a[0...(n-1)]' where n = 2^k and a_i \in F_p is defined as:
 *
 *   a_i = \sum_{j=0}^{n-1} { \omega_n^{i j} \cdot a_j }
 *
 * Here, \omega is a primitive nth root of unity in F_p.
 * Typically, the a_i's are also from the field F_p.
 * However, in some cases we might want the a_j's to be elements of a group, say an elliptic curve.
 * In that case, \omega_n^{i j} \cdot a_j is actually a scalar multiplication in the elliptic curve group.
 *
 * This is why we use GroupType here to indicate the type of group elements the a_i's are.
 * Note: We refer to the DFT as FFT in this code.
 */
namespace libutt {

using libfqfft::DomainSizeException;

/**
 * A modification of libfqfft's code, which is based on pseudocode from [CLRS 2n Ed, pp. 864].
 * This is the non-parallelized version.
 * Also, note that it's the caller's responsibility to multiply by 1/N when using this for an inverse DFT.
 */
template<typename GroupT, typename FieldT>
void FFT_serial(std::vector<GroupT> &a, const FieldT &omega)
{
    const size_t n = a.size(), logn = libff::log2(n);
    if (n != (1u << logn)) 
        throw DomainSizeException("expected n == (1u << logn)");

    /* swapping in place (from Storer's book) */
    for (size_t k = 0; k < n; ++k)
    {
        const size_t rk = libff::bitreverse(k, logn);
        if (k < rk)
            std::swap(a[k], a[rk]);
    }

    size_t m = 1; // invariant: m = 2^{s-1}
    for (size_t s = 1; s <= logn; ++s)
    {
        // w_m is 2^s-th root of unity now
        const FieldT w_m = omega^(n/(2*m));

        asm volatile  ("/* pre-inner */");
        for (size_t k = 0; k < n; k += 2*m)
        {
            FieldT w = FieldT::one();
            for (size_t j = 0; j < m; ++j)
            {
                const GroupT t = w * a[k+j+m];
                a[k+j+m] = a[k+j] - t;
                a[k+j] = a[k+j] + t;
                w *= w_m;
            }
        }
        asm volatile ("/* post-inner */");
        m *= 2;
    }
}

template<typename GroupT, typename FieldT>
void FFT(std::vector<GroupT> &a)
{
    size_t n = libff::get_power_of_two(a.size());
    FieldT omega = libff::get_root_of_unity<FieldT>(n); 

#ifdef MULTICORE
# error "Did not test the parallel FFT path yet"
#else
    FFT_serial<GroupT, FieldT>(a, omega);
#endif
}

template<typename GroupT, typename FieldT>
void invFFT(std::vector<GroupT> &a)
{
    size_t n = libff::get_power_of_two(a.size());
    FieldT omega = libff::get_root_of_unity<FieldT>(n); 

#ifdef MULTICORE
# error "Did not test the parallel FFT path yet"
#else
    FFT_serial<GroupT, FieldT>(a, omega.inverse());
#endif

    const FieldT sconst = FieldT(static_cast<long>(n)).inverse();
    for(size_t i = 0; i < n; i++) {
        a[i] = sconst * a[i];
    }
}

template<typename GroupT, typename FieldT>
void FFT_parallel(std::vector<GroupT> &a, const FieldT &omega)
{
#ifdef MULTICORE
    const size_t num_cpus = omp_get_max_threads();
#else
    const size_t num_cpus = 1;
#endif
    const size_t log_cpus = ((num_cpus & (num_cpus - 1)) == 0 ? libff::log2(num_cpus) : libff::log2(num_cpus) - 1);

    if (log_cpus == 0)
        FFT_serial(a, omega);
    else
        FFT_parallel_inner(a, omega, log_cpus);
}

template<typename GroupT, typename FieldT>
void FFT_parallel_inner(std::vector<FieldT> &a, const FieldT &omega, const size_t log_cpus)
{
    const size_t num_cpus = 1ul<<log_cpus;

    const size_t m = a.size();
    const size_t log_m = libff::log2(m);
    if (m != 1ul<<log_m) 
        throw DomainSizeException("expected m == 1ul<<log_m");

    if (log_m < log_cpus)
    {
        FFT_serial(a, omega);
        return;
    }

    std::vector<std::vector<FieldT> > tmp(num_cpus);
    for (size_t j = 0; j < num_cpus; ++j)
    {
        tmp[j].resize(1ul<<(log_m-log_cpus), FieldT::zero());
    }

#ifdef MULTICORE
    #pragma omp parallel for
#endif
    for (size_t j = 0; j < num_cpus; ++j)
    {
        const FieldT omega_j = omega^j;
        const FieldT omega_step = omega^(j<<(log_m - log_cpus));

        FieldT elt = FieldT::one();
        for (size_t i = 0; i < 1ul<<(log_m - log_cpus); ++i)
        {
            for (size_t s = 0; s < num_cpus; ++s)
            {
                // invariant: elt is omega^(j*idx)
                const size_t idx = (i + (s<<(log_m - log_cpus))) % (1u << log_m);
                tmp[j][i] += elt * a[idx]; 
                elt *= omega_step;
            }
            elt *= omega_j;
        }
    }

    const FieldT omega_num_cpus = omega^num_cpus;

#ifdef MULTICORE
    #pragma omp parallel for
#endif
    for (size_t j = 0; j < num_cpus; ++j)
    {
        FFT_serial(tmp[j], omega_num_cpus);
    }

#ifdef MULTICORE
    #pragma omp parallel for
#endif
    for (size_t i = 0; i < num_cpus; ++i)
    {
        for (size_t j = 0; j < 1ul<<(log_m - log_cpus); ++j)
        {
            // now: i = idx >> (log_m - log_cpus) and j = idx % (1u << (log_m - log_cpus)), for idx = ((i<<(log_m-log_cpus))+j) % (1u << log_m)
            a[(j<<log_cpus) + i] = tmp[i][j];
        }
    }
}

} // end of libutt
