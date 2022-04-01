#pragma once

#include <vector>
#include <algorithm>
#include <tuple>
#include <vector>
#include <iostream>

#include <utt/NtlLib.h>
#include <libfqfft/polynomial_arithmetic/basic_operations.hpp>
#include <libfqfft/polynomial_arithmetic/naive_evaluate.hpp>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/Utils.h>

namespace libutt {

    /**
     * Does an FFT: i.e., evaluates and returns p(\omega^i) for all i = 0, ..., N-1
     * but returns only the first n <= N evaluations.
     * Here, N is the smallest 2^k such that n >= N
     * 
     * @param   poly    polynomial p(X) as a vector of its coefficients
     * @param   n       # of evaluations, which implicitly determines the primitive Nth root of unity \omega (which must be smallest N of the form 2^k >= n)
     * @param   vals    stores the evaluations at the roots of unity here; does not need to be pre-allocated/resized
     */
    template<class FieldT>
    std::vector<FieldT> poly_fft(const std::vector<FieldT>& poly, size_t n) {
        size_t N = Utils::smallestPowerOfTwoAbove(n);

        FieldT omega = libff::get_root_of_unity<FieldT>(N);

        if(N < poly.size()) {
            logerror << "N: " << N << endl;
            logerror << "deg(poly): " << poly.size() << endl;
            throw std::runtime_error("N has to be greater than polynomial degree");
        }

        // the FFT is computed in place, which is why we overwrite the 'vals' vector here
        std::vector<FieldT> vals = poly;
        vals.resize(N, FieldT::zero()); // pads the extra space, if any, with zeros

#ifdef MULTICORE
        libfqfft::_basic_parallel_radix2_FFT(vals, omega);
#else
        libfqfft::_basic_serial_radix2_FFT(vals, omega);
#endif

        vals.resize(n);
        return vals;
    }

    template<typename FieldT>
    std::vector<FieldT> poly_inverse_fft(const std::vector<FieldT>& evals, size_t N)
    {
        assertTrue(Utils::isPowerOfTwo(N));
        
        FieldT omega_inv = libff::get_root_of_unity<FieldT>(N).inverse();

        std::vector<FieldT> poly = evals;

#ifdef MULTICORE
        libfqfft::_basic_parallel_radix2_FFT(poly, omega_inv);
#else
        libfqfft::_basic_serial_radix2_FFT(poly, omega_inv);
#endif

        const FieldT N_inv = FieldT(static_cast<long>(N)).inverse();

        std::transform(poly.begin(), poly.end(), poly.begin(),
            [&N_inv](const FieldT& f) -> FieldT { return f * N_inv; });

        libfqfft::_condense(poly);
        return poly;
    }
    
    template<typename FieldT>
    std::vector<FieldT> poly_constant(const FieldT& c) {
        std::vector<FieldT> f;
        
        f.push_back(c);

        return f;
    }

    template<typename FieldT>
    Fr poly_eval(const std::vector<FieldT>& f, const Fr& x) {
        return libfqfft::evaluate_polynomial(f.size(), f, x);
    }

    template<typename FieldT>
    std::vector<FieldT> poly_scale(const Fr& c, const std::vector<FieldT>& f) {
        auto s = f;
        
        std::transform(s.begin(), s.end(), s.begin(),
            [&c](const FieldT& coeff) -> FieldT { return c * coeff; });

//#ifndef NDEBUG
//        // Schwartz-Zippel test: s(\tau) should equal c*f(\tau), for a random tau if s(X) = c*f(X)
//        Fr tau = Fr::random_element();
//        assertEqual(poly_eval(s, tau), Fr(c)*poly_eval(f, tau));
//#endif

        return s;
    }
    
    template<typename FieldT>
    std::vector<FieldT> poly_negate(const std::vector<FieldT>& f) {
        auto f_neg = f;

        std::transform(f_neg.begin(), f_neg.end(), f_neg.begin(),
            [](const FieldT& c) -> FieldT { return -c; });

        return f_neg;
    }

    template<typename FieldT>
    std::vector<FieldT> poly_mult(const vector<FieldT> &a, const vector<FieldT> &b) {
        std::vector<FieldT> c;
    
        libfqfft::_polynomial_multiplication(c, a, b);

        return c;
    }

    /**
     * O(n^2) naive polynomial multiplication
     */
    template<typename FieldT>
    std::vector<FieldT> poly_mult_naive(const vector<FieldT> &a, const vector<FieldT> &b) {
        // WARNING: This is not an 'in-place' algorithm; we must store product in new vector and return it
        std::vector<FieldT> c(a.size() + b.size() - 1, FieldT::zero());
        for (size_t i = 0; i < a.size(); i++)
            for (size_t j = 0; j < b.size(); j++)
                c[i+j] += a[i] * b[j];
        return c;
    }
    
    /**
     * O(n^2) naive polynomial division
     *
     * Returns tuple of quotient and remainder (in this order)
     */
    template<typename FieldT>
    std::tuple<std::vector<FieldT>, std::vector<FieldT>> poly_div_naive(const vector<FieldT> &a, const vector<FieldT> &b) {
        std::vector<FieldT> q, r;

        libfqfft::_polynomial_division(q, r, a, b);

        return std::make_tuple(q, r);
    }
    
    template<typename FieldT>
    std::vector<FieldT> poly_div_naive_quo(const vector<FieldT> &a, const vector<FieldT> &b) {
        return std::get<0>(poly_div_naive(a, b));
    }
    
    template<typename FieldT>
    std::vector<FieldT> poly_div_naive_rem(const vector<FieldT> &a, const vector<FieldT> &b) {
        return std::get<1>(poly_div_naive(a, b));
    }
    
    template<typename FieldT>
    std::vector<FieldT> poly_add(const vector<FieldT> &a, const vector<FieldT> &b) {
        std::vector<FieldT> c;

        libfqfft::_polynomial_addition(c, a, b);

        return c;
    }

    template<typename FieldT>
    std::vector<FieldT> poly_subtract(const vector<FieldT> &a, const vector<FieldT> &b) {
        std::vector<FieldT> c;

        libfqfft::_polynomial_addition(c, a, poly_negate(b));

        return c;
    }

    /**
     * In the BFGW range proof protocol, we often need to work with f(X\omega) rather f(X).
     * This function returns f(X\omega) given f(X).
     */
    template<typename FieldT>
    std::vector<FieldT> poly_Xomega(const std::vector<FieldT>& f, const Fr& omega) {
        auto g = f;

        auto omegaAcc = Fr::one();

        for(size_t i = 0; i < g.size(); i++) {
            g[i] = g[i] * omegaAcc;

            omegaAcc = omegaAcc * omega;
        }

//#ifndef NDEBUG
//        // Schwartz-Zippel test: f(\tau\omega) should equal g(\tau), for a random tau if f(X\omega) = g(X)
//        Fr tau = Fr::random_element();
//        assertEqual(poly_eval(f, tau*omega), poly_eval(g, tau));
//#endif

        return g;
    }

    /**
     * Computes Lagrange coefficients L_i^T(0) for the set of points (\omega_N^i)_{i\in T} in O(k^2) time, where k = |T| and N = 2^k is the smallest power greater than n
     *
     * @param n             the number of total points on the polynomial
     * @param T             the f(\omega_N^i) polynomial evaluations given (less than n)
     *
     * @return              the Lagrange coefficients
     */
    std::vector<Fr> lagrange_coefficients_naive(size_t n, const std::vector<size_t>& T);

    /**
     * Computes Lagrange coefficients L_i^T(0) for the set of points (\omega_N^i)_{i\in T} in O(k^2) time, where k = |T|
     *
     * @param allOmegas     all N Nth roots of unity
     * @param T             the indices i 
     *
     * @return              the Lagrange coefficients
     */
    std::vector<Fr> lagrange_coefficients_naive(const std::vector<Fr>& allOmegas, const std::vector<size_t>& T);

    // WARNING: Slower than NTL::BuildFromRoots(), but not sure why
    ZZ_pX poly_from_roots_ntl(const vec_ZZ_p& roots, long startIncl, long endExcl);

    template<typename FieldT>
    std::vector<FieldT> poly_from_roots(const vector<FieldT>& r) {
        std::vector<FieldT> a;
        vec_ZZ_p roots;
        roots.SetLength(static_cast<long>(r.size()));

        mpz_t rop;
        mpz_init(rop);
        ZZ mid;

        for(size_t i = 0; i < r.size(); i++) {
            libutt::conv_single_fr_zp(r[i], roots[static_cast<long>(i)], rop, mid);
        }
        mpz_clear(rop);

        ZZ_pX acc(NTL::INIT_MONO, 0);
        NTL::BuildFromRoots(acc, roots);
        //acc = poly_from_roots_ntl(roots, 0, roots.length());  // WARNING: Slower than NTL::BuildFromRoots()

        libutt::convNtlToLibff(acc, a);

        return a;
    }

    // interp(roots[0,n)) = interp(roots[0, n/2)) * interp(roots[n/2+1, n))
    template<typename FieldT>
    std::vector<FieldT> poly_from_roots_slow(const vector<FieldT>& roots, size_t startIncl, size_t endExcl) {
        // base case is startIncl == endExcl - 1, so return (x - root)
        if(startIncl == endExcl - 1) {
            std::vector<FieldT> monom(2);
            monom[0] = -roots[startIncl];
            monom[1] = 1;
            return monom;
        }

        std::vector<FieldT> p;

        size_t middle = (startIncl + endExcl) / 2;

        libfqfft::_polynomial_multiplication_on_fft(
            p,
            poly_from_roots_slow(roots, startIncl, middle),
            poly_from_roots_slow(roots, middle, endExcl)
            );

        return p;
    }

    template<typename FieldT>
    std::vector<FieldT> poly_from_roots_slow(const vector<FieldT>& roots) {
        return poly_from_roots(roots, 0, roots.size());
    }

    /**
     Calculates derivative of polynomial f and stores in d
     */
    template<typename FieldT>
    void poly_differentiate(const vector<FieldT> &f, vector<FieldT> &d) {
        assertStrictlyGreaterThan(f.size(), 0);

        d.resize(f.size() - 1);
        for (size_t i = 0; i < d.size(); i++) {
            d[i] = f[i + 1] * (static_cast<int>(i) + 1);
        }
    }

    /**
     * Returns true if the polynomial is of the type X^m - c, where m != 0
     */
    template<class FieldT>
    bool poly_is_xnc(const vector<FieldT>& p) {
        if(p.size() < 2)
            return false;

        if(p[p.size() - 1] != FieldT::one())
            return false;

        for(size_t j = 1; j < p.size() - 1; j++) {
            if(p[j] != FieldT::zero())
                return false;
        }

        return true;
    }

    /**
     * Divides a(X) (of degree n) by b(X) of (degree m), where b(X) is of the form X^m + c.
     *
     * Returns the quotient in q and the remainder in r.
     */
    template<class FieldT>
    std::tuple<std::vector<FieldT>, std::vector<FieldT>> poly_div_xnc(const vector<FieldT>& a, const std::vector<FieldT>& b) {
        assertStrictlyGreaterThan(a.size(), 0);
        assertTrue(poly_is_xnc(b));

        std::vector<FieldT> q;
        std::vector<FieldT> r;

        if(a.size() < b.size()) {
            q.resize(1);
            q[0] = 0;
            r = a;
            return std::make_tuple(q, r);
        }

        size_t n = a.size() - 1;
        size_t m = b.size() - 1; // the degree of the xnc poly
        FieldT c = b[0];
        size_t hi = n, lo = n - m;
        q.resize((n - m) + 1, FieldT::zero());
        r = a;

        //logdbg << "Dividing degree n = " << n << " by degree m = " << m << endl;
        //logdbg << "Remainder has degree m - 1 = " << m - 1 << endl;
        //logdbg << "Quotient has degree n - m = " << n - m << endl;

        // We are just executing naive polynomial division. Initially, q is all zeros.
        for(size_t i = 0; i < q.size(); i++) {
            q[q.size() - i - 1] = r[hi];
            // r[hi] is removed and need to adjust r[lo]
            r[lo] = r[lo] - c*r[hi];
            
            r.resize(r.size() - 1);

            hi--;
            lo--;

#ifndef NDEBUG
            // assert a = q*b+r invariant holds
            vector<FieldT> rhs;
            libfqfft::_polynomial_multiplication(rhs, b, q);
            libfqfft::_polynomial_addition(rhs, rhs, r);
    
            assertEqual(a, rhs);
#endif
        }

        assertEqual(r.size(), m);

        return std::make_tuple(q, r);
    }

    template<class FieldT>
    void poly_print(const std::vector<FieldT>& p) {
        size_t n = p.size();

        for(size_t i = 0; i < n - 1; i++) {
            auto deg = (n-1) - i;
            auto c = p[deg];

            if(deg == 1)
                std::cout << c << " x + ";
            else
                std::cout << c << " x^" << deg << " + ";
        }

        std::cout << p[0];

        std::cout << " of degree " << n-1 << std::flush;
    }

    /**
     * Prints a polynomial in a more-user friendly way if it has coefficients that
     * are w_n^k roots of unity, where n = allRoots.size().
     */
    template<class FieldT>
    void poly_print_wnk(const std::vector<FieldT>& p, const std::vector<FieldT>& allRoots) {
        size_t d = p.size();
        FieldT minusOne = FieldT(-1);
        for(size_t i = 0; i < d; i++) {
            auto deg = (d-1) - i;
            auto c = p[deg];

            // don't print (0 x^i) coeffs
            if(c == 0)
                continue;

            // identify if the coefficient is a root of unity (special treatment for 1 and -1)
            auto it = std::find(allRoots.begin(), allRoots.end(), c);

            // print the coefficient
            if(it != allRoots.end()) {
                std::string coeff;
                bool isPositive = (c != minusOne);

                if (c == FieldT::one() || c == minusOne) {
                    if(deg == 0) {
                        coeff = "1";
                    } else {
                        coeff = "";
                    }
                } else {
                    size_t pos = static_cast<size_t>(it - allRoots.begin());
                    size_t n = allRoots.size();
                    coeff = "w_" + std::to_string(n) + "^" + std::to_string(pos);
                }

                if(deg != d-1 || !isPositive) {
                    std::cout << (isPositive ? " + " : " - ");
                }

                std::cout << coeff;

            } else {
                throw std::runtime_error("Non-root-of-unity coefficient found");
            }

            // print x^deg after the coefficient
            if(deg == 1) {
                std::cout << " x";
            } else if(deg == 0) {
                // for the last coefficient we don't print any x^deg (because it would be x^0)
            } else {
                std::cout << " x^" << deg; 
            }
        }

        std::cout << " of degree " << d-1 << std::flush;
    }

    template<class T>
    bool poly_equal_slow(const std::vector<T>& a, const std::vector<T>& b) {
        std::vector<T> res;
        libfqfft::_polynomial_subtraction(res, a, b);
        
        return res.size() == 0;
    }

}
