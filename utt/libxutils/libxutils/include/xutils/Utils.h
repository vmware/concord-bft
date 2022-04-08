/*
 * Utils.h
 *
 *  Created on: Oct 10, 2014
 *      Author: Alin Tomescu <alinush@mit.edu>
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>
#include <random>
#include <sstream>
#include <iterator>
#include <iomanip>
#include <string>
#include <set>
#include <chrono>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

// TODO: namespace into libxutils

class Utils {
public:
    /**
     * @param T is the type of the elements being averaged
     * @Param E is the type of the average itself
     */
    template<class T, class E>
    class MinMaxAvg {
    public:
        T min, max;
        E avg;

    public:
        static MinMaxAvg<T, E> compute(const std::vector<T>& v) {
            MinMaxAvg<T, E> mma;
            mma.min = v[0];
            mma.max = v[0];
            mma.avg = 0;

            for(auto it = v.begin(); it != v.end(); it++) {
                if(*it < mma.min) {
                    mma.min = *it;
                }
                if(*it > mma.max) {
                    mma.max = *it;
                }
                mma.avg += static_cast<E>(*it);
            }

            mma.avg = mma.avg / static_cast<E>(v.size());

            return mma;
        }
    };

public:
    template<class T>
    static std::string withCommas(T value) {
        std:: string numWithCommas = std::to_string(value);
        if(numWithCommas.length() > 3) {
            size_t insertPosition = numWithCommas.length() - 3;
            while(true) {
                numWithCommas.insert(insertPosition, ",");
                if(insertPosition > 3)
                    insertPosition -= 3;
                else
                    break;
            }
        }
        return numWithCommas;
    }

    template<class C, class V>
    static void removeFirst(C& c, const V& v) {
        auto it = std::find(c.begin(), c.end(), v);
        if (it != c.end()) {
            c.erase(it);
        }
    }

    static bool fileExists(const std::string& filename);

    /**
     * Returns the number of bits needed to represent a positive integer n:
     * Specifically, return 1 if n == 0 and floor(log_2(n)) otherwise.
     * e.g., numBits(1) = 1
     * e.g., numBits(2) = 2
     * e.g., numBits(3) = 2
     * e.g., numBits(4) = 3
     */
    static int numBits(int n) {
        assertGreaterThanOrEqual(n, 0);
        if(n == 0)
            return 1;

        return log2floor(n) + 1;
    }

    template <class T>
    static int numDigits(T number)
    {
        int digits = 0;

        if (number < 0)
            digits++;

        while(number > 0) {
            number /= 10;
            digits++;
        }

        return digits;
    }

    /**
     * Returns ceil(log2(n))
     */
    template<class T>
    static T log2ceil(T n) {
        if(n <= 0)
            throw std::logic_error("Cannot compute log2ceil(x) when x <= 0");

        int power = 1;
        int log2 = 0;
        while(power < n) {
            power *= 2;
            log2++;
        }
        return log2;
    }

    /**
     * Returns floor(log2(n))
     */
    template<class T>
    static T log2floor(T n) {
        if(n <= 0)
            throw std::logic_error("Cannot compute log2floor(x) when x <= 0");

        T res = 0;
        while (n /= 2) {
            res++;
        }
        return res;
    }

    /**
     * Returns 2^exp
     */
    static int pow2(int exp) {
        if(exp >= 0) {
            return 1 << exp;
        } else {
            throw std::logic_error("Power must be positive");
        }
    }

    /**
     * Returns true if n = 2^k for some non-negative integer k.
     */
    static bool isPowerOfTwo(size_t n) {
        return !(n == 0) && !(n & (n - 1));
    }

    /**
     * Used to be called: int nearestPowerOfTwo(int n)
     * Returns the smallest 2^i >= n
     */
    template<class T>
    static T smallestPowerOfTwoAbove(T n) {
        assertGreaterThanOrEqual(n, 0);

        T power = 1;
        while(power < n) {
            power *= 2;
        }
        return power;
    }

    /**
     * Returns the greatest 2^i <= n
     */
    template<class T>
    static T greatestPowerOfTwoBelow(T n) {
        if(n == 0)
            return 0;

        T ret = 1;

        while(n >>= 1)
            ret <<= 1;

        return ret;
    }

    template<class T>
    static std::string humanizeBytes(T bytes, size_t precision = 2) {
        std::ostringstream str;
        long double result = static_cast<long double>(bytes);
        const char * units[] = { "bytes", "KiB", "MiB", "GiB", "TiB" };
        unsigned int numUnits = sizeof(units)/sizeof(units[0]);
        unsigned int i = 0;
        while(result >= 1024.0 && i < numUnits - 1) {
            result /= 1024.0;
            i++;
        }

        str << std::fixed << std::setprecision(static_cast<int>(precision));
        str << result;
        str << " ";
        str << units[i];

        return str.str();
    }

    static std::string humanizeMicroseconds(std::chrono::microseconds::rep mus, size_t precision = 2) {
        std::ostringstream str;
        double result = static_cast<double>(mus);
        const char * units[] = { "mus", "ms", "secs", "mins", "hrs", "days", "years" };
        unsigned int numUnits = sizeof(units)/sizeof(units[0]);
        unsigned int i = 0;

        //logdbg << "Starting with " << result;
        //std::cout << " ";
        //std::cout << units[i] << std::endl;

        while(result >= 1000.0 && i < 2) {
            result /= 1000.0;
            i++;
            //logdbg << " * " << result;
            //std::cout << " ";
            //std::cout << units[i] << std::endl;
        }

        while(result >= 60.0 && i >= 2 && i < 4) {
            result /= 60.0;
            i++;
            //logdbg << " * " << result;
            //std::cout << " ";
            //std::cout << units[i] << std::endl;
        }

        if(i == 4 && result >= 24.0) {
            result /= 24.0;
            i++;
            //logdbg << " * " << result;
            //std::cout << " ";
            //std::cout << units[i] << std::endl;
        }

        if(i == 5 && result >= 365.25) {
            result /= 365.25;
            i++;
            //logdbg << " * " << result;
            //std::cout << " ";
            //std::cout << units[i] << std::endl;
        }

        assertStrictlyLessThan(i, numUnits);
        str << std::fixed << std::setprecision(static_cast<int>(precision));
        str << result;
        str << " ";
        str << units[i];

        return str.str();
    }

    /**
     * Returns k random numbers in the range [0, n) + offset as an std::set
     */
    template<class T>
    static void randomSubset(std::set<T>& s, int n, int k, T offset = 0) {
        std::vector<T> v;
        randomSubset(v, n, k, offset);

        s.clear();
        s.insert(v.begin(), v.end());
        assertEqual(s.size(), v.size());
    }

    /**
     * Returns k random numbers in the range [0, n) + offset as an std::vector
     */
    template<class T>
    static void randomSubset(std::vector<T>& v, int n, int k, T offset = 0) {
        // NOTE: Does not need cryptographically secure RNG
        v.resize(static_cast<size_t>(n));
        for (size_t i = 0; i < v.size(); i++) {
            v[i] = static_cast<T>(i) + offset;
        }

        std::shuffle(v.begin(), v.end(), std::mt19937{std::random_device{}()});

        v.resize(static_cast<size_t>(k));
    }

    static void hex2bin(const char * hexBuf, size_t hexBufLen, unsigned char * bin, size_t binCapacity);
    static void bin2hex(const void * data, size_t dataLen, char * hexBuf, size_t hexBufCapacity);

    static std::string bin2hex(const void * data, size_t dataLen);
    static void hex2bin(const std::string& hexStr, unsigned char * bin, size_t binCapacity);
};
