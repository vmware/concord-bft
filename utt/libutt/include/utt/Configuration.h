#pragma once

#include <stdexcept>

// NOTE: Instead of modifying this file, just build using `cmake -DCMAKE_BUILD_TYPE=Trace` which will define TRACE for you
//#define TRACE

// Some compilers have different #define's for C++11's noexcept

#ifdef _GLIBCXX_NOEXCEPT
# define _NOEXCEPT _GLIBCXX_USE_NOEXCEPT
#else
# ifndef _NOEXCEPT
#  error "_NOEXCEPT is not defined"
# endif
#endif
