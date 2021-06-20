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

// NOTE: Instead of modifying this file, just build using `cmake -DCMAKE_BUILD_TYPE=Trace` which will define TRACE for
// you
//#define TRACE

// Some compilers have different #define's for C++11's noexcept

#ifdef _GLIBCXX_NOEXCEPT
#define _NOEXCEPT _GLIBCXX_USE_NOEXCEPT
#else
#ifndef _NOEXCEPT
#error "_NOEXCEPT is not defined"
#endif
#endif
