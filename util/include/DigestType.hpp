// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

//#define MD5_DIGEST
#define SHA256_DIGEST
//#define SHA512_DIGEST

#ifdef DIGEST_SIZE
#error Inconsistent Digest definition
#endif

#if defined MD5_DIGEST
#ifdef DIGEST_SIZE
#error Inconsistent Digest definition
#endif
#define DIGEST_SIZE (16)
#endif

#if defined SHA256_DIGEST
#ifdef DIGEST_SIZE
#error Inconsistent Digest definition
#endif
#define DIGEST_SIZE (32)
#endif

#if defined SHA512_DIGEST
#ifdef DIGEST_SIZE
#error Inconsistent Digest definition
#endif
#define DIGEST_SIZE (64)
#endif

#ifndef DIGEST_SIZE
#error no Digest type
#endif
