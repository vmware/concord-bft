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

#ifndef _WIN32
extern "C" {
#include <relic/relic.h>
#include <relic/relic_ep.h>
#include <relic/relic_epx.h>
#include <relic/relic_fpx.h>
}
#else
// TODO: ALIN: Can't we simply fix this by installing relic in its own directory on Windows?
// Since it's a cmake build it should be easy to set an install prefix.
extern "C" {
#include <relic.h>
#include <relic_ep.h>
#include <relic_epx.h>
#include <relic_fpx.h>
}
#endif
