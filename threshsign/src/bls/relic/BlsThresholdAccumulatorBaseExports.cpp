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

#ifdef ERROR  // TODO(GG): should be fixed by encapsulating relic (or windows) definitions in cpp files
#undef ERROR
#endif

/**
 * Template instantiations/exports.
 * Yes, including a ".[c/t]pp" file is weird, but C++ is weird:
 * https://isocpp.org/wiki/faq/templates#templates-defn-vs-decl
 */
#include "threshsign/Configuration.h"

#include "ThresholdAccumulatorBase.cpp"
#include "LagrangeInterpolation.h"

#include "threshsign/bls/relic/BlsThresholdScheme.h"

template class ThresholdAccumulatorBase<BLS::Relic::BlsPublicKey, BLS::Relic::G1T, BLS::Relic::BlsSigshareParser>;
