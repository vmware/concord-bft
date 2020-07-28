// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license,
// as noted in the LICENSE file.

#include "Logger.hpp"
#include <fstream>
#include <iostream>

namespace logging {

ScopedMdc::ScopedMdc(const std::string& key, const std::string& val) : key_{key} { MDC_PUT(key, val); }
ScopedMdc::~ScopedMdc() { MDC_REMOVE(key_); }

}  // namespace logging

// globally defined loggers
logging::Logger GL = logging::getLogger("concord");
logging::Logger CNSUS = logging::getLogger("concord.bft.consensus");
logging::Logger THRESHSIGN_LOG = logging::getLogger("concord.threshsign");
logging::Logger BLS_LOG = logging::getLogger("concord.threshsign.bls");
logging::Logger VC_LOG = logging::getLogger("concord.viewchange");
