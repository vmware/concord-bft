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

thread_local uint64_t bft_seq = 0;
uint64_t getSeq() { return bft_seq++; }

// globally defined loggers
logging::Logger GL = logging::getLogger("concord.bft");
logging::Logger CNSUS = logging::getLogger("concord.bft.consensus");
logging::Logger THRESHSIGN_LOG = logging::getLogger("concord.bft.threshsign");
logging::Logger BLS_LOG = logging::getLogger("concord.bft.threshsign.bls");
logging::Logger EDDSA_MULTISIG_LOG = logging::getLogger("threshsign.eddsa");
logging::Logger KEY_EX_LOG = logging::getLogger("concord.bft.key-exchange");
logging::Logger CAT_BLOCK_LOG = logging::getLogger("concord.kvbc.categorized-blockchain");
logging::Logger V4_BLOCK_LOG = logging::getLogger("concord.kvbc.v4-blockchain");
logging::Logger VC_LOG = logging::getLogger("concord.bft.viewchange");
logging::Logger ST_DST_LOG = logging::getLogger("concord.bft.st.dst");
logging::Logger ST_SRC_LOG = logging::getLogger("concord.bft.st.src");
logging::Logger MSGS = logging::getLogger("concord.bft.msgs");
logging::Logger CL_MNGR = logging::getLogger("concord.bft.client-manager");
logging::Logger TS_MNGR = logging::getLogger("concord.bft.time-service-manager");
logging::Logger STATE_SNAPSHOT = logging::getLogger("concord.thin_replica.replica-state-snapshot");
logging::Logger ADPTV_PRUNING = logging::getLogger("concord.adaptive-pruning");
