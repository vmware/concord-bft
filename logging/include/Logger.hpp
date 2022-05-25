// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to
// the terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#define MDC_THREAD_KEY "thread"
#define MDC_REPLICA_ID_KEY "rid"
#define MDC_CID_KEY "cid"
#define MDC_SEQ_NUM_KEY "sn"
#define MDC_PRIMARY_KEY "pri"
#define MDC_PATH_KEY "path"

#include <cstdint>

uint64_t getSeq();

#ifndef USE_LOG4CPP
#include "Logging.hpp"
#else
#include "Logging4cplus.hpp"
#endif

extern logging::Logger GL;
extern logging::Logger CNSUS;
extern logging::Logger THRESHSIGN_LOG;
extern logging::Logger BLS_LOG;
extern logging::Logger EDDSA_MULTISIG_LOG;
extern logging::Logger KEY_EX_LOG;
extern logging::Logger CAT_BLOCK_LOG;
extern logging::Logger V4_BLOCK_LOG;
extern logging::Logger VC_LOG;
extern logging::Logger ST_SRC_LOG;
extern logging::Logger ST_DST_LOG;
extern logging::Logger MSGS;
extern logging::Logger CL_MNGR;
extern logging::Logger TS_MNGR;
extern logging::Logger STATE_SNAPSHOT;
extern logging::Logger ADPTV_PRUNING;

namespace logging {

Logger getLogger(const std::string& name);
void initLogger(const std::string& configFileName);

class ScopedMdc {
 public:
  ScopedMdc(const std::string& key, const std::string& val);
  ~ScopedMdc();

 private:
  const std::string key_;
};

}  // namespace logging

/*
 * These macros are meant to append temporary key-value pairs to the log messages.
 * The duration of this adding is the scope where SCOPED_MDC was called - when reaching to the end of this scope,
 * the key-value will be automatically removed.
 */
#define SCOPED_MDC(k, v) logging::ScopedMdc __s_mdc__(k, v)
#define SCOPED_MDC_CID(v) logging::ScopedMdc __s_mdc_cid__(MDC_CID_KEY, v)
#define SCOPED_MDC_SEQ_NUM(v) logging::ScopedMdc __s_mdc_seq_num__(MDC_SEQ_NUM_KEY, v)
#define SCOPED_MDC_PRIMARY(v) logging::ScopedMdc __s_mdc_primary__(MDC_PRIMARY_KEY, v)
#define SCOPED_MDC_PATH(v) logging::ScopedMdc __s_mdc_path__(MDC_PATH_KEY, v)
