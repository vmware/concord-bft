// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <string>

namespace concord::kvbc::keyTypes {
static const char bft_seq_num_key = 0x21;
static const char reconfiguration_pruning_key = 0x24;
static const char reconfiguration_download_key = 0x26;
static const char reconfiguration_install_key = 0x27;
static const char reconfiguration_key_exchange = 0x28;
static const char reconfiguration_add_remove = 0x29;
static const char reconfiguration_wedge_key = 0x2a;
static const char reconfiguration_client_data_prefix = 0x2c;
static const char reconfiguration_epoch_key = 0x2d;
static const char reconfiguration_tls_exchange_key = 0x2e;

static const char reconfiguration_restart_key = 0x30;
static const char reconfiguration_ts_key = 0x31;
static const char reconfiguration_rep_main_key = 0x32;
static const std::string genesis_block_key(1, 0x32);
static const std::string state_public_key_set(1, 0x33);

static const std::string blockchain_version(1, 0x50);
static const std::string v4_snapshot_sequence(1, 0x51);
static const std::string v4_snapshot_sequence_checkpoint(1, 0x52);

enum PRUNING_COMMAND_TYPES : uint16_t {
  PRUNING_START = 0x0,
  BACKWARD_COMP = 0x1,
  TICKS_CHANGE_REQUEST = 0x2,
  SWITCH_MODE_REQUEST = 0x3,
  STOP_REQUEST = 0x4,
  PRUNING_END
};
enum CLIENT_COMMAND_TYPES : uint8_t {
  start_ = 0x0,
  PUBLIC_KEY_EXCHANGE = 0x1,             // identifier of public key exchange request by client
  CLIENT_KEY_EXCHANGE_COMMAND = 0x2,     // identifier of client key exchange request by operator
  CLIENT_SCALING_COMMAND = 0x3,          // identifier of client scaling request by operator
  CLIENT_SCALING_COMMAND_STATUS = 0X4,   // identifier of client update request after successful scaling
  CLIENT_SCALING_EXECUTE_COMMAND = 0x5,  // identifier of client scaling execute request by operator
  CLIENT_RESTART_COMMAND = 0x7,          // identifier of client restart command by operator
  CLIENT_RESTART_STATUS = 0x8,           // identifier of client restart status
  end_
};
}  // namespace concord::kvbc::keyTypes
