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

namespace concord::kvbc::keyTypes {
static const char bft_seq_num_key = 0x21;
static const char reconfiguration_pruning_key = 0x24;
static const char reconfiguration_wedge_key = 0x25;
static const char reconfiguration_download_key = 0x26;
static const char reconfiguration_install_key = 0x27;
static const char reconfiguration_key_exchange = 0x28;
static const char reconfiguration_add_remove = 0x29;
static const char reconfiguration_client_key_exchange = 0x30;

}  // namespace concord::kvbc::keyTypes
