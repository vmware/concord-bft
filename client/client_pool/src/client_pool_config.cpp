// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "include/client/client_pool/client_pool_config.hpp"

namespace concord::config_pool {
using bft::communication::CommFactory;
using bft::communication::ICommunication;
using bft::communication::PlainUdpConfig;
using bft::communication::TlsTcpConfig;

ClientPoolConfig::ClientPoolConfig() { logger_ = logging::getLogger("com.vmware.external_client_pool"); }

}  // namespace concord::config_pool
