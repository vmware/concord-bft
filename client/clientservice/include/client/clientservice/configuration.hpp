// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <yaml-cpp/yaml.h>

#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

// Fill the given ConcordClientConfig with default values
void setDefaultConfiguration(concord::client::concordclient::ConcordClientConfig&);

void parseConfigFile(concord::client::concordclient::ConcordClientConfig&, const YAML::Node&);

// Configure connection between ThinReplicaClient and ThinReplicaServer on the replicas
void configureSubscription(concord::client::concordclient::ConcordClientConfig&,
                           const std::string& tr_id,
                           bool insecure,
                           const std::string& tls_path);

}  // namespace concord::client::clientservice
