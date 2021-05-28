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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef THIN_REPLICA_CLIENT_HEALTHSTATUS_HPP_
#define THIN_REPLICA_CLIENT_HEALTHSTATUS_HPP_

namespace client::thin_replica_client {

enum HealthStatus {
  Healthy,
  Unhealthy,
};

}  // namespace client::thin_replica_client

#endif  // THIN_REPLICA_CLIENT_HEALTHSTATUS_HPP_
