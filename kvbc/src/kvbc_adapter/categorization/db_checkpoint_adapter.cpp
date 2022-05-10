// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "assertUtils.hpp"
#include "kvbc_adapter/categorization/db_checkpoint_adapter.hpp"

namespace concord::kvbc::adapter::categorization {

DbCheckpointImpl::DbCheckpointImpl(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain>& kvbc)
    : kvbc_(kvbc.get()) {
  ConcordAssertNE(kvbc_, nullptr);
}

}  // namespace concord::kvbc::adapter::categorization
