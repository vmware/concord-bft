// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>
#include "secret_data.h"

namespace concord::secretsretriever {

// This function is inteded to be used only once, at startup. Then the result
// should be stored for use throughout the application
concord::secretsmanager::SecretData SecretRetrieve(const std::string &secret_url);

}  // namespace concord::secretsretriever