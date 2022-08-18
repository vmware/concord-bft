// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

#include "bftclient/base_types.h"

namespace concord::osexample {

// This interface is used to generate messages to be sent to bft client.
class IMessageGenerator {
 public:
  virtual ~IMessageGenerator() = default;
  virtual bft::client::Msg createReadRequest() = 0;
  virtual bft::client::Msg createWriteRequest() = 0;
};

}  // end of namespace concord::osexample
