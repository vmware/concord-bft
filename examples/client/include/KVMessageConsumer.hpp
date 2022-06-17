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

#include "Logger.hpp"
#include "IMessageConsumer.hpp"

namespace concord::osexample {

// This class is used to consume and deserialize the response,
// which came from bft client after sending client request message.
class KVMessageConsumer : public concord::osexample::IMessageConsumer {
 public:
  void consumeMessage() override final;
  void deserialize(const bft::client::Reply& rep, bool isReadOnly = false) override final;

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("osexample::KVMessageConsumer"));
    return logger_;
  }
};

}  // end of namespace concord::osexample
