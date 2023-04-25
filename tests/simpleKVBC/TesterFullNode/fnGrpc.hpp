// Concord
//
// Copyright (c) 2019-2023 VMware, Inc. All Rights Reserved.
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

#include <grpcpp/grpcpp.h>
#include <request.grpc.pb.h>
#include "internalCommandsHandler.hpp"
#include "bftclient/bft_client.h"

class RequestServiceImpl final : public vmware::concord::client::request::v1::RequestService::Service {
 public:
  RequestServiceImpl(const std::shared_ptr<bft::client::Client> client,
                     const std::shared_ptr<InternalCommandsHandler> cmdHandler,
                     const bool fn_execute_enable)
      : logger_(logging::getLogger("concord.client.clientservice.event")),
        bft_client_(client),
        cmdHandler_(cmdHandler),
        fn_execute_enable_(fn_execute_enable){};
  grpc::Status Send(grpc::ServerContext* context,
                    const vmware::concord::client::request::v1::Request* request,
                    vmware::concord::client::request::v1::Response* response) override;

 private:
  void sendToInternalHandler(const bft::client::RequestConfig req_config,
                             const bft::client::Msg&& msg,
                             const bftEngine::MsgFlag flags,
                             vmware::concord::client::request::v1::Response* response);

  logging::Logger logger_;
  std::shared_ptr<bft::client::Client> bft_client_;
  std::shared_ptr<InternalCommandsHandler> cmdHandler_;
  bool fn_execute_enable_;
  std::string reply_buffer_;
};