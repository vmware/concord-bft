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

#include <grpcpp/grpcpp.h>
#include <concord_client.grpc.pb.h>

namespace concord::client::clientservice {

class RequestServiceImpl final : public vmware::concord::client::v1::RequestService::Service {
 public:
  grpc::Status Send(grpc::ServerContext* context,
                    const vmware::concord::client::v1::Request* request,
                    vmware::concord::client::v1::Response* response) override;
};

}  // namespace concord::client::clientservice
