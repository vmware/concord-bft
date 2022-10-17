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

#ifndef THIN_REPLICA_CLIENT_TRACE_CONTEXTS_HPP_
#define THIN_REPLICA_CLIENT_TRACE_CONTEXTS_HPP_

#include <opentracing/span.h>
#include <grpcpp/server.h>
#include <optional>

#include "thin_replica.pb.h"
#include "client/concordclient/event_update.hpp"
#include "Logger.hpp"

using opentracing::expected;

namespace cc = concord::client::concordclient;

namespace client::thin_replica_client {

class TraceContexts {
 public:
  using SpanPtr = std::unique_ptr<opentracing::Span>;
  using SpanCtxPtr = std::unique_ptr<opentracing::SpanContext>;

  static void InjectSpan(const SpanPtr& span, cc::EventVariant& update);
  static expected<std::unique_ptr<opentracing::SpanContext>> ExtractSpan(const cc::EventVariant& update);
  static SpanPtr CreateChildSpanFromBinary(const std::string& trace_context,
                                           const std::string& child_name,
                                           const std::string& correlation_id = {});
  static SpanCtxPtr ExtractSpanFromMetadata(const opentracing::Tracer& tracer, const grpc::ServerContext& context);
};

}  // namespace client::thin_replica_client

#endif  // THIN_REPLICA_CLIENT_TRACE_CONTEXTS_HPP_
