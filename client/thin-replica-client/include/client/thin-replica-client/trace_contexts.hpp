// Copyright (c) 2021 VMware, Inc. All rights reserved. VMware Confidential

#ifndef THIN_REPLICA_CLIENT_TRACE_CONTEXTS_HPP_
#define THIN_REPLICA_CLIENT_TRACE_CONTEXTS_HPP_

#include <log4cplus/logger.h>
#include <opentracing/span.h>

#include "thin_replica.pb.h"
#include "update.hpp"

using opentracing::expected;

namespace client::thin_replica_client {

class TraceContexts {
 public:
  using SpanPtr = std::unique_ptr<opentracing::Span>;

  static void InjectSpan(const SpanPtr& span, Update& update);
  static expected<std::unique_ptr<opentracing::SpanContext>> ExtractSpan(const Update& update);
  static SpanPtr CreateChildSpanFromBinary(const std::string& trace_context,
                                           const std::string& child_name,
                                           const std::string& correlation_id,
                                           const log4cplus::Logger& logger);
};

}  // namespace client::thin_replica_client

#endif  // THIN_REPLICA_CLIENT_TRACE_CONTEXTS_HPP_
