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

#include "client/thin-replica-client/trace_contexts.hpp"

#include <log4cplus/loggingmacros.h>
#include <opentracing/propagation.h>
#include <opentracing/span.h>
#include <opentracing/tracer.h>
#include <sstream>
#include <string>
#include <unordered_map>

#include "thin_replica.pb.h"

using com::vmware::concord::thin_replica::W3cTraceContext;
using opentracing::expected;
using opentracing::string_view;
using opentracing::TextMapReader;
using opentracing::TextMapWriter;

using SpanPtr = std::unique_ptr<opentracing::Span>;

namespace client::thin_replica_client {

const std::string kCorrelationIdTag = "cid";

class SimpleTextMapReaderWriter : public opentracing::TextMapWriter, public opentracing::TextMapReader {
 public:
  SimpleTextMapReaderWriter(std::unordered_map<std::string, std::string>& text_map) : text_map_(text_map) {}

  expected<void> Set(string_view key, string_view value) const override {
    text_map_[key] = value;
    return {};
  }

  expected<void> ForeachKey(std::function<expected<void>(string_view key, string_view value)> f) const override {
    for (const auto& key_value : text_map_) {
      auto result = f(key_value.first, key_value.second);
      if (!result) return result;
    }
    return {};
  }

 private:
  std::unordered_map<std::string, std::string>& text_map_;
};

void TraceContexts::InjectSpan(const TraceContexts::SpanPtr& span, EventVariant& update) {
  if (span) {
    // TODO: Event Group
    if (std::holds_alternative<Update>(update)) {
      std::unordered_map<std::string, std::string> text_map;
      SimpleTextMapReaderWriter writer(text_map);
      span->tracer().Inject(span->context(), writer);
      W3cTraceContext serialized_context;
      serialized_context.mutable_key_values()->insert(text_map.begin(), text_map.end());
      std::get<Update>(update).span_context = serialized_context.SerializeAsString();
    }
  }
}

expected<std::unique_ptr<opentracing::SpanContext>> TraceContexts::ExtractSpan(const EventVariant& update) {
  // TODO: Event Group
  if (std::holds_alternative<Update>(update)) {
    auto& legacy_event = std::get<Update>(update);
    if (!legacy_event.span_context.empty()) {
      W3cTraceContext trace_context;
      trace_context.ParseFromString(legacy_event.span_context);
      std::unordered_map<std::string, std::string> text_map(trace_context.key_values().begin(),
                                                            trace_context.key_values().end());
      SimpleTextMapReaderWriter reader(text_map);
      return opentracing::Tracer::Global()->Extract(reader);
    }
  }
  return std::unique_ptr<opentracing::SpanContext>();
}

SpanPtr TraceContexts::CreateChildSpanFromBinary(const std::string& trace_context,
                                                 const std::string& child_name,
                                                 const std::string& correlation_id,
                                                 const log4cplus::Logger& logger) {
  if (trace_context.empty()) {
    LOG4CPLUS_DEBUG(logger, "Span for correlation ID: '" << correlation_id << "' is empty");
    return nullptr;
  } else {
    std::istringstream context_stream(trace_context);
    auto parent_span_context = opentracing::Tracer::Global()->Extract(context_stream);
    if (parent_span_context) {
      return opentracing::Tracer::Global()->StartSpan(
          child_name,
          {opentracing::FollowsFrom(&**parent_span_context), opentracing::SetTag{kCorrelationIdTag, correlation_id}});
    } else {
      LOG4CPLUS_DEBUG(logger,
                      "Failed to extract span for correlation ID: '" << correlation_id
                                                                     << "', error:" << parent_span_context.error());
      return nullptr;
    }
  }
}
}  // namespace client::thin_replica_client
