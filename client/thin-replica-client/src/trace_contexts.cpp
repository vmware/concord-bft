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

using concord::client::concordclient::EventGroup;
using concord::client::concordclient::EventVariant;
using concord::client::concordclient::Update;

using SpanPtr = std::unique_ptr<opentracing::Span>;

namespace client::concordclient {

const std::string kCorrelationIdTag = "cid";

struct SimpleTextMapReaderWriter : public opentracing::TextMapWriter, public opentracing::TextMapReader {
  SimpleTextMapReaderWriter(std::unordered_map<std::string, std::string>& text_map_) : text_map(text_map_) {}

  expected<void> Set(string_view key, string_view value) const override {
    text_map[key] = value;
    return {};
  }

  expected<void> ForeachKey(std::function<expected<void>(string_view key, string_view value)> f) const override {
    for (const auto& key_value : text_map) {
      auto result = f(key_value.first, key_value.second);
      if (!result) return result;
    }
    return {};
  }

  std::unordered_map<std::string, std::string>& text_map;
};

void TraceContexts::InjectSpan(const TraceContexts::SpanPtr& span, EventVariant& update) {
  if (not span) {
    return;
  }

  std::unordered_map<std::string, std::string> text_map;
  SimpleTextMapReaderWriter writer(text_map);
  span->tracer().Inject(span->context(), writer);

  if (std::holds_alternative<EventGroup>(update)) {
    std::get<EventGroup>(update).trace_context = writer.text_map;
  } else {
    W3cTraceContext serialized_context;
    serialized_context.mutable_key_values()->insert(text_map.begin(), text_map.end());
    std::get<Update>(update).span_context = serialized_context.SerializeAsString();
  }
}

expected<std::unique_ptr<opentracing::SpanContext>> TraceContexts::ExtractSpan(const EventVariant& update) {
  if (std::holds_alternative<EventGroup>(update)) {
    auto& eg = std::get<EventGroup>(update);
    if (!eg.trace_context.empty()) {
      std::unordered_map<std::string, std::string> text_map = eg.trace_context;
      SimpleTextMapReaderWriter reader(text_map);
      return opentracing::Tracer::Global()->Extract(reader);
    }
  } else if (std::holds_alternative<Update>(update)) {
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
                                                 const std::string& cid) {
  if (trace_context.empty()) {
    return nullptr;
  }
  std::istringstream context_stream(trace_context);
  auto parent_span_context = opentracing::Tracer::Global()->Extract(context_stream);
  if (not parent_span_context) {
    return nullptr;
  }
  return opentracing::Tracer::Global()->StartSpan(
      child_name, {opentracing::FollowsFrom(&**parent_span_context), opentracing::SetTag{kCorrelationIdTag, cid}});
}
}  // namespace client::concordclient
