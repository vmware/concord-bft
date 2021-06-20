// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license,
// as noted in the LICENSE file.

#include "OpenTracing.hpp"
#include <string>

#ifdef USE_OPENTRACING
#include <opentracing/tracer.h>
#include <sstream>
#endif

namespace concordUtils {
SpanContext SpanWrapper::context() const {
#ifdef USE_OPENTRACING
  if (!impl()) {
    // Span is not initialized
    return SpanContext{};
  }
  std::ostringstream context;
  impl()->tracer().Inject(impl()->context(), context);
  return SpanContext{context.str()};
#else
  return SpanContext{};
#endif
}

SpanWrapper startSpan(const std::string& operation_name) {
#ifdef USE_OPENTRACING
  return SpanWrapper{opentracing::Tracer::Global()->StartSpan(operation_name)};
#else
  (void)operation_name;
  return SpanWrapper{};
#endif
}

SpanWrapper startChildSpan(const std::string& child_operation_name, const SpanWrapper& parent_span) {
#ifdef USE_OPENTRACING
  if (!parent_span) {
    return SpanWrapper{};
  }
  auto tracer = opentracing::Tracer::Global();
  auto span = tracer->StartSpan(child_operation_name, {opentracing::ChildOf(&parent_span.impl()->context())});
  return SpanWrapper{std::move(span)};
#else
  (void)child_operation_name;
  (void)parent_span;
  return SpanWrapper{};
#endif
}

SpanWrapper startChildSpanFromContext(const SpanContext& context, const std::string& child_operation_name) {
#ifdef USE_OPENTRACING
  if (context.data().empty()) {
    return SpanWrapper{};
  }
  std::istringstream context_stream{context.data()};
  auto tracer = opentracing::Tracer::Global();
  auto parent_span_context = tracer->Extract(context_stream);
  // DD: It might happen in 2 cases:
  // 1. invalid context
  // 2. Tracer is not initialized
  if (!parent_span_context) {
    return SpanWrapper{};
  }
  auto span = tracer->StartSpan(child_operation_name, {opentracing::ChildOf(parent_span_context->get())});
  return SpanWrapper{std::move(span)};
#else
  (void)context;
  (void)child_operation_name;
  return SpanWrapper{};
#endif
}

#ifdef USE_OPENTRACING
SpanWrapper startChildSpanFromContext(const opentracing::SpanContext& context,
                                      const std::string& child_operation_name) {
  auto tracer = opentracing::Tracer::Global();
  auto span = tracer->StartSpan(child_operation_name, {opentracing::ChildOf(&context)});
  return {std::move(span)};
}
#endif
}  // namespace concordUtils
