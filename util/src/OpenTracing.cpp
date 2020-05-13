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

#ifdef USE_OPENTRACING
#include <opentracing/tracer.h>
#include <sstream>
#endif

namespace concordUtils {
void SpanWrapper::setTag(const std::string& name, const std::string& value) {
#ifdef USE_OPENTRACING
  span_ptr_->SetTag(name, value);
#else
  (void)name;
  (void)value;
#endif
}

void SpanWrapper::finish() {
#ifdef USE_OPENTRACING
  span_ptr_->Finish();
#endif
}

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
  auto tracer = opentracing::Tracer::Global();
  if (!tracer) {
    // Tracer is not initialized by the parent
    return SpanWrapper{};
  } else {
    return SpanWrapper{tracer->StartSpan(operation_name)};
  }
#else
  (void)operation_name;
  return SpanWrapper{};
#endif
}

SpanWrapper startChildSpan(const std::string& child_operation_name, const SpanWrapper& parent_span) {
#ifdef USE_OPENTRACING
  auto tracer = opentracing::Tracer::Global();
  if (!tracer) {
    // Tracer is not initialized by the parent
    return SpanWrapper{};
  }
  if (!parent_span.impl()) {
    return SpanWrapper{};
  }
  return SpanWrapper{tracer->StartSpan(child_operation_name, {opentracing::ChildOf(&parent_span.impl()->context())})};
#else
  (void)child_operation_name;
  (void)parent_span;
  return SpanWrapper{};
#endif
}

SpanWrapper startChildSpanFromContext(const SpanContext& context, const std::string& child_operation_name) {
#ifdef USE_OPENTRACING
  auto tracer = opentracing::Tracer::Global();
  if (!tracer) {
    // Tracer is not initialized by the parent
    return SpanWrapper{};
  }
  if (context.empty()) {
    return SpanWrapper{};
  }
  std::istringstream context_stream{context};
  auto parent_span_context = tracer->Extract(context_stream);
  if (!parent_span_context) {
    std::ostringstream stream;
    stream << "Failed to extract span, error:" << parent_span_context.error();
    throw std::runtime_error(stream.str());
  }
  return SpanWrapper{tracer->StartSpan(child_operation_name, {opentracing::ChildOf(parent_span_context->get())})};
#else
  (void)context;
  (void)child_operation_name;
  return SpanWrapper{};
#endif
}
}  // namespace concordUtils
