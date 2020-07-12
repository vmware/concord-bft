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

#ifndef OPENTRACING_UTILS_HPP
#define OPENTRACING_UTILS_HPP

#ifdef USE_OPENTRACING
#include <memory>
#include <opentracing/span.h>
#include <type_traits>
#endif
#include <string>

namespace concordUtils {

using SpanContext = std::string;

class SpanWrapper {
 public:
  SpanWrapper() {}
  SpanWrapper(const SpanWrapper&) = delete;
  SpanWrapper& operator=(const SpanWrapper&) = delete;
  SpanWrapper(SpanWrapper&&) = default;
  SpanWrapper& operator=(SpanWrapper&&) = default;
  ~SpanWrapper() = default;
  explicit operator bool() const {
#ifdef USE_OPENTRACING
    return (impl().get() != nullptr);
#else
    return false;
#endif
  }

  template <typename ValueT>
  void setTag(const std::string& name, const ValueT& value) {
#ifdef USE_OPENTRACING
    if (!span_ptr_) {
      return;
    }
    span_ptr_->SetTag(name, value);
#else
    (void)name;
    (void)value;
#endif
  }

  void finish();
  SpanContext context() const;

  friend SpanWrapper startSpan(const std::string& operation_name);
  friend SpanWrapper startChildSpan(const std::string& operation_name, const SpanWrapper& parent_span);
  friend SpanWrapper startChildSpanFromContext(const SpanContext& context, const std::string& child_operation_name);

#ifdef USE_OPENTRACING
  using SpanPtr = std::unique_ptr<opentracing::Span>;
  const SpanPtr& impl() const { return span_ptr_; }

 private:
  SpanWrapper(SpanPtr&& span) : span_ptr_(std::move(span)) {}

  SpanPtr span_ptr_;
#endif
};

SpanWrapper startSpan(const std::string& operation_name);
SpanWrapper startChildSpan(const std::string& child_operation_name, const SpanWrapper& parent_span);
SpanWrapper startChildSpanFromContext(const SpanContext& context, const std::string& child_operation_name);
}  // namespace concordUtils
#endif /* end of include guard: OPENTRACING_UTILS_HPP */
