#include "jaegertracing/Tracer.h"
#include <string>

int main() {
  std::string jaeger_agent = jaegertracing::reporters::Config::kDefaultLocalAgentHostPort;
  jaegertracing::samplers::Config sampler_config(jaegertracing::kSamplerTypeConst, 1.0);
  jaegertracing::reporters::Config reporter_config(jaegertracing::reporters::Config::kDefaultQueueSize,
                                                   jaegertracing::reporters::Config::defaultBufferFlushInterval(),
                                                   false /* do not log spans */,
                                                   jaeger_agent);
  jaegertracing::Config jaeger_config(false /* not disabled */, sampler_config, reporter_config);
  auto tracer = jaegertracing::Tracer::make(
      "concord", jaeger_config /*, std::unique_ptr<jaegertracing::logging::Logger>(new JaegerLogger())*/);
  opentracing::Tracer::InitGlobal(std::static_pointer_cast<opentracing::Tracer>(tracer));
}
