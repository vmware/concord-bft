set(bench_source_files
  Benchmark.cpp
  IThresholdSchemeBenchmark.cpp
)

add_library(bench OBJECT ${bench_source_files})
target_include_directories(bench
   PRIVATE ../../include ../../src)
