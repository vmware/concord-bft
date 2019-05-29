add_subdirectory(lib)

set(bls_bench_sources
  BenchThresholdBls.cpp
  BenchRelic.cpp
  BenchLagrange.cpp
  BenchMultiExp.cpp
)

foreach(appSrc ${bls_bench_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    add_relic_executable(${appName} ${appSrc} ../bin/bench)
    target_link_libraries(${appName} PUBLIC util)
endforeach()
