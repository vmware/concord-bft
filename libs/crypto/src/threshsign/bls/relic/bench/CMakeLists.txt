add_subdirectory(lib)

set(bls_bench_sources
        BenchThresholdBls.cpp
        BenchRelic.cpp
        BenchLagrange.cpp
        BenchMultiExp.cpp
        )

foreach(appSrc ${bls_bench_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    add_executable(${appName} ${appSrc} $<TARGET_OBJECTS:bench>)
    target_include_directories(${appName} PRIVATE ${RELIC_INCLUDE_DIRS} ../../../)
    target_link_libraries(${appName} PRIVATE mainapp relic_mainapp threshsign)
endforeach()
