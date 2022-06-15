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

if(USE_RELIC AND USE_EDDSA_OPENSSL)
    find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)
    add_executable(threshsign_bench BenchmarkThreshsign.cpp)
    target_link_libraries(threshsign_bench threshsign Boost::program_options)
    target_include_directories(threshsign_bench PUBLIC .. ../../thirdparty/include)
endif()