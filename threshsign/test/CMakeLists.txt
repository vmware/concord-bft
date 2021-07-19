set(bls_test_sources
    TestBlsBatchVerifier.cpp
    TestGroupElementSizes.cpp
    TestRelicSerialization.cpp
    TestLagrange.cpp
    TestRelic.cpp
    TestThresholdBls.cpp
    TestVectorOfShares.cpp
)

foreach(appSrc ${bls_test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    add_relic_executable(${appName} ${appSrc} ../bin/test)
    add_test(NAME ${appName} COMMAND ${appName})
endforeach()

set(test_sources
    TestMisc.cpp
)

foreach(appSrc ${test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    add_threshsign_executable(${appName} ${appSrc} ../bin/test)
    add_test(NAME ${appName} COMMAND ${appName})
endforeach()
