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
    add_executable(${appName} ${appSrc})
    target_link_libraries(${appName}  mainapp relic_mainapp threshsign)
    target_include_directories(${appName} PRIVATE ../ ../../../)
    add_test(NAME ${appName} COMMAND ${appName})
endforeach()

