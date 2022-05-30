
if(USE_RELIC)
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
endif()

set(test_sources
        TestMisc.cpp
        )

foreach(appSrc ${test_sources})
    get_filename_component(appName ${appSrc} NAME_WE)
    add_threshsign_executable(${appName} ${appSrc} ../bin/test)
    add_test(NAME ${appName} COMMAND ${appName})
endforeach()

find_package(GTest REQUIRED)

if(USE_EDDSA_OPENSSL)
    add_executable(eddsa_multisig_test TestEdDSAMultisig.cpp)
    add_test(eddsa_multisig_test eddsa_multisig_test)
    target_link_libraries(eddsa_multisig_test GTest::Main util threshsign)
endif()