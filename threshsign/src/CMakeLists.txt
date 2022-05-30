set(common_source_files
        XAssert.cpp
        Utils.cpp
        Timer.cpp
        IPublicKey.cpp
        ISecretKey.cpp
        IPublicParameters.cpp
        IThresholdVerifier.cpp
        VectorOfShares.cpp
        ThresholdSignaturesTypes.cpp
        )

add_library(threshsign STATIC ${common_source_files})
target_include_directories(threshsign PUBLIC ../include)
target_link_libraries(threshsign PUBLIC Threads::Threads)

if(USE_RELIC)
    add_subdirectory(bls)
    target_compile_definitions(threshsign PUBLIC USE_RELIC)
    link_with_relic_library(threshsign)
    target_sources(threshsign PUBLIC $<TARGET_OBJECTS:bls_relic>)
endif()

if(USE_EDDSA_OPENSSL)
    add_subdirectory(eddsa)
    target_compile_definitions(threshsign PUBLIC USE_EDDSA_OPENSSL)
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL REQUIRED)
    endif()
    target_link_libraries(threshsign PUBLIC OpenSSL::Crypto)
    target_sources(threshsign PUBLIC $<TARGET_OBJECTS:eddsa_openssl>)
endif()

if(BUILD_TESTING)
    add_subdirectory(app)
endif()
