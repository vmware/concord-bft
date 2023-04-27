option(USE_MULTISIG_EDDSA "Enable use of OpenSSL library EdDSA implementation for multisignature generation and verification" ON)

add_library(threshsign STATIC ThresholdSignaturesTypes.cpp)

target_include_directories(threshsign PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(threshsign PUBLIC concord-crypto)

if(USE_MULTISIG_EDDSA)
    message(STATUS "Using OpenSSL EdDSA threshold signatures")
    target_compile_definitions(threshsign PUBLIC USE_MULTISIG_EDDSA)
    target_include_directories(threshsign PUBLIC ${Boost_INCLUDE_DIR})
    add_subdirectory(eddsa)
    target_link_libraries(threshsign PUBLIC eddsa_multisig_lib)
endif()

if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(threshsign PUBLIC /usr/local/ssl/include ${CONCORD-BFT_LOCAL_PATH}/libs/util/include/ /usr/local/include/  ${CONCORD-BFT_LOCAL_PATH}/libs/)
endif()

if(BUILD_TESTING)
    add_subdirectory(bench)
endif()
