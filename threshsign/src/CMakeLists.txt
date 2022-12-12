
set(common_source_files
        XAssert.cpp
        Utils.cpp
        Timer.cpp
        IPublicParameters.cpp
        VectorOfShares.cpp
        ThresholdSignaturesTypes.cpp
        )

add_library(threshsign STATIC ${common_source_files})
target_include_directories(threshsign PUBLIC ../include ${Boost_INCLUDE_DIR}
)
if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(threshsign PUBLIC /usr/local/ssl/include ${CMAKE_CURRENT_SOURCE_DIR}/../../util/include/ /usr/local/include/)
endif()
target_link_libraries(threshsign PUBLIC Threads::Threads concord-crypto)

message(STATUS "Using OpenSSL EdDSA threshold signatures")
add_subdirectory(eddsa)
    set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)
    set(OPENSSL_USE_STATIC_LIBS FALSE)
target_link_libraries(threshsign PUBLIC OpenSSL::Crypto z)
target_sources(threshsign PUBLIC $<TARGET_OBJECTS:eddsa_openssl>)
if(BUILD_TESTING)
    add_subdirectory(app)
endif()
