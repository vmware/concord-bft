set(common_source_files
        XAssert.cpp
        Utils.cpp
        Timer.cpp
        IPublicParameters.cpp
        VectorOfShares.cpp
        ThresholdSignaturesTypes.cpp
        )

add_library(threshsign STATIC ${common_source_files})
target_include_directories(threshsign PUBLIC ../include)
target_link_libraries(threshsign PUBLIC Threads::Threads)

message(STATUS "Using OpenSSL EdDSA threshold signatures")
add_subdirectory(eddsa)
if(NOT BUILD_THIRDPARTY)
    set(OPENSSL_USE_STATIC_LIBS TRUE)
    find_package(OpenSSL REQUIRED)
    set(OPENSSL_USE_STATIC_LIBS FALSE)
endif()
target_link_libraries(threshsign PUBLIC OpenSSL::Crypto z)
target_sources(threshsign PUBLIC $<TARGET_OBJECTS:eddsa_openssl>)

if(BUILD_TESTING)
    add_subdirectory(app)
endif()
