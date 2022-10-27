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
find_library(CRYPTO_STATIC_LIBRARY NAMES libcrypto.a PATHS ${OPENSSL_ROOT_DIR}/lib/ REQUIRED)

message(STATUS "Using OpenSSL EdDSA threshold signatures")
add_subdirectory(eddsa)
if(NOT BUILD_THIRDPARTY)
    find_package(OpenSSL REQUIRED)
endif()

message(STATUS "Using ${CRYPTO_STATIC_LIBRARY} for threshsign library")
target_link_libraries(threshsign PUBLIC ${CRYPTO_STATIC_LIBRARY} dl z pthread)
target_sources(threshsign PUBLIC $<TARGET_OBJECTS:eddsa_openssl>)

if(BUILD_TESTING)
    add_subdirectory(app)
endif()
