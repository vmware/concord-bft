cmake_minimum_required(VERSION 3.24)
add_library(concord-crypto STATIC src/factory.cpp src/crypto.cpp)

target_include_directories(concord-crypto PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
                                                 $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/crypto>
                                                 ${Boost_INCLUDE_DIR}
                                          PRIVATE src
                                          )

if(USE_OPENSSL)
    find_package(Threads REQUIRED)
    set(ZLIB_USE_STATIC_LIBS "ON")
    find_package(ZLIB REQUIRED)
    if(NOT BUILD_THIRDPARTY)
        find_package(OpenSSL COMPONENTS Crypto REQUIRED)
    endif()
    target_sources(concord-crypto PRIVATE src/openssl/crypto.cpp
                                          src/openssl/certificates.cpp
                                          )
    if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
        find_library(OPEN_SSL_STATIC_LIBRARY NAMES "libcrypto.a" HINTS  ${OPENSSL_ROOT_DIR}/lib/)
        target_link_libraries(concord-crypto PUBLIC ${OPEN_SSL_STATIC_LIBRARY} z ${CMAKE_DL_LIBS} Threads::Threads)
    else()
        target_link_libraries(concord-crypto PUBLIC OpenSSL::Crypto ZLIB::ZLIB)
    endif()
    target_compile_definitions(concord-crypto PUBLIC USE_OPENSSL)
endif()

target_link_libraries(concord-crypto PUBLIC util)

if(BUILD_TESTING)
    include(FetchContent)
    FetchContent_Declare(picobench
                        GIT_REPOSITORY "https://github.com/iboB/picobench.git"
                        GIT_TAG "v2.01"
                        GIT_SHALLOW TRUE
                        GIT_PROGRESS TRUE
                        OVERRIDE_FIND_PACKAGE
                        )

    FetchContent_MakeAvailable(picobench)

    add_subdirectory(test)
endif()

add_subdirectory(src/threshsign)
