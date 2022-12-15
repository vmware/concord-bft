add_library(concord-crypto STATIC src/factory.cpp src/crypto.cpp)

target_include_directories(concord-crypto PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
                                                 $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/crypto>
                                                 ${Boost_INCLUDE_DIR}
                                          PRIVATE src
                                          )

if(USE_OPENSSL)
    find_package(OpenSSL REQUIRED)
    target_sources(concord-crypto PRIVATE src/openssl/crypto.cpp
                                          src/openssl/certificates.cpp
                                          )
    target_link_libraries(concord-crypto PUBLIC OpenSSL::Crypto z)
    target_compile_definitions(concord-crypto PUBLIC USE_OPENSSL)
endif()

target_link_libraries(concord-crypto PUBLIC util)

if(BUILD_TESTING)
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