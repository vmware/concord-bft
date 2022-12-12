find_package(Boost ${MIN_BOOST_VERSION})
find_package(GTest REQUIRED)
add_library(concord-crypto STATIC src/factory.cpp src/crypto.cpp)

target_include_directories(concord-crypto PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
                                                 $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/crypto>
                                                 ${Boost_INCLUDE_DIR}
                                          PRIVATE src
)
target_link_libraries(concord-crypto PUBLIC util)
if(USE_OPENSSL)
    find_package(OpenSSL REQUIRED)
    target_sources(concord-crypto PRIVATE src/openssl/crypto.cpp
                                          src/openssl/certificates.cpp
    )
    target_link_libraries(concord-crypto PUBLIC OpenSSL::Crypto z)
    target_compile_definitions(concord-crypto PUBLIC USE_OPENSSL)
endif()


if(BUILD_TESTING)
    add_subdirectory(test)
endif()
