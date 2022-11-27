find_package(Boost ${MIN_BOOST_VERSION})

set(eddsa_openssl_source_files
        EdDSAMultisigFactory.cpp
        EdDSAMultisigSigner.cpp
        EdDSAMultisigVerifier.cpp)

add_library(eddsa_openssl STATIC ${eddsa_openssl_source_files})
target_include_directories(eddsa_openssl PRIVATE ../../include ${Boost_INCLUDE_DIR})
if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(eddsa_openssl PRIVATE /usr/local/ssl/include /usr/local/include/ ${CMAKE_CURRENT_SOURCE_DIR}/../../../util/include/)
endif()
