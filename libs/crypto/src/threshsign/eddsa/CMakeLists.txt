find_package(Boost ${MIN_BOOST_VERSION})

add_library(eddsa_multisig_lib STATIC EdDSAMultisigFactory.cpp
                                      EdDSAMultisigSigner.cpp
                                      EdDSAMultisigVerifier.cpp
            )
target_include_directories(eddsa_multisig_lib PRIVATE ../../include ${Boost_INCLUDE_DIR})

if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(eddsa_multisig_lib PRIVATE /usr/local/ssl/include /usr/local/include/ ${CMAKE_CURRENT_SOURCE_DIR}/../../../util/include/)
endif()

if(BUILD_TESTING)
    add_subdirectory(test)
endif()
