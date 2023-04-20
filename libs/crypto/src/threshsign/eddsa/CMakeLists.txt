find_package(Boost ${MIN_BOOST_VERSION})

add_library(eddsa_multisig_lib STATIC EdDSAMultisigFactory.cpp
                                      EdDSAMultisigSigner.cpp
                                      EdDSAMultisigVerifier.cpp
            )
target_include_directories(eddsa_multisig_lib PRIVATE ../../include ${Boost_INCLUDE_DIR})
target_link_libraries(eddsa_multisig_lib PUBLIC concord-crypto)
if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(eddsa_multisig_lib PRIVATE /usr/local/ssl/include /usr/local/include/)
endif()

if(BUILD_TESTING)
    add_subdirectory(test)
endif()
