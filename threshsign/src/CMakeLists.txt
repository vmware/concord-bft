set(common_source_files
    XAssert.cpp
    Utils.cpp
    Timer.cpp
    IPublicKey.cpp
    ISecretKey.cpp
    IPublicParameters.cpp
    IThresholdVerifier.cpp
    VectorOfShares.cpp
    ThresholdSignaturesTypes.cpp
    # TODO: create a separate target
    eddsa/EdDSAMultisigFactory.cpp
    eddsa/SSLEdDSAPublicKey.cpp
    eddsa/SSLEdDSAPrivateKey.cpp
    eddsa/EdDSAMultisigSigner.cpp
    eddsa/EdDSAMultisigVerifier.cpp)

add_library(common OBJECT ${common_source_files})

if(BUILD_THIRDPARTY)
    add_dependencies(common relic)
endif()

target_include_directories(common PRIVATE ../include ${RELIC_INCLUDE_DIRS})

add_subdirectory(bls)
if (BUILD_TESTING)
	add_subdirectory(app)
endif()
