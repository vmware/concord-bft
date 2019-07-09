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
)

add_library(common OBJECT ${common_source_files})

target_include_directories(common PRIVATE ../include .)

add_subdirectory(bls)
add_subdirectory(app)
