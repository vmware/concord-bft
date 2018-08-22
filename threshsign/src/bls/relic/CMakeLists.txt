set(bls_relic_source_files
  BlsBatchVerifier.cpp
  BlsAlmostMultisigAccumulator.cpp
  BlsAlmostMultisigCoefficients.cpp
  BlsMultisigAccumulator.cpp
  BlsMultisigKeygen.cpp
  BlsMultisigVerifier.cpp
  BlsNumTypes.cpp
  BlsPolynomial.cpp
  BlsPublicParameters.cpp
  BlsThresholdAccumulator.cpp
  BlsThresholdAccumulatorBaseExports.cpp
  BlsThresholdFactory.cpp
  BlsThresholdKeygen.cpp
  BlsThresholdSigner.cpp
  BlsThresholdVerifier.cpp
  LagrangeInterpolation.cpp
  Library.cpp 
)

add_library(bls_relic OBJECT ${bls_relic_source_files})

target_include_directories(bls_relic PRIVATE 
    ../../../include 
    ../../../lib 
    ../..
    .)
