#
# Configuration options
#
set(
  CURVE
  "BN128"
  CACHE
  STRING
  "Default curve: one of ALT_BN128, BN128, EDWARDS, MNT4, MNT6"
)

option(
  BINARY_OUTPUT
  "In serialization of elliptic curve points, output raw binary data (instead of decimal), which is smaller and faster."
  OFF
)

add_definitions(
  -DCURVE_${CURVE}
)

option(
  USE_MULTITHREADING
  "Enable parallelized execution of DKG protocols using OpenMP"
  OFF
)

if(${CURVE} STREQUAL "BN128")
  add_definitions(
    -DBN_SUPPORT_SNARK=1
  )
endif()

set(sigProcessor_test_sources
    sigProcessorTests.cpp
)

set(sig_processor_src src/sigProcessor.cpp)
add_library(sig_processor STATIC ${sig_processor_src})
target_link_libraries(sig_processor PUBLIC utt_api corebft util)
target_include_directories(sig_processor PUBLIC include)

add_subdirectory(tests)
