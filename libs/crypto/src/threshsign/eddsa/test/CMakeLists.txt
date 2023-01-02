find_package(GTest REQUIRED)

add_executable(eddsa_multisig_test TestEdDSAMultisig.cpp)
add_test(eddsa_multisig_test eddsa_multisig_test)
target_link_libraries(eddsa_multisig_test GTest::Main threshsign)
