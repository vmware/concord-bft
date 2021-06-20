find_package(GTest REQUIRED)
find_package(cryptopp REQUIRED)

add_executable(bft_client_test bft_client_test.cpp)
add_test(bft_client_test bft_client_test)
target_include_directories(bft_client_test PRIVATE ../src)
target_link_libraries(bft_client_test PRIVATE
  GTest::Main
  GTest::GTest
  bftclient_new
  threshsign
  cryptopp
  secretsmanager
)

add_executable(bft_client_api_tests bft_client_api_tests.cpp)
add_test(bft_client_api_tests bft_client_api_tests)
target_include_directories(bft_client_api_tests PRIVATE ../src)
target_include_directories(corebft PUBLIC ${CRYPTOPP_INCLUDE_DIRS})
target_link_libraries(bft_client_api_tests PRIVATE
  GTest::Main
  GTest::GTest
  bftclient_new
  threshsign
  cryptopp
  secretsmanager
)
