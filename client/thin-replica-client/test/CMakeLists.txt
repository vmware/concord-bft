find_package(GTest REQUIRED)
find_package(GMock REQUIRED)

add_test(NAME thin_replica_client_tests COMMAND thin_replica_client_tests)
add_executable(thin_replica_client_tests
  thin_replica_client_test.cpp
  thin_replica_client_mocks.hpp
  thin_replica_client_mocks.cpp)
target_include_directories(thin_replica_client_tests PRIVATE ../src)
target_link_libraries(thin_replica_client_tests
  ${GMOCK_LIBRARY}
  thin_replica_client_lib
  GTest::Main
  GTest::GTest)

add_test(NAME trc_rpc_use_tests COMMAND trc_rpc_use_tests)
add_executable(trc_rpc_use_tests
  trc_rpc_use_test.cpp
  thin_replica_client_mocks.hpp
  thin_replica_client_mocks.cpp)
target_include_directories(trc_rpc_use_tests PRIVATE ../src)
target_link_libraries(trc_rpc_use_tests
  ${GMOCK_LIBRARY}
  thin_replica_client_lib
  GTest::Main
  GTest::GTest)

add_test(NAME trc_byzantine_tests COMMAND trc_byzantine_tests)
add_executable(trc_byzantine_tests
  trc_byzantine_test.cpp
  thin_replica_client_mocks.hpp
  thin_replica_client_mocks.cpp)
target_include_directories(trc_byzantine_tests PRIVATE ../src)
target_link_libraries(trc_byzantine_tests
  ${GMOCK_LIBRARY}
  thin_replica_client_lib
  GTest::Main
  GTest::GTest)

add_test(NAME trc_basic_update_queue_tests COMMAND trc_basic_update_queue_tests)
add_executable(trc_basic_update_queue_tests trc_basic_update_queue_test.cpp)
target_include_directories(trc_basic_update_queue_tests PRIVATE ../src)
target_link_libraries(trc_basic_update_queue_tests
  thin_replica_client_lib
  GTest::Main
  GTest::GTest)

add_test(NAME trc_hash_tests COMMAND trc_hash_tests)
add_executable(trc_hash_tests trc_hash_test.cpp)
target_include_directories(trc_hash_tests PRIVATE ../src)
target_link_libraries(trc_hash_tests
  thin_replica_client_lib
  kvbc
  GTest::Main
  GTest::GTest)
