find_package(GTest REQUIRED)
find_package(GMock REQUIRED)

add_test(NAME cc_basic_update_queue_tests COMMAND cc_basic_update_queue_tests)
add_executable(cc_basic_update_queue_tests cc_basic_update_queue_test.cpp)
target_include_directories(cc_basic_update_queue_tests PRIVATE ../src)
target_link_libraries(cc_basic_update_queue_tests
  concordclient-event-api
  GTest::Main
  GTest::GTest)

