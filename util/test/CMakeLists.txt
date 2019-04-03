add_executable(metric_tests metric_test.cpp)

add_test(metric_tests metric_tests)

target_link_libraries(metric_tests gtest_main util)
