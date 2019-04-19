add_executable(metric_tests metric_test.cpp)
add_test(metric_tests metric_tests)
target_link_libraries(metric_tests gtest_main util)

add_executable(metric_server MetricServerTestMain.cpp)
target_link_libraries(metric_server util)

add_test(NAME metric_server_tests COMMAND python3 -m unittest
    metric_server_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
