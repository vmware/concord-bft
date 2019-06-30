add_executable(metric_tests metric_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(metric_tests metric_tests)
target_link_libraries(metric_tests gtest_main util)

add_executable(metric_server MetricServerTestMain.cpp $<TARGET_OBJECTS:logging_dev>)
target_link_libraries(metric_server util)

add_test(NAME metric_server_tests COMMAND python3 -m unittest
    metric_server_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(mt_tests multithreading.cpp)
add_test(util_mt_tests mt_tests)
target_link_libraries(mt_tests gtest_main util)