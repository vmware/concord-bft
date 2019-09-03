add_executable(metric_tests metric_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(metric_tests metric_tests)
target_link_libraries(metric_tests gtest_main util)
target_compile_options(metric_tests PUBLIC -Wno-sign-compare)

add_executable(metric_server MetricServerTestMain.cpp $<TARGET_OBJECTS:logging_dev>)
target_link_libraries(metric_server util)

add_test(NAME metric_server_tests COMMAND python3 -m unittest
    metric_server_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(mt_tests multithreading.cpp)
add_test(util_mt_tests mt_tests)
target_link_libraries(mt_tests gtest_main util)
target_compile_options(mt_tests PUBLIC -Wno-sign-compare)

add_executable(sliver_test sliver_test.cpp)
add_test(sliver_test sliver_test)
target_link_libraries(sliver_test gtest util)
target_compile_options(sliver_test PUBLIC -Wno-sign-compare)

add_executable(serializable_test serializable_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(serializable_test serializable_test)
target_link_libraries(serializable_test gtest util)
target_compile_options(serializable_test PUBLIC -Wno-sign-compare)
