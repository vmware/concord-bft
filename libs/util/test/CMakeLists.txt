find_package(GTest REQUIRED)

add_executable(metric_tests metric_test.cpp)
add_test(metric_tests metric_tests)
target_link_libraries(metric_tests GTest::Main util)
target_compile_options(metric_tests PUBLIC -Wno-sign-compare)

add_executable(metric_server MetricServerTestMain.cpp )
target_link_libraries(metric_server util)

add_executable(mt_tests multithreading.cpp)
add_test(util_mt_tests mt_tests)
target_link_libraries(mt_tests GTest::Main util )
target_compile_options(mt_tests PUBLIC -Wno-sign-compare)

add_executable(sliver_test sliver_test.cpp )
add_test(sliver_test sliver_test)
target_link_libraries(sliver_test GTest::Main util)
target_compile_options(sliver_test PUBLIC -Wno-sign-compare)

add_executable(serializable_test serializable_test.cpp )
add_test(serializable_test serializable_test)
target_link_libraries(serializable_test GTest::Main util)
target_compile_options(serializable_test PUBLIC -Wno-sign-compare)

add_executable(timers_tests timers_tests.cpp )
add_test(timers_tests timers_tests)
target_link_libraries(timers_tests GTest::Main util)


add_executable(RollingAvgAndVar_test RollingAvgAndVar_test.cpp )
add_test(RollingAvgAndVar_test RollingAvgAndVar_test)
target_link_libraries(RollingAvgAndVar_test GTest::Main util)

add_executable(thread_pool_test thread_pool_test.cpp)
add_test(thread_pool_test thread_pool_test)
target_link_libraries(thread_pool_test GTest::Main util)

add_executable(hex_tools_test hex_tools_test.cpp)
add_test(hex_tools_test hex_tools_test)
target_link_libraries(hex_tools_test GTest::Main util)

add_executable(callback_registry_test callback_registry_test.cpp)
add_test(callback_registry_test callback_registry_test)
target_link_libraries(callback_registry_test GTest::Main util)

add_executable(lru_cache_test lru_cache_test.cpp )
add_test(lru_cache_test lru_cache_test)
target_link_libraries(lru_cache_test GTest::Main util)

add_executable(simple_memory_pool_test simple_memory_pool_test.cpp)
add_test(simple_memory_pool_test simple_memory_pool_test)
target_link_libraries(simple_memory_pool_test GTest::Main util)

add_executable(MultiSizeBufferPool_test MultiSizeBufferPool_test.cpp)
add_test(MultiSizeBufferPool_test MultiSizeBufferPool_test)
target_link_libraries(MultiSizeBufferPool_test GTest::Main util)

add_executable(RawMemoryPool_test RawMemoryPool_test.cpp)
add_test(RawMemoryPool_test RawMemoryPool_test)
target_link_libraries(RawMemoryPool_test GTest::Main util)

