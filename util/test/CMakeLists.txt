find_package(GTest REQUIRED)

if(NOT BUILD_THIRDPARTY)
    find_package(OpenSSL REQUIRED)
endif()

add_executable(metric_tests metric_test.cpp)
add_test(metric_tests metric_tests)
target_link_libraries(metric_tests GTest::Main util)
target_compile_options(metric_tests PUBLIC -Wno-sign-compare)

add_executable(metric_server MetricServerTestMain.cpp )
target_link_libraries(metric_server util)

add_test(NAME metric_server_tests COMMAND python3 -m unittest
        metric_server_test
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

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

add_executable(sha_hash_tests sha_hash_tests.cpp)
add_test(sha_hash_tests sha_hash_tests)
target_link_libraries(sha_hash_tests GTest::Main util OpenSSL::Crypto)

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

add_executable(scope_exit_test scope_exit_test.cpp)
add_test(scope_exit_test scope_exit_test)
target_link_libraries(scope_exit_test GTest::Main util)

add_executable(lru_cache_test lru_cache_test.cpp )
add_test(lru_cache_test lru_cache_test)
target_link_libraries(lru_cache_test GTest::Main util)

add_executable(openssl_crypto_wrapper_test openssl_crypto_wrapper_tests.cpp)
add_test(openssl_crypto_wrapper_test openssl_crypto_wrapper_test)
target_link_libraries(openssl_crypto_wrapper_test GTest::Main util)

add_executable(simple_memory_pool_test simple_memory_pool_test.cpp)
add_test(simple_memory_pool_test simple_memory_pool_test)
target_link_libraries(simple_memory_pool_test GTest::Main util)