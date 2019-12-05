add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)

add_test(NAME skvbc_basic_tests COMMAND sh -c
        "python3 -m unittest test_skvbc 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_linearizability_tests COMMAND sh -c
        "python3 -m unittest test_skvbc_history_tracker test_skvbc_linearizability 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_fast_path_tests COMMAND sh -c
        "python3 -m unittest test_skvbc_fast_path 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_slow_path_tests COMMAND sh -c
        "python3 -m unittest test_skvbc_slow_path 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

if (BUILD_ROCKSDB_STORAGE)
    add_test(NAME skvbc_persistence_tests COMMAND sh -c
            "python3 -m unittest test_skvbc_persistence 2>&1 > /dev/null"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
