add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)

add_test(NAME skvbc_basic_tests COMMAND sudo python3 -m unittest
        test_skvbc
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_linearizability_tests COMMAND sudo python3 -m unittest
        test_skvbc_history_tracker
        test_skvbc_linearizability
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

if (BUILD_ROCKSDB_STORAGE)
    add_test(NAME skvbc_persistence_tests COMMAND sudo python3 -m unittest
            test_skvbc_persistence
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
