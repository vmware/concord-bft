add_test(NAME skvbc_tests COMMAND python3 -m unittest
    test_skvbc
    test_skvbc_chaos
    test_skvbc_linearizability
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

if (BUILD_ROCKSDB_STORAGE)
  add_test(NAME skvbc_persistence_tests COMMAND python3 -m unittest
      test_skvbc_persistence
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
