set(APOLLO_TEST_ENV "BUILD_ROCKSDB_STORAGE=${BUILD_ROCKSDB_STORAGE}")

set(STORAGE_TYPES "v1direct" "v2merkle")
if (CI_TEST_STORAGE_TYPE)
  set(STORAGE_TYPES ${CI_TEST_STORAGE_TYPE})
endif()

foreach(STORAGE_TYPE ${STORAGE_TYPES})
  add_test(NAME skvbc_basic_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_linearizability_tests_${STORAGE_TYPE} COMMAND sudo sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_history_tracker test_skvbc_linearizability 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_fast_path_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_fast_path 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_slow_path_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_slow_path 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_view_change_tests_${STORAGE_TYPE} COMMAND sudo sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_view_change 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_auto_view_change_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_auto_view_change 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_preexecution_tests_${STORAGE_TYPE} COMMAND sh -c
          "env STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_preexecution 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  if (BUILD_ROCKSDB_STORAGE)
    add_test(NAME skvbc_persistence_tests_${STORAGE_TYPE} COMMAND sh -c
            "env STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_persistence 2>&1 > /dev/null"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

    add_test(NAME skvbc_ro_replica_tests_${STORAGE_TYPE} COMMAND sh -c
            "env STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_ro_replica 2>&1 > /dev/null"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
endforeach()
