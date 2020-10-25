set(APOLLO_TEST_ENV "BUILD_ROCKSDB_STORAGE=${BUILD_ROCKSDB_STORAGE}")
set(MINIO_BINARY_PATH "$ENV{HOME}/minio")

if(USE_S3_OBJECT_STORE)
        find_program(CHMOD_PROG chmod)
        if(NOT CHMOD_PROG)
                message(FATAL_ERROR "chmod not found")
        endif()
        if(NOT EXISTS ${MINIO_BINARY_PATH})
            message(STATUS "Minio does not exsit in ${MINIO_BINARY_PATH}")
        endif()
        # This env var is used by apollo tests to locate the path to the binary
        set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} CONCORD_BFT_MINIO_BINARY_PATH=${MINIO_BINARY_PATH}")
endif()

if(LEAKCHECK)
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} GRACEFUL_SHUTDOWN=true")
endif()

set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} KEEP_APOLLO_LOGS=${KEEP_APOLLO_LOGS}")

set(STORAGE_TYPES "v1direct" "v2merkle")
if (CI_TEST_STORAGE_TYPE)
  set(STORAGE_TYPES ${CI_TEST_STORAGE_TYPE})
endif()

if (OMIT_TEST_OUTPUT)
    set(TEST_OUTPUT "2>&1 > /dev/null")
endif()

foreach(STORAGE_TYPE ${STORAGE_TYPES})
  add_test(NAME skvbc_basic_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_basic_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_multi_sig_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_multi_sig_${STORAGE_TYPE} python3 -m unittest test_skvbc_multi_sig ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_linearizability_tests_${STORAGE_TYPE} COMMAND sudo sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_linearizability_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_linearizability ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME test_skvbc_history_tracker_${STORAGE_TYPE} COMMAND sudo sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=test_skvbc_history_tracker_${STORAGE_TYPE} python3 -m unittest test_skvbc_history_tracker ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_fast_path_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_fast_path_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_fast_path ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_slow_path_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_slow_path_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_slow_path ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_view_change_tests_${STORAGE_TYPE} COMMAND sudo sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_view_change_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_view_change ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_auto_view_change_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_auto_view_change_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_auto_view_change ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_preexecution_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_preexecution_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_preexecution ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_network_partitioning_tests_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_network_partitioning_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_network_partitioning ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_checkpoints_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_checkpoints_${STORAGE_TYPE} python3 -m unittest test_skvbc_checkpoints 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_chaotic_startup_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_chaotic_startup_${STORAGE_TYPE} python3 -m unittest test_skvbc_chaotic_startup 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_backup_restore_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_backup_restore_${STORAGE_TYPE} python3 -m unittest test_skvbc_backup_restore 2>&1 > /dev/null"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  add_test(NAME skvbc_reconfiguration_${STORAGE_TYPE} COMMAND sh -c
          "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_reconfiguration_${STORAGE_TYPE} python3 -m unittest test_skvbc_reconfiguration ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  if (BUILD_ROCKSDB_STORAGE)
    add_test(NAME skvbc_persistence_tests_${STORAGE_TYPE} COMMAND sh -c
            "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_persistence_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_persistence ${TEST_OUTPUT}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

    add_test(NAME skvbc_ro_replica_tests_${STORAGE_TYPE} COMMAND sh -c
            "env ${APOLLO_TEST_ENV} STORAGE_TYPE=${STORAGE_TYPE} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_ro_replica_tests_${STORAGE_TYPE} python3 -m unittest test_skvbc_ro_replica ${TEST_OUTPUT}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
endforeach()
