add_subdirectory(util)
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
  set(ASAN_LOG_DIR "${CMAKE_BINARY_DIR}/asan_logs")
  file(MAKE_DIRECTORY ${ASAN_LOG_DIR})
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} GRACEFUL_SHUTDOWN=true")
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} \
  ASAN_OPTIONS=log_path=${ASAN_LOG_DIR}/asan.log:symbolize=1:check_initialization_order=1:detect_leaks=1 \
  ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer\
  ")
endif()

if(THREADCHECK)
  set(TSAN_LOG_DIR "${CMAKE_BINARY_DIR}/tsan_logs")
  file(MAKE_DIRECTORY ${TSAN_LOG_DIR})
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} GRACEFUL_SHUTDOWN=true")
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} \
  TSAN_OPTIONS=log_path=${TSAN_LOG_DIR}/tsan.log:history_size=7 \
  TSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer\
  ")
endif()

if(UNDEFINED_BEHAVIOR_CHECK)
  set(UBSAN_LOG_DIR "${CMAKE_BINARY_DIR}/ubsan_logs")
  file(MAKE_DIRECTORY ${UBSAN_LOG_DIR})
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} GRACEFUL_SHUTDOWN=true")
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} \
  UBSAN_OPTIONS=log_path=${UBSAN_LOG_DIR}/ubsan.log:history_size=7:print_stacktrace=1:external_symbolizer_path=/usr/bin/llvm-symbolizer \
  UBSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer\
  ")
endif()

if(HEAPTRACK)
  # shut down processes gracefully, enable heaptrack and set log dir
  set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} GRACEFUL_SHUTDOWN=true ENABLE_HEAPTRACK=TRUE")
endif()

if(CODECOVERAGE)
    #[[ We need to gracefully shut down processes in order to
        generate raw profile data for Code Coverage for Apollo tests.]]
    set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} GRACEFUL_SHUTDOWN=true")
    set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} CODECOVERAGE=true")
endif()

set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} KEEP_APOLLO_LOGS=${KEEP_APOLLO_LOGS}")
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} TXN_SIGNING_ENABLED=${TXN_SIGNING_ENABLED}")
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} TIME_SERVICE_ENABLED=TRUE")
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=1")
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} USE_FAKE_CLOCK_IN_TIME_SERVICE=${USE_FAKE_CLOCK_IN_TIME_SERVICE}")

if (OMIT_TEST_OUTPUT)
    set(TEST_OUTPUT "2>&1 > /dev/null")
endif()

add_test(NAME skvbc_basic_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_basic_tests python3 -m unittest test_skvbc ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=4")
add_test(NAME skvbc_basic_tests_v4 COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_basic_tests python3 -m unittest test_skvbc ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=1")


if (BUILD_ROCKSDB_STORAGE)
    add_test(NAME skvbc_persistence_tests COMMAND sh -c
            "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_persistence_tests python3 -m unittest test_skvbc_persistence ${TEST_OUTPUT}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

    set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=4")
    add_test(NAME skvbc_persistence_tests_v4 COMMAND sh -c
            "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_persistence_tests python3 -m unittest test_skvbc_persistence ${TEST_OUTPUT}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=1")

    if (USE_S3_OBJECT_STORE)
        add_test(NAME skvbc_ro_replica_tests_1 COMMAND sh -c
                "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_ro_replica_tests python3 -m unittest test_skvbc_ro_replica ${TEST_OUTPUT}"
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
        add_test(NAME skvbc_s3_integrity_check_tests_1 COMMAND sh -c
                "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_s3_integrity_check_tests python3 -m unittest test_skvbc_s3_integrity_check ${TEST_OUTPUT}"
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
        set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=4")
        add_test(NAME skvbc_ro_replica_tests_4 COMMAND sh -c
                "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_ro_replica_tests python3 -m unittest test_skvbc_ro_replica ${TEST_OUTPUT}"
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
        add_test(NAME skvbc_s3_integrity_check_tests_4 COMMAND sh -c
                "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_s3_integrity_check_tests python3 -m unittest test_skvbc_s3_integrity_check ${TEST_OUTPUT}"
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
        set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=1")
    endif()
endif()

if (USE_S3_OBJECT_STORE AND BUILD_COMM_TCP_TLS AND TXN_SIGNING_ENABLED)
    add_test(NAME skvbc_reconfiguration COMMAND sh -c
            "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_reconfiguration python3 -m unittest test_skvbc_reconfiguration ${TEST_OUTPUT}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()

add_test(NAME skvbc_multi_sig COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_multi_sig python3 -m unittest test_skvbc_multi_sig ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_linearizability_tests COMMAND sudo sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_linearizability_tests python3 -m unittest test_skvbc_linearizability ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME test_skvbc_history_tracker COMMAND sudo sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=test_skvbc_history_tracker python3 -m unittest test_skvbc_history_tracker ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_commit_path_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_commit_path_tests python3 -m unittest test_skvbc_commit_path ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_view_change_tests COMMAND sudo sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_view_change_tests python3 -m unittest test_skvbc_view_change ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_auto_view_change_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_auto_view_change_tests python3 -m unittest test_skvbc_auto_view_change ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# add_test(NAME skvbc_state_transfer_tests COMMAND sh -c
#         "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_state_transfer_tests python3 -m unittest test_skvbc_state_transfer ${TEST_OUTPUT}"
#         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=4")
add_test(NAME skvbc_state_transfer_tests_v4 COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_state_transfer_tests python3 -m unittest test_skvbc_state_transfer ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=1")

# PreExecution tests are executed two times - with PreExecution Result Authentication enabled and disabled.
add_test(NAME skvbc_preexecution_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_preexecution_tests python3 -m unittest test_skvbc_preexecution ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
add_test(NAME skvbc_preexecution_with_result_auth_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} PRE_EXEC_RESULT_AUTH_ENABLED=True TEST_NAME=skvbc_preexecution_with_result_auth_tests python3 -m unittest test_skvbc_preexecution ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_batch_preexecution_tests COMMAND sh -c
          "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_batch_preexecution_tests python3 -m unittest test_skvbc_batch_preexecution ${TEST_OUTPUT}"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_network_partitioning_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_network_partitioning_tests python3 -m unittest test_skvbc_network_partitioning ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_checkpoints COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_checkpoints python3 -m unittest test_skvbc_checkpoints 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_chaotic_startup COMMAND sudo sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_chaotic_startup python3 -m unittest test_skvbc_chaotic_startup 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_publish_clients_keys COMMAND sudo sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_publish_clients_keys python3 -m unittest test_skvbc_publish_clients_keys 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_backup_restore COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_backup_restore python3 -m unittest test_skvbc_backup_restore 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=4")
add_test(NAME skvbc_backup_restore_v4 COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_backup_restore python3 -m unittest test_skvbc_backup_restore 2>&1 > /dev/null"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
set(APOLLO_TEST_ENV "${APOLLO_TEST_ENV} BLOCKCHAIN_VERSION=1")


add_test(NAME skvbc_consensus_batching COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_consensus_batching python3 -m unittest test_skvbc_consensus_batching ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_block_accumulation_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_block_accumulation_tests python3 -m unittest test_skvbc_block_accumulation ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# Disabled - see BC-19213
# if (TXN_SIGNING_ENABLED)
#     add_test(NAME skvbc_client_transaction_signing COMMAND sh -c
#             "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_client_transaction_signing python3 -m unittest test_skvbc_client_transaction_signing ${TEST_OUTPUT}"
#             WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
# endif()

add_test(NAME skvbc_pyclient_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_pyclient_tests python3 -m unittest test_skvbc_pyclient ${TEST_OUTPUT}"
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_cron COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_cron python3 -m unittest test_skvbc_cron ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_byzantine_primary_preexecution_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_byzantine_primary_preexecution_tests python3 -m unittest test_skvbc_byzantine_primary_preexecution ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

if (USE_FAKE_CLOCK_IN_TIME_SERVICE)        
    add_test(NAME skvbc_time_service COMMAND sh -c
              "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_time_service python3 -m unittest test_skvbc_time_service ${TEST_OUTPUT}"
              WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()

add_test(NAME skvbc_dbsnapshot_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_dbsnapshot_tests python3 -m unittest test_skvbc_dbsnapshot ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_reply_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_reply_tests python3 -m unittest test_skvbc_reply ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME skvbc_diagnostics_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_diagnostics_tests python3 -m unittest test_skvbc_diagnostics ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

if(ENABLE_RESTART_RECOVERY_TESTS)
add_test(NAME skvbc_restart_recovery_tests COMMAND sh -c
        "env ${APOLLO_TEST_ENV} BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS} TEST_NAME=skvbc_restart_recovery_tests python3 -m unittest test_skvbc_restart_recovery ${TEST_OUTPUT}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
