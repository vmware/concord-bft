add_subdirectory(util)

# APOLLO_TEST_ENV is a list
set(APOLLO_TEST_ENV "BUILD_COMM_TCP_TLS=${BUILD_COMM_TCP_TLS}" 
					"KEEP_APOLLO_LOGS=${KEEP_APOLLO_LOGS}"
					"TXN_SIGNING_ENABLED=${TXN_SIGNING_ENABLED}"
					"TIME_SERVICE_ENABLED=TRUE"
					"USE_FAKE_CLOCK_IN_TIME_SERVICE=${USE_FAKE_CLOCK_IN_TIME_SERVICE}"
					)

if(LEAKCHECK)
  set(ASAN_LOG_DIR "${CMAKE_BINARY_DIR}/asan_logs")
  file(MAKE_DIRECTORY ${ASAN_LOG_DIR})
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  list(APPEND APOLLO_TEST_ENV "GRACEFUL_SHUTDOWN=true")
  list(APPEND APOLLO_TEST_ENV 
  "ASAN_OPTIONS=log_path=${ASAN_LOG_DIR}/asan.log:symbolize=1:check_initialization_order=1:detect_leaks=1 ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer")
endif()

if(THREADCHECK)
  set(TSAN_LOG_DIR "${CMAKE_BINARY_DIR}/tsan_logs")
  file(MAKE_DIRECTORY ${TSAN_LOG_DIR})
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  list(APPEND APOLLO_TEST_ENV "GRACEFUL_SHUTDOWN=true")
  list(APPEND APOLLO_TEST_ENV 
  "TSAN_OPTIONS=log_path=${TSAN_LOG_DIR}/tsan.log:history_size=7 TSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer")
endif()

if(UNDEFINED_BEHAVIOR_CHECK)
  set(UBSAN_LOG_DIR "${CMAKE_BINARY_DIR}/ubsan_logs")
  file(MAKE_DIRECTORY ${UBSAN_LOG_DIR})
  # For Leak Sanitizers to report errors we need to shut down processes gracefully
  list(APPEND APOLLO_TEST_ENV "GRACEFUL_SHUTDOWN=true")
  list(APPEND APOLLO_TEST_ENV
  "UBSAN_OPTIONS=log_path=${UBSAN_LOG_DIR}/ubsan.log:history_size=7:print_stacktrace=1:external_symbolizer_path=/usr/bin/llvm-symbolizer UBSAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer")
endif()

if(HEAPTRACK)
  # shut down processes gracefully, enable heaptrack and set log dir
  # Keep this ENABLE_HEAPTRACK variable empty to support advanced operations in apollo infra
  list(APPEND APOLLO_TEST_ENV "GRACEFUL_SHUTDOWN=true" "ENABLE_HEAPTRACK=")
endif()

if(CODECOVERAGE)
    #[[ We need to gracefully shut down processes in order to
        generate raw profile data for Code Coverage for Apollo tests.]]
    list(APPEND APOLLO_TEST_ENV "GRACEFUL_SHUTDOWN=true" "CODECOVERAGE=true")
endif()

if (OMIT_TEST_OUTPUT)
    set(TEST_OUTPUT "2>&1 > /dev/null")
endif()

#! apollo_test : function for defining an apollo test
#
# \param: NAME  name  			test name
# \param: SUITE suite 			suite file name
# \flag:  V4          			use V4 storage (default is V1)
# \flag:  SUDO        			use sudo when invoking the test
# \group: VARS k1=v1 [k2=v2...] additional environment variables for the test (optional) 
#
# E.g.:
#
# apollo_test(NAME skvbc_basic_tests SUITE test_skvbc)
# apollo_test(NAME skvbc_basic_tests SUITE test_skvbc V4)
# apollo_test(NAME skvbc_view_change_tests SUITE test_skvbc_view_change SUDO)
# apollo_test(NAME skvbc_preexecution_with_result_auth_tests SUITE test_skvbc_preexecution VARS PRE_EXEC_RESULT_AUTH_ENABLED=True)

function(apollo_test)
    set(options V4 SUDO)
    set(oneValueArgs NAME SUITE)
    set(multiValueArgs VARS)
    cmake_parse_arguments(APOLLO_TEST "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    set(APOLLO_TEST_TIMEOUT 3000)
	if(DEFINED APOLLO_TEST_VARS)
		list(JOIN APOLLO_TEST_VARS " " ADDITIONAL_ENV_VARS)
	endif()
	if(APOLLO_TEST_SUDO)
		set(DO_SUDO sudo)
	endif()
	if(APOLLO_TEST_V4)
		set(BLOCKCHAIN_VERSION 4)
		set(APOLLO_TEST_NAME "${APOLLO_TEST_NAME}_v4")
	else()
		set(BLOCKCHAIN_VERSION 1)
	endif()
	list(JOIN APOLLO_TEST_ENV " " APOLLO_ENV)
	add_test(NAME ${APOLLO_TEST_NAME}
			 WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
			 COMMAND ${DO_SUDO} sh -c
        	 "env ${APOLLO_ENV} ${ADDITIONAL_ENV_VARS} BLOCK_CHAIN_VERSION=${BLOCKCHAIN_VERSION} TEST_NAME=${APOLLO_TEST_NAME} python3 -m unittest ${APOLLO_TEST_SUITE} ${TEST_OUTPUT}"
        	)
  set_tests_properties(${APOLLO_TEST_NAME} PROPERTIES TIMEOUT ${APOLLO_TEST_TIMEOUT}) 
endfunction()
	
apollo_test(NAME skvbc_basic_tests SUITE test_skvbc)
apollo_test(NAME skvbc_basic_tests SUITE test_skvbc V4)
if (BUILD_ROCKSDB_STORAGE)
	apollo_test(NAME skvbc_persistence_tests SUITE test_skvbc_persistence)
    apollo_test(NAME skvbc_persistence_tests SUITE test_skvbc_persistence V4)
    if (USE_S3_OBJECT_STORE)
        apollo_test(NAME skvbc_ro_replica_tests SUITE test_skvbc_ro_replica)
        apollo_test(NAME skvbc_ro_replica_tests SUITE test_skvbc_ro_replica V4)
        apollo_test(NAME skvbc_s3_integrity_check_tests SUITE test_skvbc_s3_integrity_check)
		apollo_test(NAME skvbc_s3_integrity_check_tests SUITE test_skvbc_s3_integrity_check V4)
    endif()
endif()

if (USE_S3_OBJECT_STORE AND BUILD_COMM_TCP_TLS AND TXN_SIGNING_ENABLED)
    apollo_test(NAME skvbc_reconfiguration SUITE test_skvbc_reconfiguration)
endif()

apollo_test(NAME skvbc_multi_sig SUITE test_skvbc_multi_sig)
apollo_test(NAME skvbc_linearizability_tests SUITE test_skvbc_linearizability SUDO)
apollo_test(NAME test_skvbc_history_tracker SUITE test_skvbc_history_tracker SUDO)
apollo_test(NAME skvbc_commit_path_tests SUITE test_skvbc_commit_path)
apollo_test(NAME skvbc_view_change_tests SUITE test_skvbc_view_change SUDO)
apollo_test(NAME skvbc_auto_view_change_tests SUITE test_skvbc_auto_view_change)
apollo_test(NAME skvbc_state_transfer_tests SUITE test_skvbc_state_transfer)
apollo_test(NAME skvbc_state_transfer_tests SUITE test_skvbc_state_transfer V4)
#PreExecution tests are executed two times - with PreExecution Result Authentication enabled and disabled.
apollo_test(NAME skvbc_preexecution_tests SUITE test_skvbc_preexecution)
apollo_test(NAME skvbc_preexecution_with_result_auth_tests SUITE test_skvbc_preexecution VARS PRE_EXEC_RESULT_AUTH_ENABLED=True)
apollo_test(NAME skvbc_batch_preexecution_tests SUITE test_skvbc_batch_preexecution)
apollo_test(NAME skvbc_network_partitioning_tests SUITE test_skvbc_network_partitioning)
apollo_test(NAME skvbc_checkpoints SUITE test_skvbc_checkpoints)
apollo_test(NAME skvbc_chaotic_startup SUITE test_skvbc_chaotic_startup SUDO )
apollo_test(NAME skvbc_publish_clients_keys SUITE test_skvbc_publish_clients_keys SUDO)
apollo_test(NAME skvbc_backup_restore SUITE test_skvbc_backup_restore)
apollo_test(NAME skvbc_backup_restore SUITE test_skvbc_backup_restore V4)
apollo_test(NAME skvbc_consensus_batching SUITE test_skvbc_consensus_batching)
apollo_test(NAME skvbc_block_accumulation_tests SUITE test_skvbc_block_accumulation)
# Disabled - see BC-19213
# if (TXN_SIGNING_ENABLED)
#     apollo_test(NAME skvbc_client_transaction_signing SUITE test_skvbc_client_transaction_signing)
# endif()
apollo_test(NAME skvbc_pyclient_tests SUITE test_skvbc_pyclient)
apollo_test(NAME skvbc_cron SUITE test_skvbc_cron)
apollo_test(NAME skvbc_byzantine_primary_preexecution_tests SUITE test_skvbc_byzantine_primary_preexecution)
if (USE_FAKE_CLOCK_IN_TIME_SERVICE)        
	apollo_test(NAME skvbc_time_service SUITE test_skvbc_time_service)
endif()
apollo_test(NAME skvbc_dbsnapshot_tests SUITE test_skvbc_dbsnapshot)
apollo_test(NAME skvbc_reply_tests SUITE test_skvbc_reply)
apollo_test(NAME skvbc_diagnostics_tests SUITE test_skvbc_diagnostics)
apollo_test(NAME osexample_demo_tests SUITE test_osexample_demo)
if(ENABLE_RESTART_RECOVERY_TESTS)
	apollo_test(NAME skvbc_restart_recovery_tests test_skvbc_restart_recovery)
endif()
