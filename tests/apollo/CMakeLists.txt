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
  add_test(NAME skvbc_preexecution_tests_${STORAGE_TYPE} COMMAND sh -c
          "env STORAGE_TYPE=${STORAGE_TYPE} python3 -m unittest test_skvbc_preexecution"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endforeach()
