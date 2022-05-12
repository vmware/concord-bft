project("Thin Replica Server")

add_subdirectory("proto")

file(GLOB thin_replica_server_src "src/grpc_services.cpp" "src/replica_state_snapshot_service_impl.cpp")

add_library(thin_replica_server ${thin_replica_server_src})
target_include_directories(thin_replica_server PUBLIC include)

if(RUN_PERF_TRS_TRC_TOOL)
  if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/thin_replica_impl_perf.hpp)
    execute_process(COMMAND python3 create_thin_replica_perf_script.py WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../tools/trs_performance)
  endif()
  target_compile_definitions(thin_replica_server PUBLIC RUN_PERF_TRS_TRC_TOOL=1)
endif()

target_link_libraries(thin_replica_server PUBLIC
  concord_block_update
  thin-replica-proto
  replica-state-snapshot-proto
  kvbc
  logging
  util
)

# Unit tests
file(COPY test/resources/trs_trc_tls_certs DESTINATION test/resources)
file(COPY test/resources/tls_certs DESTINATION test/resources)
if (BUILD_TESTING)
    add_subdirectory(test)
endif()
