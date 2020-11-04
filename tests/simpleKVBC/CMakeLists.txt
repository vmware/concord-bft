add_subdirectory(TesterClient)
add_subdirectory(TesterReplica)
add_custom_target(copy_blockchain_scripts2 ALL COMMENT "Copying scripts abcd")
add_custom_command(TARGET copy_blockchain_scripts2
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/scripts ${CMAKE_CURRENT_BINARY_DIR}/scripts)

add_custom_target(copy_ror_perf_test ALL COMMENT "Copying ror_perf_test")
add_custom_command(TARGET copy_ror_perf_test
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/ror_perf_test ${CMAKE_CURRENT_BINARY_DIR}/ror_perf_test)