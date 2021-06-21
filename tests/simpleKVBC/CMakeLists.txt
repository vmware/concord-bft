add_subdirectory(cmf)
add_subdirectory(TesterClient)
add_subdirectory(TesterReplica)
add_custom_target(copy_blockchain_scripts2 ALL COMMENT "Copying scripts abcd")
add_custom_command(TARGET copy_blockchain_scripts2
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/scripts ${CMAKE_CURRENT_BINARY_DIR}/scripts)
