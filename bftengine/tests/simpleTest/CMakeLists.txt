set(simpleTest_client_source_files
    client.cpp
    config.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

set(simpleTest_replica_source_files
    replica.cpp
    config.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

add_executable(simpleTest_client 
    ${simpleTest_client_source_files}
)

add_executable(simpleTest_server
    ${simpleTest_replica_source_files}
)

if(USE_LOG4CPP)
    target_compile_definitions(simpleTest_server PUBLIC USE_LOG4CPP=1)    
    target_compile_definitions(simpleTest_client PUBLIC USE_LOG4CPP=1)    
endif()

if(${USE_COMM_PLAIN_TCP})
    target_compile_definitions(simnpleTest_client PUBLIC USE_COMM_PLAIN_TCP=1)
    target_compile_definitions(simnpleTest_server PUBLIC USE_COMM_PLAIN_TCP=1)
endif()

set_target_properties(simpleTest_client PROPERTIES OUTPUT_NAME client)
set_target_properties(simpleTest_server PROPERTIES OUTPUT_NAME server)

target_link_libraries(simpleTest_client PUBLIC corebft)
target_link_libraries(simpleTest_server PUBLIC corebft)

target_include_directories(simpleTest_client
                           PRIVATE
                           ${concord_bft_tools_SOURCE_DIR}
                           ${bftengine_SOURCE_DIR}/include)
target_include_directories(simpleTest_server
                           PRIVATE
                           ${concord_bft_tools_SOURCE_DIR}
                           ${bftengine_SOURCE_DIR}/include)

add_custom_target(copy_simple_test_scripts ALL COMMENT "Copying scripts of simpleTest")
add_custom_command(TARGET copy_simple_test_scripts
    COMMAND ${CMAKE_COMMAND} -E make_directory $<TARGET_FILE_DIR:simpleTest_client>/scripts
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/scripts $<TARGET_FILE_DIR:simpleTest_client>/scripts)

    
