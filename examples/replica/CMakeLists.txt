project (test_replica LANGUAGES CXX)

set(test_replica_source 
        main.cpp
        src/KVCommandHandler.cpp
        src/SetupReplica.cpp
        ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)
add_executable(test_replica ${test_replica_source})

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(test_replica PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
	target_compile_definitions(test_replica PUBLIC USE_COMM_TLS_TCP)
endif()

if(BUILD_ROCKSDB_STORAGE)
	target_compile_definitions(test_replica PUBLIC "USE_ROCKSDB=1")
endif()

target_include_directories(test_replica PUBLIC
         ..
         ../..
         ${CMAKE_CURRENT_SOURCE_DIR}
         include
         src
         ${CMAKE_SOURCE_DIR}/tests/config
         ${libkvbc_SOURCE_DIR}/include
)
target_include_directories(test_replica PRIVATE
         ${CMAKE_SOURCE_DIR}/util/include
         ${concord_bft_tools_SOURCE_DIR}
         ${bftengine_SOURCE_DIR}/include
         ${bftengine_SOURCE_DIR}/src/preprocessor
)
target_link_libraries(test_replica PUBLIC
         kvbc
         corebft
         threshsign
         util
         yaml-cpp
         test_config_lib
         stdc++fs
         cmf_kv_replica_msgs
)
