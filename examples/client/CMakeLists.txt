project (test_client LANGUAGES CXX)

set(test_client_sources
    main.cpp
    src/SetupClient.cpp
    src/MessageSender.cpp
    src/KVMessageGenerator.cpp
    src/KVMessageConsumer.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

add_executable(test_client ${test_client_sources})

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(test_client PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
    target_compile_definitions(test_client PUBLIC USE_COMM_TLS_TCP)
endif()

target_include_directories(test_client PUBLIC
         ..
         ../..
         ${CMAKE_SOURCE_DIR}/tests/config
         ${CMAKE_CURRENT_SOURCE_DIR}
         include
         src
)

target_link_libraries(test_client PUBLIC
         kvbc
         corebft
         threshsign
         util
         yaml-cpp
         test_config_lib
         stdc++fs
         bftclient
         bftclient_new
         cmf_kv_replica_msgs
)
