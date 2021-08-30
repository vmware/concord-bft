cmake_minimum_required (VERSION 3.2)
project(skvbc_client VERSION 0.1.0.0 LANGUAGES CXX)

set(client_sources
    main.cpp
    ../basicRandomTestsRunner.cpp
    ../simpleKVBTestsBuilder.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

add_executable(skvbc_client ${client_sources})

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(skvbc_client PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
    target_compile_definitions(skvbc_client PUBLIC USE_COMM_TLS_TCP)
endif()

target_link_libraries(skvbc_client PUBLIC kvbc corebft threshsign util test_config_lib skvbc_messages_cmf)

target_include_directories(skvbc_client PUBLIC ..)
target_include_directories(skvbc_client PUBLIC ../..)
target_include_directories(skvbc_client PUBLIC ${libkvbc_SOURCE_DIR}/include)


