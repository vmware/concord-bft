cmake_minimum_required (VERSION 3.2)
project(skvbc_cre VERSION 0.1.0.0 LANGUAGES CXX)

set(cre_sources
    main.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

add_executable(skvbc_cre ${cre_sources})

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(skvbc_cre PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
    target_compile_definitions(skvbc_cre PUBLIC USE_COMM_TLS_TCP)
endif()

target_link_libraries(skvbc_cre PUBLIC cre kvbc corebft threshsign util cmf_messages bftclient_new test_config_lib stdc++fs)

target_include_directories(skvbc_cre PUBLIC ..)
target_include_directories(skvbc_cre PUBLIC ../..)

