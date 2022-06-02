set(client_sources
    main.cpp
	UTTClientApp.cpp
	PrintState.cpp
    ../utt_blockchain_app.cpp
    ../transactions.cpp
	../utt_config.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp
)

add_executable(utt_client ${client_sources})

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(utt_client PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
    target_compile_definitions(utt_client PUBLIC USE_COMM_TLS_TCP)
endif()

target_include_directories(utt_client
	PUBLIC
		..
		../..
		${libkvbc_SOURCE_DIR}/include
		${UTT_DEMO_INCLUDES}
)

target_link_libraries(utt_client PUBLIC kvbc corebft threshsign util test_config_lib utt_messages_cmf ${UTT_DEMO_LINKS})


