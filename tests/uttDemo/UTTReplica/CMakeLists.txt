add_executable(utt_replica main.cpp
		 UTTCommandsHandler.cpp
		 setup.cpp
		 ../utt_blockchain_app.cpp
		 ../transactions.cpp
		 ../utt_config.cpp
		 ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp)

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(utt_replica PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
	target_compile_definitions(utt_replica PUBLIC USE_COMM_TLS_TCP)
endif()

if(BUILD_ROCKSDB_STORAGE)
	target_compile_definitions(utt_replica PUBLIC "USE_ROCKSDB=1")
endif()

target_include_directories(utt_replica
	PUBLIC
		..
		../..
		${libkvbc_SOURCE_DIR}/include
		${UTT_DEMO_INCLUDES}
	PRIVATE
		${bftengine_SOURCE_DIR}/src/preprocessor
)

target_link_libraries(utt_replica PUBLIC kvbc corebft threshsign util test_config_lib stdc++fs utt_messages_cmf ${UTT_DEMO_LINKS})



