cmake_minimum_required (VERSION 3.2)
project(utt_replica VERSION 0.1.0.0 LANGUAGES CXX)

add_executable(utt_replica main.cpp
		 UTTCommandsHandler.cpp
		 setup.cpp
		 ../app_state.cpp
		 ../transactions.cpp
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

target_link_libraries(utt_replica PUBLIC kvbc corebft threshsign util test_config_lib stdc++fs skvbc_messages_cmf)

target_include_directories(utt_replica PUBLIC ..)
target_include_directories(utt_replica PUBLIC ../..)
target_include_directories(utt_replica PUBLIC ${libkvbc_SOURCE_DIR}/include)
target_include_directories(utt_replica PRIVATE ${bftengine_SOURCE_DIR}/src/preprocessor)



