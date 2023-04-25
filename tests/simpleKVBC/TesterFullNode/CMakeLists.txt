cmake_minimum_required (VERSION 3.2)
project(skvbc_fullnode VERSION 0.1.0.0 LANGUAGES CXX)

add_executable(skvbc_fullnode main.cpp
		fnGrpc.cpp)

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(skvbc_fullnode PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(BUILD_ROCKSDB_STORAGE)
	target_compile_definitions(skvbc_fullnode PUBLIC "USE_ROCKSDB=1")
endif()

target_link_libraries(skvbc_fullnode PUBLIC skvbc_replica_lib bftclient_new clientservice-proto test_config_lib stdc++fs)

target_include_directories(skvbc_fullnode PUBLIC ..)
target_include_directories(skvbc_fullnode PUBLIC ../../TesterReplica/)
target_include_directories(skvbc_fullnode PUBLIC ${libskvbc_replica_lib_SOURCE_DIR})