cmake_minimum_required (VERSION 3.2)
project(byzantine_replica VERSION 0.1.0.0 LANGUAGES CXX)

add_executable(byzantine_replica ByzantineReplicaImp.cpp
								 main.cpp
								 internalCommandsHandler.cpp
								 setup.cpp
   								 ../simpleKVBTestsBuilder.cpp
       							 ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp)

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(byzantine_replica PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
	target_compile_definitions(byzantine_replica PUBLIC USE_COMM_TLS_TCP)
endif()

if(BUILD_ROCKSDB_STORAGE)
	target_compile_definitions(byzantine_replica PUBLIC "USE_ROCKSDB=1")
endif()

target_link_libraries(byzantine_replica PUBLIC kvbc corebft threshsign util test_config_lib )

target_include_directories(byzantine_replica PUBLIC ..)
target_include_directories(byzantine_replica PUBLIC ../..)
target_include_directories(byzantine_replica PUBLIC ${libkvbc_SOURCE_DIR}/include)
target_include_directories(byzantine_replica PUBLIC ${CMAKE_SOURCE_DIR}/bftengine/src)
