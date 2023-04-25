cmake_minimum_required (VERSION 3.2)
project(skvbc_replica VERSION 0.1.0.0 LANGUAGES CXX)

add_library(skvbc_replica_lib 
    internalCommandsHandler.cpp
    setup.cpp
    WrapCommunication.cpp
    strategy/ShufflePrePrepareMsgStrategy.cpp
    strategy/StrategyUtils.cpp
    strategy/MangledPreProcessResultMsgStrategy.cpp
    strategy/CorruptCheckpointMsgStrategy.cpp)

target_link_libraries(skvbc_replica_lib PUBLIC kvbc corebft threshsign util concord-crypto test_config_lib stdc++fs skvbc_messages_cmf)

target_include_directories(skvbc_replica_lib PUBLIC ..)
target_include_directories(skvbc_replica_lib PUBLIC ../..)
target_include_directories(skvbc_replica_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(skvbc_replica_lib PUBLIC ${libkvbc_SOURCE_DIR}/include)
target_include_directories(skvbc_replica_lib PRIVATE ${bftengine_SOURCE_DIR}/src/preprocessor)

add_executable(skvbc_replica main.cpp)

target_link_libraries(skvbc_replica PUBLIC skvbc_replica_lib)

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(skvbc_replica PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
	target_compile_definitions(skvbc_replica PUBLIC USE_COMM_TLS_TCP)
endif()

if(BUILD_ROCKSDB_STORAGE)
	target_compile_definitions(skvbc_replica PUBLIC "USE_ROCKSDB=1")
endif()

target_include_directories(skvbc_replica PUBLIC ..)
target_include_directories(skvbc_replica PUBLIC ../..)
target_include_directories(skvbc_replica PUBLIC ${libkvbc_SOURCE_DIR}/include)
target_include_directories(skvbc_replica PRIVATE ${bftengine_SOURCE_DIR}/src/preprocessor)