cmake_minimum_required (VERSION 3.2)
project(skvbc_replica VERSION 0.1.0.0 LANGUAGES CXX)

set(replica_sources
	main.cpp
        internalCommandsHandler.cpp
        ../simpleKVBTestsBuilder.cpp
	${CONFIG_FOLDER_PATH_VALUE}/test_comm_config.cpp
	${CONFIG_FOLDER_PATH_VALUE}/config_file_parser.cpp)

add_executable(skvbc_replica ${replica_sources} $<TARGET_OBJECTS:logging_dev>)

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(skvbc_replica PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
	target_compile_definitions(skvbc_replica PUBLIC USE_COMM_TLS_TCP)
endif()

target_link_libraries(skvbc_replica LINK_PUBLIC simpleKVBC)

target_link_libraries(skvbc_replica PUBLIC corebft threshsign util)

target_include_directories(skvbc_replica PUBLIC ../../SimpleKVBC/include
    ../include)
target_include_directories(skvbc_replica PUBLIC ..)
target_include_directories(skvbc_replica PUBLIC ${CONFIG_FOLDER_PATH_VALUE})


