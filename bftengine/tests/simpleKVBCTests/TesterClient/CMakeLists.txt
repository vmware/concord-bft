cmake_minimum_required (VERSION 3.2)
project(skvbc_client VERSION 0.1.0.0 LANGUAGES CXX)

set(client_sources
	main.cpp
	../simpleKVBCTests.cpp
	${CONFIG_FOLDER_PATH_VALUE}/test_comm_config.cpp
	${CONFIG_FOLDER_PATH_VALUE}/config_file_parser.cpp)

add_executable(skvbc_client ${client_sources})

if(USE_LOG4CPP)
	target_compile_definitions(skvbc_client PUBLIC USE_LOG4CPP=1)
endif()

if(${USE_COMM_PLAIN_TCP})
	target_compile_definitions(skvbc_client PUBLIC USE_COMM_PLAIN_TCP)
endif()

if(${USE_COMM_TLS_TCP})
	target_compile_definitions(skvbc_client PUBLIC USE_COMM_TLS_TCP)
endif()

target_link_libraries(skvbc_client LINK_PUBLIC simpleKVBC)

target_link_libraries(skvbc_client PUBLIC threshsign)

target_link_libraries(skvbc_client PUBLIC corebft)

target_include_directories(skvbc_client PUBLIC ../../SimpleKVBC/include)
target_include_directories(skvbc_client PUBLIC ..)
target_include_directories(skvbc_client PUBLIC ${CONFIG_FOLDER_PATH_VALUE})


