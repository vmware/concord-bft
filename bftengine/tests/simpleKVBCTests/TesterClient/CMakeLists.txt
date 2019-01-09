cmake_minimum_required (VERSION 3.2)
project(skvbc_client VERSION 0.1.0.0 LANGUAGES CXX)

set(client_sources
	main.cpp
	../simpleKVBCTests.cpp
)

add_executable(skvbc_client ${client_sources})

target_link_libraries(skvbc_client LINK_PUBLIC simpleKVBC)

target_link_libraries(skvbc_client PUBLIC threshsign)

target_link_libraries(skvbc_client PUBLIC corebft)

target_include_directories(skvbc_client PUBLIC ../../SimpleKVBC/include)
target_include_directories(skvbc_client PUBLIC ..)

