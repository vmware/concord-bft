cmake_minimum_required (VERSION 3.2)
project(skvbc_replica VERSION 0.1.0.0 LANGUAGES CXX)

set(replica_sources
	main.cpp
	../simpleKVBCTests.cpp
)

add_executable(skvbc_replica ${replica_sources})

target_link_libraries(skvbc_replica LINK_PUBLIC simpleKVBC)

target_link_libraries(skvbc_replica PUBLIC corebft)


target_include_directories(skvbc_replica PUBLIC ../../SimpleKVBC/include)
target_include_directories(skvbc_replica PUBLIC ..)

