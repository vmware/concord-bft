cmake_minimum_required (VERSION 3.2)
project(libsimpleKVBC VERSION 0.1.0.0 LANGUAGES CXX)

set(simpleKVBC_sources
	BlockchainDBAdapter.cpp
	ClientImp.cpp
	Comparators.cpp
	ReplicaImp.cpp
	InMemoryDBClient.cpp
	Slice.cpp
	../../../../tools/KeyfileIOUtils.cpp
)

add_library(simpleKVBC ${simpleKVBC_sources})
target_link_libraries(simpleKVBC PUBLIC corebft threshsign util logging)

target_include_directories(simpleKVBC PUBLIC .)
target_include_directories(simpleKVBC PUBLIC ../../../../tools)
target_include_directories(simpleKVBC PUBLIC ../include)
