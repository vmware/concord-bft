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

target_include_directories(simpleKVBC PUBLIC .)
target_include_directories(simpleKVBC PUBLIC ../../../../bftengine)
target_include_directories(simpleKVBC PUBLIC ../../../../bftengine/include/bcstatetransfer)
target_include_directories(simpleKVBC PUBLIC ../../../../bftengine/include/communication)
target_include_directories(simpleKVBC PUBLIC ../../../../bftengine/include/bftengine)
target_include_directories(simpleKVBC PUBLIC ../../../../bftengine/include)
target_include_directories(simpleKVBC PUBLIC ../../../../threshsign/include)
target_include_directories(simpleKVBC PUBLIC ../../../../logging/include)
target_include_directories(simpleKVBC PUBLIC ../../../../tools)
target_include_directories(simpleKVBC PUBLIC ../include)