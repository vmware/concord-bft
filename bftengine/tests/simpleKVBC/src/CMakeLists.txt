cmake_minimum_required (VERSION 3.2)
project(libsimpleKVBC VERSION 0.1.0.0 LANGUAGES CXX)

set(simpleKVBC_sources
	ClientImp.cpp
	ReplicaImp.cpp
        replica_state_sync_imp.cpp
        block_metadata.cpp
	${CONFIG_FOLDER_PATH_VALUE}/test_comm_config.cpp
	../../../../tools/KeyfileIOUtils.cpp
)

add_library(simpleKVBC ${simpleKVBC_sources})
target_link_libraries(simpleKVBC PUBLIC corebft threshsign util concordbft_storage)

target_include_directories(simpleKVBC PUBLIC .)
target_include_directories(simpleKVBC PUBLIC ../../../../tools)
target_include_directories(simpleKVBC PUBLIC ../include)
target_include_directories(simpleKVBC PUBLIC ${bftengine_SOURCE_DIR}/tests/simpleStorage)
target_include_directories(simpleKVBC PUBLIC ${CONFIG_FOLDER_PATH_VALUE})
