cmake_minimum_required (VERSION 3.2)
project(libkvbc VERSION 0.1.0.0 LANGUAGES CXX)


add_library(concord_block_update INTERFACE)
target_include_directories(concord_block_update INTERFACE
  block_update
  include
)

add_library(kvbc  src/ClientImp.cpp
    src/Replica.cpp
    src/replica_state_sync_imp.cpp
    src/block_metadata.cpp
    src/direct_kv_db_adapter.cpp
    src/merkle_tree_db_adapter.cpp
    src/merkle_tree_key_manipulator.cpp
    src/direct_kv_block.cpp
    src/merkle_tree_block.cpp
    src/direct_kv_storage_factory.cpp
    src/merkle_tree_storage_factory.cpp
    src/pruning_handler.cpp
    src/pruning_reserved_pages_client.cpp
    src/st_reconfiguration_sm.cpp
    src/sparse_merkle/base_types.cpp
    src/sparse_merkle/keys.cpp
    src/sparse_merkle/internal_node.cpp
    src/sparse_merkle/tree.cpp
    src/sparse_merkle/update_cache.cpp
    src/sparse_merkle/walker.cpp
    
    src/reconfiguration_kvbc_handler.cpp
)

if (BUILD_ROCKSDB_STORAGE)
    target_sources(kvbc PRIVATE src/categorization/immutable_kv_category.cpp
                                src/categorization/versioned_kv_category.cpp
                                src/categorization/kv_blockchain.cpp
                                src/categorization/blocks.cpp
                                src/categorization/blockchain.cpp
                                src/categorization/block_merkle_category.cpp)

endif (BUILD_ROCKSDB_STORAGE)
target_link_libraries(kvbc PUBLIC corebft util)
target_link_libraries(kvbc PUBLIC categorized_kvbc_msgs pruning_msgs event_group_msgs)

add_subdirectory("proto")
target_sources(kvbc PRIVATE src/kvbc_app_filter/kvbc_app_filter.cpp)
target_link_libraries(kvbc PUBLIC concord_block_update concord-kvbc-proto)

target_include_directories(kvbc PUBLIC include util)

if(NOT BUILD_THIRDPARTY)
    find_package(OpenSSL REQUIRED)
endif()
target_link_libraries(kvbc PRIVATE OpenSSL::Crypto)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()


add_subdirectory(benchmark)
add_subdirectory(tools)
find_package(CMFC REQUIRED)
add_subdirectory(cmf)

set_property(GLOBAL PROPERTY KVBC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
