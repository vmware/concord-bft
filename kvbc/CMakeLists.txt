cmake_minimum_required (VERSION 3.2)
project(libkvbc VERSION 0.1.0.0 LANGUAGES CXX)

add_library(kvbc  src/ClientImp.cpp
    src/ReplicaImp.cpp
    src/replica_state_sync_imp.cpp
    src/block_metadata.cpp
    src/direct_kv_db_adapter.cpp
    src/merkle_tree_db_adapter.cpp
    src/merkle_tree_key_manipulator.cpp
    src/direct_kv_block.cpp
    src/merkle_tree_block.cpp
    src/direct_kv_storage_factory.cpp
    src/merkle_tree_storage_factory.cpp
    src/sparse_merkle/base_types.cpp
    src/sparse_merkle/keys.cpp
    src/sparse_merkle/internal_node.cpp
    src/sparse_merkle/tree.cpp
    src/sparse_merkle/update_cache.cpp
    src/sparse_merkle/walker.cpp
)

if (BUILD_ROCKSDB_STORAGE)
    target_sources(kvbc PRIVATE src/categorization/immutable_kv_category.cpp
                                src/categorization/kv_blockchain.cpp
                                src/categorization/blocks.cpp
                                src/categorization/blockchain.cpp
                                src/categorization/block_merkle_category.cpp)
endif (BUILD_ROCKSDB_STORAGE)

target_link_libraries(kvbc PUBLIC corebft )
target_link_libraries(kvbc PUBLIC categorized_kvbc_msgs)


target_include_directories(kvbc PUBLIC include util)

find_package(OpenSSL REQUIRED)
target_link_libraries(kvbc PRIVATE ${OPENSSL_LIBRARIES})

if (BUILD_TESTING)
    add_subdirectory(test)
endif()

add_subdirectory(benchmark)
add_subdirectory(tools)
find_package(CMFC REQUIRED)
add_subdirectory(cmf)
