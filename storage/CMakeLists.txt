add_library(concordbft_storage STATIC src/db_metadata_storage.cpp
                                      src/base_db_adapter.cpp
                                      src/direct_kv_db_adapter.cpp
                                      src/merkle_tree_db_adapter.cpp
                                      src/block.cpp)

target_include_directories(concordbft_storage PUBLIC include)
target_link_libraries(concordbft_storage PUBLIC corebft)

add_library(
  object_store_lib
  ${CMAKE_CURRENT_SOURCE_DIR}/src/object_store/object_store_client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/object_store/ecs_s3_client.hpp)
  target_include_directories(object_store_lib PUBLIC include)
  target_link_libraries(object_store_lib PRIVATE s3 util)


if (BUILD_ROCKSDB_STORAGE)
  #TODO [TK] find_package
  find_library(ROCKSDB rocksdb)
  find_path(ROCKSDB_INCLUDE_DIR "rocksdb/utilities/transaction_db.h")
  find_library(LIBBZ2    bz2)
  find_library(LIBLZ4    lz4)
  find_library(LIBZSTD   zstd)
  find_library(LIBZ      z)
  find_library(LIBSNAPPY snappy)

  #cmake_policy(SET CMP0076 NEW) for cmake 3.14
  target_sources(concordbft_storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/rocksdb_client.cpp
          ${CMAKE_CURRENT_SOURCE_DIR}/src/rocksdb_key_comparator.cpp)
  target_compile_definitions(concordbft_storage PUBLIC USE_ROCKSDB=1 __BASE=1 SPARSE_STATE=1)
  target_include_directories(concordbft_storage PUBLIC ${ROCKSDB_INCLUDE_DIR})
  target_link_libraries(concordbft_storage PRIVATE ${ROCKSDB} ${LIBBZ2} ${LIBLZ4} ${LIBZSTD} ${LIBZ} ${LIBSNAPPY})

  if (BUILD_TESTING)
    add_subdirectory(test)
  endif()
endif(BUILD_ROCKSDB_STORAGE)

find_package(OpenSSL REQUIRED)
target_sources(concordbft_storage PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/src/memorydb_client.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/src/sparse_merkle/internal_node.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/src/sparse_merkle/tree.cpp
    )
target_include_directories(corebft PUBLIC ${OPENSSL_INCLUDE_DIR})
target_link_libraries(concordbft_storage PRIVATE ${OPENSSL_LIBRARIES})

