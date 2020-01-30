add_library(concordbft_storage STATIC src/db_metadata_storage.cpp
                                      src/blockchain_db_adapter.cpp)

target_include_directories(concordbft_storage PUBLIC include)
target_link_libraries(concordbft_storage PUBLIC corebft)

if (BUILD_ROCKSDB_STORAGE)
  #cmake_policy(SET CMP0076 NEW) for cmake 3.14
  target_sources(concordbft_storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/rocksdb_client.cpp
                                            ${CMAKE_CURRENT_SOURCE_DIR}/src/rocksdb_key_comparator.cpp)
  target_compile_definitions(concordbft_storage PUBLIC USE_ROCKSDB=1 __BASE=1 SPARSE_STATE=1)
  target_include_directories(concordbft_storage PUBLIC ${CONAN_INCLUDE_DIRS_ROCKSDB})
  target_link_libraries(concordbft_storage
          PRIVATE
          CONAN_PKG::rocksdb
          CONAN_PKG::bzip2
          CONAN_PKG::lz4
          CONAN_PKG::zstd
          CONAN_PKG::zlib
          CONAN_PKG::snappy)

  if (BUILD_TESTING)
    add_subdirectory(test)
  endif()
endif(BUILD_ROCKSDB_STORAGE)

target_sources(concordbft_storage PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/memorydb_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/sparse_merkle/internal_node.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/sparse_merkle/tree.cpp
)
target_link_libraries(concordbft_storage PRIVATE CONAN_PKG::OpenSSL)

