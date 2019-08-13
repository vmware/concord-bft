add_library(concordbft_storage STATIC src/db_metadata_storage.cpp
                                      src/blockchain_db_adapter.cpp)

target_include_directories(concordbft_storage PUBLIC include)
target_link_libraries(concordbft_storage corebft)

if (BUILD_ROCKSDB_STORAGE)
  find_library(ROCKSDB   rocksdb)
  find_library(LIBBZ2    bz2)
  find_library(LIBLZ4    lz4)
  find_library(LIBZSTD   zstd)
  find_library(LIBZ      z)
  find_library(LIBSNAPPY snappy)

  #cmake_policy(SET CMP0076 NEW) for cmake 3.14
  target_sources(concordbft_storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/rocksdb_client.cpp
                                            ${CMAKE_CURRENT_SOURCE_DIR}/src/rocksdb_key_comparator.cpp)
  target_compile_definitions(concordbft_storage PUBLIC USE_ROCKSDB=1 __BASE=1 SPARSE_STATE=1 USE_TLS=1)
  target_link_libraries(concordbft_storage ${ROCKSDB} ${LIBBZ2} ${LIBLZ4} ${LIBZSTD} ${LIBZ} ${LIBSNAPPY})

  if (BUILD_TESTING)
    add_subdirectory(test)
  endif()

else() # NOT BUILD_ROCKSDB_STORAGE
  target_sources(concordbft_storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/memorydb_client.cpp)
endif(BUILD_ROCKSDB_STORAGE)
