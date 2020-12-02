add_library(concordbft_storage STATIC src/memorydb_client.cpp
                                      src/direct_kv_key_manipulator.cpp
                                      src/merkle_tree_key_manipulator.cpp
                                      src/s3/key_manipulator.cpp)

target_include_directories(concordbft_storage PUBLIC include)

if (USE_S3_OBJECT_STORE)
find_library(LIBS3 s3)
target_compile_definitions(concordbft_storage PUBLIC USE_S3_OBJECT_STORE=1)
target_sources(concordbft_storage PRIVATE src/s3/client.cpp)
target_link_libraries(concordbft_storage PRIVATE ${LIBS3})
endif(USE_S3_OBJECT_STORE)

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
  target_sources(concordbft_storage PRIVATE src/rocksdb_client.cpp src/rocksdb_key_comparator.cpp)
  target_compile_definitions(concordbft_storage PUBLIC USE_ROCKSDB=1 __BASE=1 SPARSE_STATE=1)
  target_include_directories(concordbft_storage PUBLIC ${ROCKSDB_INCLUDE_DIR})
  target_link_libraries(concordbft_storage PRIVATE ${ROCKSDB} ${LIBBZ2} ${LIBLZ4} ${LIBZSTD} ${LIBZ} ${LIBSNAPPY} ${CMAKE_DL_LIBS})

endif(BUILD_ROCKSDB_STORAGE)

add_subdirectory(test)
