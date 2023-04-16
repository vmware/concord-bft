project (libbftstorage LANGUAGES CXX)

add_library(concordbft_storage STATIC src/memorydb_client.cpp
                                      src/direct_kv_key_manipulator.cpp
                                      src/merkle_tree_key_manipulator.cpp
                                      src/s3/key_manipulator.cpp)

target_include_directories(concordbft_storage PUBLIC .)

if (USE_S3_OBJECT_STORE)
    find_library(LIBS3 s3)
    target_sources(concordbft_storage PRIVATE src/s3/client.cpp src/s3/config_parser.cpp)
    target_link_libraries(concordbft_storage PRIVATE ${LIBS3})
endif(USE_S3_OBJECT_STORE)

if (BUILD_ROCKSDB_STORAGE)
  find_library(ROCKSDB_LIBRARY rocksdb)
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
  target_link_libraries(concordbft_storage PRIVATE ${ROCKSDB_LIBRARY} ${LIBBZ2} ${LIBLZ4} ${LIBZSTD} ${LIBZ} ${LIBSNAPPY} ${CMAKE_DL_LIBS} util)

endif(BUILD_ROCKSDB_STORAGE)

target_sources(concordbft_storage PUBLIC FILE_SET storage_pub_hdrs
                                       TYPE HEADERS
                                       FILES 
                                       memorydb/transaction.hpp
                                       memorydb/client.hpp
                                       memorydb/key_comparator.hpp
                                       rocksdb/rocksdb_exception.hpp
                                       rocksdb/native_iterator.hpp
                                       rocksdb/transaction.hpp
                                       rocksdb/client.hpp
                                       rocksdb/native_write_batch.hpp
                                       rocksdb/key_comparator.hpp
                                       rocksdb/native_write_batch.ipp
                                       rocksdb/native_client.ipp
                                       rocksdb/details.hpp
                                       rocksdb/native_client.hpp
                                       s3/s3_metrics.hpp
                                       s3/config_parser.hpp
                                       s3/client.hpp
                                       s3/key_manipulator.hpp
                                       storage_metrics.hpp
                                       db_interface.hpp
                                       merkle_tree_key_manipulator.hpp
                                       key_manipulator_interface.hpp
                                       direct_kv_key_manipulator.hpp
                                       db_types.hpp
             )
install(TARGETS concordbft_storage FILE_SET storage_pub_hdrs DESTINATION storage)

add_library(concordbft_storage_test_headers INTERFACE)
target_include_directories(concordbft_storage_test_headers INTERFACE test)

if (BUILD_TESTING)
add_subdirectory(test)
endif()