add_library(concordbft_storage_rocksdb STATIC
    rocksdb_client.cpp
    rocksdb_metadata_storage.cpp
)
target_include_directories(concordbft_storage_rocksdb PUBLIC
    ${bftengine_SOURCE_DIR}/include)

target_include_directories(concordbft_storage_rocksdb PUBLIC .)

find_library(ROCKSDB rocksdb)
find_library(LIBBZ2 bz2)
find_library(LIBLZ4 lz4)
find_library(LIBZSTD zstd)
find_library(LIBZ z)
find_library(LIBSNAPPY snappy)
target_compile_definitions(concordbft_storage_rocksdb PUBLIC USE_ROCKSDB=1)
target_link_libraries(concordbft_storage_rocksdb PUBLIC ${ROCKSDB} ${LIBBZ2} ${LIBLZ4} ${LIBZSTD} ${LIBZ} ${LIBSNAPPY})

target_compile_definitions(concordbft_storage_rocksdb PUBLIC __BASE=1 SPARSE_STATE=1 USE_TLS=1)
if(${BUILD_COMM_TCP_TLS})
    target_compile_definitions(concordbft_storage_rocksdb PUBLIC USE_COMM_TLS_TCP)
endif()
