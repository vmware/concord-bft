add_library(concordbft_storage_blockchain STATIC
    blockchain_db_adapter.cpp
    blockchain_db_helpers.cpp
    comparators.cpp
)

target_include_directories(concordbft_storage_blockchain PUBLIC
    ${bftengine_SOURCE_DIR}/include/bftengine
    .
)

target_link_libraries(concordbft_storage_blockchain PUBLIC util)

if (BUILD_ROCKSDB_STORAGE)
    target_link_libraries(concordbft_storage_blockchain PUBLIC concordbft_storage_rocksdb)
else()
    target_link_libraries(concordbft_storage_blockchain PUBLIC concordbft_storage_memory)
endif()

