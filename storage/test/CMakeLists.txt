add_executable(multiIO_test multiIO_test.cpp)
add_test(multiIO_test multiIO_test)
target_link_libraries(multiIO_test PUBLIC
    gtest
    util
    concordbft_storage_rocksdb
    concordbft_storage_blockchain
)

add_executable(metadataStorage_test metadataStorage_test.cpp)
add_test(metadataStorage_test metadataStorage_test)
target_link_libraries(metadataStorage_test PUBLIC
    gtest
    util
    concordbft_storage_rocksdb
    concordbft_storage_blockchain
)
