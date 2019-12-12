add_executable(multiIO_test multiIO_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(multiIO_test multiIO_test)
target_link_libraries(multiIO_test PUBLIC
    gtest
    util
    concordbft_storage
)

add_executable(metadataStorage_test metadataStorage_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(metadataStorage_test metadataStorage_test)
target_link_libraries(metadataStorage_test PUBLIC
    gtest
    util
    concordbft_storage
)

add_executable(sparse_merkle_base_types_test sparse_merkle/base_types_test.cpp
    $<TARGET_OBJECTS:logging_dev>)
add_test(sparse_merkle_base_types_test sparse_merkle_base_types_test)
target_link_libraries(sparse_merkle_base_types_test PUBLIC
    gtest
    concordbft_storage
)
