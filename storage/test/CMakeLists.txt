find_package(GTest REQUIRED)

if (BUILD_ROCKSDB_STORAGE)
    add_executable(native_rocksdb_client_test native_rocksdb_client_test.cpp )
    add_test(native_rocksdb_client_test native_rocksdb_client_test)

    target_link_libraries(native_rocksdb_client_test PUBLIC
        GTest::Main
        GTest::GTest
        concordbft_storage
        util
        stdc++fs
    )
endif(BUILD_ROCKSDB_STORAGE)
