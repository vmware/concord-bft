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

if(USE_S3_OBJECT_STORE)
    add_executable(s3_client_test s3_test.cpp)
    add_test(NAME s3_client_test COMMAND env CI=on MINIO_BINARY_PATH=$ENV{HOME}/minio ${CMAKE_CURRENT_BINARY_DIR}/s3_client_test)
    target_link_libraries(s3_client_test PUBLIC
        GTest::Main
        GTest::GTest
        concordbft_storage
        util
        test_config_lib
        stdc++fs
    )
endif(USE_S3_OBJECT_STORE)
