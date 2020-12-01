if (USE_S3_OBJECT_STORE)
    find_package(GTest REQUIRED)
    add_executable(s3_metrics_test metrics_test.cpp )
    add_test(s3_metrics_test s3_metrics_test)

    target_link_libraries(s3_metrics_test PUBLIC
        GTest::Main
        util_shared
        concordbft_storage
        kvbc
    )

    target_include_directories(s3_metrics_test PRIVATE
        ${CMAKE_SOURCE_DIR}/kvbc/include/
        ${CMAKE_SOURCE_DIR}/bftengine/include/)

endif(USE_S3_OBJECT_STORE)
